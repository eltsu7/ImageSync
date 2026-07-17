//! Copy execution: atomic temp+rename, optional xxh3 verify.
//!
//! Performance notes:
//! - The actual copy runs inside a single `spawn_blocking` call using
//!   blocking `std::fs`. Tokio's async `fs` API uses `spawn_blocking` *per
//!   read/write call*, which incurs a futex+context-switch round-trip per
//!   chunk and was the main bottleneck for this tool. Doing the entire
//!   copy in one blocking task brings throughput to within a few percent
//!   of `cp(1)`.
//! - Progress is reported through a callback that is invoked from the
//!   blocking thread. The callback is throttled internally so we don't
//!   spam the engine's event channel: an update is sent at most every
//!   `PROGRESS_BYTES_INTERVAL` bytes plus one final update at completion.
//! - We deliberately do NOT `fsync` per file. Atomicity-on-visibility is
//!   provided by the temp+rename pattern; if the OS crashes before its
//!   writeback flushes, a re-run will re-import the missing files via
//!   the case-insensitive dedupe.
//!
//! Inspired by RapidPhotoDownloader's `copyfiles.py`, which uses a
//! 1 MiB io buffer and emits progress every 5 MiB.
//!
//! See <https://docs.rs/tokio/latest/tokio/fs/> for the upstream warning
//! about per-call `spawn_blocking` overhead.

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::config::VerifyConfig;
use crate::error::{Error, Result};

const COPY_BUF_SIZE: usize = 1024 * 1024; // 1 MiB
const PROGRESS_BYTES_INTERVAL: u64 = 4 * 1024 * 1024; // emit every ~4 MiB

/// Copy `src` to `dest` atomically:
/// 1. Create dest's parent dir.
/// 2. Stream copy into `<dest>.imagesync-tmp-<rand>`.
/// 3. Rename to `dest`.
///
/// Returns total bytes copied. Calls `progress(bytes_done, bytes_total)`
/// every ~4 MiB and at completion. The callback runs on a blocking
/// worker thread; it must not block on tokio primitives.
pub async fn copy_atomic<F>(
    src: &Path,
    dest: &Path,
    bytes_total: u64,
    verify: &VerifyConfig,
    progress: F,
) -> Result<u64>
where
    F: FnMut(u64, u64) + Send + 'static,
{
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| Error::io(parent, e))?;
    }
    let tmp = temp_path_for(dest);
    let src = src.to_path_buf();
    let dest = dest.to_path_buf();
    let verify = verify.clone();

    // The whole copy happens in one blocking task.
    tokio::task::spawn_blocking(move || copy_blocking(&src, &dest, &tmp, bytes_total, &verify, progress))
        .await
        .map_err(|e| {
            Error::IoBare(std::io::Error::other(format!("copy task panicked: {e}")))
        })?
}

fn copy_blocking<F>(
    src: &Path,
    dest: &Path,
    tmp: &Path,
    bytes_total: u64,
    verify: &VerifyConfig,
    mut progress: F,
) -> Result<u64>
where
    F: FnMut(u64, u64) + Send,
{
    let mut reader = File::open(src).map_err(|e| Error::io(src, e))?;
    let mut writer = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(tmp)
        .map_err(|e| Error::io(tmp, e))?;

    let mut buf = vec![0u8; COPY_BUF_SIZE];
    let mut hasher_src = if verify.enabled {
        Some(xxhash_rust::xxh3::Xxh3::new())
    } else {
        None
    };
    let mut total: u64 = 0;
    let mut last_progress_total: u64 = 0;

    loop {
        let n = reader.read(&mut buf).map_err(|e| Error::io(src, e))?;
        if n == 0 {
            break;
        }
        if let Some(h) = hasher_src.as_mut() {
            h.update(&buf[..n]);
        }
        writer
            .write_all(&buf[..n])
            .map_err(|e| Error::io(tmp, e))?;
        total += n as u64;
        if total - last_progress_total >= PROGRESS_BYTES_INTERVAL {
            last_progress_total = total;
            progress(total, bytes_total);
        }
    }
    writer.flush().map_err(|e| Error::io(tmp, e))?;
    drop(writer);
    drop(reader);

    if let Some(h) = hasher_src {
        let src_hash = h.digest();
        let dst_hash = hash_file_xxh3_blocking(tmp)?;
        if src_hash != dst_hash {
            let _ = std::fs::remove_file(tmp);
            return Err(Error::IoBare(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "verify failed: src xxh3={:016x} dst xxh3={:016x}",
                    src_hash, dst_hash
                ),
            )));
        }
    }

    std::fs::rename(tmp, dest).map_err(|e| Error::io(dest, e))?;

    // Final progress event so the gauge ends at 100%.
    progress(total, bytes_total);
    Ok(total)
}

/// Move a staged file into its final destination.
///
/// Staging lives under the destination root, so this is normally a metadata-only
/// `rename` on the same filesystem (free, atomic-on-visibility). Falls back to
/// copy+remove if the rename crosses a filesystem boundary (`EXDEV`) — defensive
/// for unusual mount layouts.
pub async fn finalize_move(staged: &Path, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| Error::io(parent, e))?;
    }
    let staged = staged.to_path_buf();
    let dest = dest.to_path_buf();
    tokio::task::spawn_blocking(move || {
        match std::fs::rename(&staged, &dest) {
            Ok(()) => Ok(()),
            Err(e) if is_cross_device(&e) => {
                // Cross-filesystem: copy then remove the source.
                std::fs::copy(&staged, &dest).map_err(|e| Error::io(&dest, e))?;
                let _ = std::fs::remove_file(&staged);
                Ok(())
            }
            Err(e) => Err(Error::io(&dest, e)),
        }
    })
    .await
    .map_err(|e| Error::IoBare(std::io::Error::other(format!("move task panicked: {e}"))))?
}

#[cfg(unix)]
fn is_cross_device(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(libc_exdev())
}

#[cfg(not(unix))]
fn is_cross_device(e: &std::io::Error) -> bool {
    // Windows rename across volumes yields ERROR_NOT_SAME_DEVICE (17).
    e.raw_os_error() == Some(17)
}

#[cfg(unix)]
fn libc_exdev() -> i32 {
    18 // EXDEV on Linux/macOS/BSD
}

#[cfg(test)]
pub(crate) fn is_cross_device_for_test(e: &std::io::Error) -> bool {
    is_cross_device(e)
}

fn hash_file_xxh3_blocking(path: &Path) -> Result<u64> {
    let mut f = File::open(path).map_err(|e| Error::io(path, e))?;
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut buf = vec![0u8; COPY_BUF_SIZE];
    loop {
        let n = f.read(&mut buf).map_err(|e| Error::io(path, e))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.digest())
}

fn temp_path_for(dest: &Path) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let pid = std::process::id();
    let name = format!(
        "{}.imagesync-tmp-{pid}-{nanos:08x}",
        dest.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("file")
    );
    match dest.parent() {
        Some(p) => p.join(name),
        None => PathBuf::from(name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn copies_file_and_creates_dirs() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("a.bin");
        std::fs::write(&src, b"hello world").unwrap();
        let dest = dir.path().join("nested/dir/a.bin");
        let bytes = copy_atomic(&src, &dest, 11, &VerifyConfig::default(), |_, _| {})
            .await
            .unwrap();
        assert_eq!(bytes, 11);
        assert_eq!(std::fs::read(&dest).unwrap(), b"hello world");
    }

    #[tokio::test]
    async fn verify_round_trips() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("a.bin");
        let payload: Vec<u8> = (0u8..255).cycle().take(2_000_000).collect();
        std::fs::write(&src, &payload).unwrap();
        let dest = dir.path().join("a.bin.copy");
        let v = VerifyConfig {
            enabled: true,
            algorithm: crate::config::VerifyAlgorithm::Xxh3,
        };
        let bytes = copy_atomic(&src, &dest, payload.len() as u64, &v, |_, _| {})
            .await
            .unwrap();
        assert_eq!(bytes, payload.len() as u64);
        assert_eq!(std::fs::read(&dest).unwrap(), payload);
    }

    #[tokio::test]
    async fn progress_callback_invoked() {
        use std::sync::{Arc, Mutex};
        let dir = tempdir().unwrap();
        let src = dir.path().join("big.bin");
        // ~10 MiB of data so we cross a few PROGRESS_BYTES_INTERVAL boundaries.
        let payload = vec![0u8; 10 * 1024 * 1024];
        std::fs::write(&src, &payload).unwrap();
        let dest = dir.path().join("big.bin.copy");
        let calls: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let calls2 = calls.clone();
        copy_atomic(
            &src,
            &dest,
            payload.len() as u64,
            &VerifyConfig::default(),
            move |done, total| {
                calls2.lock().unwrap().push((done, total));
            },
        )
        .await
        .unwrap();
        let calls = calls.lock().unwrap();
        assert!(!calls.is_empty(), "progress should have been reported");
        // Final callback should equal total size.
        assert_eq!(calls.last().unwrap().0, payload.len() as u64);
    }
}
