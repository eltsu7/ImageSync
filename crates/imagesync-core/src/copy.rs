//! Copy execution: atomic temp+rename, optional xxh3 verify.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::VerifyConfig;
use crate::error::{Error, Result};

const COPY_BUF_SIZE: usize = 1024 * 1024; // 1 MiB

/// Copy `src` to `dest` atomically:
/// 1. Create dest's parent dir.
/// 2. Stream copy into `<dest>.imagesync-tmp-<rand>`.
/// 3. fsync the temp file.
/// 4. Rename to `dest`.
///
/// Returns total bytes copied. Calls `progress(bytes_done, bytes_total)` as
/// data is written.
pub async fn copy_atomic<F>(
    src: &Path,
    dest: &Path,
    bytes_total: u64,
    verify: &VerifyConfig,
    mut progress: F,
) -> Result<u64>
where
    F: FnMut(u64, u64) + Send,
{
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| Error::io(parent, e))?;
    }
    let tmp = temp_path_for(dest);

    let mut reader = tokio::fs::File::open(src)
        .await
        .map_err(|e| Error::io(src, e))?;
    let mut writer = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp)
        .await
        .map_err(|e| Error::io(&tmp, e))?;

    let mut buf = vec![0u8; COPY_BUF_SIZE];
    let mut hasher_src = xxhash_rust::xxh3::Xxh3::new();
    let mut total: u64 = 0;
    loop {
        let n = reader
            .read(&mut buf)
            .await
            .map_err(|e| Error::io(src, e))?;
        if n == 0 {
            break;
        }
        if verify.enabled {
            hasher_src.update(&buf[..n]);
        }
        writer
            .write_all(&buf[..n])
            .await
            .map_err(|e| Error::io(&tmp, e))?;
        total += n as u64;
        progress(total, bytes_total);
    }
    writer.flush().await.map_err(|e| Error::io(&tmp, e))?;
    writer.sync_all().await.map_err(|e| Error::io(&tmp, e))?;
    drop(writer);
    drop(reader);

    if verify.enabled {
        let src_hash = hasher_src.digest();
        let dst_hash = hash_file_xxh3(&tmp).await?;
        if src_hash != dst_hash {
            // Best-effort cleanup
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(Error::IoBare(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "verify failed: src xxh3={:016x} dst xxh3={:016x}",
                    src_hash, dst_hash
                ),
            )));
        }
    }

    tokio::fs::rename(&tmp, dest)
        .await
        .map_err(|e| Error::io(dest, e))?;
    Ok(total)
}

async fn hash_file_xxh3(path: &Path) -> Result<u64> {
    let mut f = tokio::fs::File::open(path)
        .await
        .map_err(|e| Error::io(path, e))?;
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut buf = vec![0u8; COPY_BUF_SIZE];
    loop {
        let n = f.read(&mut buf).await.map_err(|e| Error::io(path, e))?;
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
}
