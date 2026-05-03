//! Abstract source of media files.
//!
//! v0.1 ships [`FilesystemSource`] only — used for SD card readers and for
//! cameras in Mass Storage (MSC) mode, which appear as regular drives. The
//! trait is shaped so a future `PtpSource` (libgphoto2) can drop in without
//! engine changes.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};

/// Identifier for a source, stable across a single engine run.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceId(pub String);

impl std::fmt::Display for SourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A single file enumerated by a source.
#[derive(Debug, Clone)]
pub struct SourceFile {
    /// Source-relative logical path (always uses `/` for portability).
    pub rel_path: String,
    /// File size in bytes.
    pub size: u64,
    /// File modification time, if known. Used as date fallback.
    pub mtime: Option<chrono::NaiveDateTime>,
    /// Lowercase extension, no dot (e.g. `"arw"`). Empty if none.
    pub extension: String,
    /// Source-specific identifier passed back to [`MediaSource::open_path`]
    /// / [`MediaSource::full_path`]. For [`FilesystemSource`] this is the
    /// absolute path on disk.
    pub backend_handle: BackendHandle,
}

#[derive(Debug, Clone)]
pub enum BackendHandle {
    /// Absolute filesystem path. Used by [`FilesystemSource`].
    Path(PathBuf),
    /// PTP/MTP object id (placeholder for the future implementation).
    #[allow(dead_code)]
    PtpObject(u32),
}

#[async_trait]
pub trait MediaSource: Send + Sync {
    fn id(&self) -> &SourceId;
    fn display_name(&self) -> &str;

    /// Logical root path (used for profile detection and reporting).
    fn root_path(&self) -> &Path;

    /// Enumerate all candidate files. Implementations should be tolerant of
    /// non-media files; the engine filters by extension afterwards.
    async fn list_files(&self) -> Result<Vec<SourceFile>>;

    /// Resolve a backend handle to a readable filesystem path. For
    /// [`FilesystemSource`] this is the original path. For a PTP source this
    /// would download to a temp file and return that path. Engine uses this
    /// for ExifTool reads and for the copy step.
    async fn full_path(&self, file: &SourceFile) -> Result<PathBuf>;
}

/// Filesystem-backed source: a mounted SD card, a camera in MSC mode, or any
/// directory the user picks.
pub struct FilesystemSource {
    id: SourceId,
    display_name: String,
    root: PathBuf,
    /// If non-empty, only these subdirs (relative to `root`) are scanned. If
    /// any of them is missing the missing ones are silently skipped.
    scan_subdirs: Vec<String>,
}

impl FilesystemSource {
    pub fn new(id: impl Into<String>, display_name: impl Into<String>, root: PathBuf) -> Self {
        Self {
            id: SourceId(id.into()),
            display_name: display_name.into(),
            root,
            scan_subdirs: Vec::new(),
        }
    }

    /// Restrict scanning to specific subdirectories of the root. Empty means
    /// scan everything.
    pub fn with_subdirs(mut self, subdirs: Vec<String>) -> Self {
        self.scan_subdirs = subdirs;
        self
    }

    pub fn into_arc(self) -> Arc<dyn MediaSource> {
        Arc::new(self)
    }
}

#[async_trait]
impl MediaSource for FilesystemSource {
    fn id(&self) -> &SourceId {
        &self.id
    }

    fn display_name(&self) -> &str {
        &self.display_name
    }

    fn root_path(&self) -> &Path {
        &self.root
    }

    async fn list_files(&self) -> Result<Vec<SourceFile>> {
        let root = self.root.clone();
        let subdirs = self.scan_subdirs.clone();

        // walkdir is blocking; spawn on the blocking thread pool.
        let files = tokio::task::spawn_blocking(move || -> Result<Vec<SourceFile>> {
            let mut roots: Vec<PathBuf> = if subdirs.is_empty() {
                vec![root.clone()]
            } else {
                subdirs
                    .iter()
                    .map(|s| root.join(s))
                    .filter(|p| p.exists())
                    .collect()
            };
            // If profile listed subdirs but none exist on this card, scan the
            // root anyway so users with unusual layouts still get something.
            if roots.is_empty() {
                roots.push(root.clone());
            }

            let mut out = Vec::new();
            for r in roots {
                for entry in walkdir::WalkDir::new(&r).follow_links(false) {
                    let entry = match entry {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::warn!("walk error: {e}");
                            continue;
                        }
                    };
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    let abs = entry.path().to_path_buf();
                    let rel = abs
                        .strip_prefix(&root)
                        .unwrap_or(&abs)
                        .to_string_lossy()
                        .replace('\\', "/");
                    let metadata = match entry.metadata() {
                        Ok(m) => m,
                        Err(e) => {
                            tracing::warn!("stat error for {}: {e}", abs.display());
                            continue;
                        }
                    };
                    let size = metadata.len();
                    let mtime = metadata
                        .modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .and_then(|d| {
                            chrono::DateTime::from_timestamp(d.as_secs() as i64, d.subsec_nanos())
                        })
                        .map(|dt| dt.naive_local());
                    let extension = abs
                        .extension()
                        .and_then(|s| s.to_str())
                        .map(|s| s.to_ascii_lowercase())
                        .unwrap_or_default();
                    out.push(SourceFile {
                        rel_path: rel,
                        size,
                        mtime,
                        extension,
                        backend_handle: BackendHandle::Path(abs),
                    });
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::Source(format!("scan task panicked: {e}")))??;

        Ok(files)
    }

    async fn full_path(&self, file: &SourceFile) -> Result<PathBuf> {
        match &file.backend_handle {
            BackendHandle::Path(p) => Ok(p.clone()),
            BackendHandle::PtpObject(_) => Err(Error::Source(
                "FilesystemSource cannot resolve PTP handles".into(),
            )),
        }
    }
}

/// Quick test for "is this directory likely a camera card?": presence of a
/// `DCIM` subdir is the universal DCF marker.
pub fn looks_like_camera_card(root: &Path) -> bool {
    root.join("DCIM").is_dir()
}
