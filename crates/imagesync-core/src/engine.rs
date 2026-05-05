//! High-level engine: scan → plan → execute, emitting [`EngineEvent`]s.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::classify;
use crate::config::EngineConfig;
use crate::error::Result;
use crate::events::{CopyOutcome, EngineEvent, PlannedAction, PlannedFile};
use crate::exiftool::ExifTool;
use crate::metadata;
use crate::plan::{self, ScannedFile, SyncPlan};
use crate::profiles::ProfileRegistry;
use crate::source::{BackendHandle, MediaSource};

/// Options for [`Engine::execute`].
#[derive(Debug, Clone, Default)]
pub struct ExecuteOptions {
    pub dry_run: bool,
}

pub struct Engine {
    config: EngineConfig,
    registry: ProfileRegistry,
}

impl Engine {
    pub fn new(config: EngineConfig, registry: ProfileRegistry) -> Self {
        Self { config, registry }
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn registry(&self) -> &ProfileRegistry {
        &self.registry
    }

    /// Spawn a scan + plan task. Returns a join handle that resolves to the
    /// final [`SyncPlan`] together with a stream of progress events. The
    /// caller MUST consume the stream concurrently with awaiting the join
    /// handle, otherwise the channel buffer fills up and progress events
    /// look like a single end-of-run flush.
    pub fn scan_and_plan(
        &self,
        source: Arc<dyn MediaSource>,
    ) -> (
        tokio::task::JoinHandle<Result<SyncPlan>>,
        ReceiverStream<EngineEvent>,
    ) {
        let (tx, rx) = mpsc::channel::<EngineEvent>(256);
        let cfg = self.config.clone();
        let registry = self.registry.clone();
        let tx2 = tx.clone();
        let handle = tokio::spawn(async move {
            let res = run_scan_and_plan(cfg, registry, source, tx2.clone()).await;
            if let Err(e) = &res {
                let _ = tx2
                    .send(EngineEvent::Error {
                        message: format!("{e}"),
                        rel_path: None,
                    })
                    .await;
            }
            res
        });
        drop(tx);
        (handle, ReceiverStream::new(rx))
    }

    /// Execute a plan, emitting events and returning a summary at the end.
    pub fn execute(
        &self,
        plan: SyncPlan,
        source: Arc<dyn MediaSource>,
        opts: ExecuteOptions,
    ) -> ReceiverStream<EngineEvent> {
        let (tx, rx) = mpsc::channel::<EngineEvent>(256);
        let cfg = self.config.clone();
        tokio::spawn(async move {
            let _ = run_execute(cfg, plan, source, opts, tx).await;
        });
        ReceiverStream::new(rx)
    }

}

/// Free-function body of [`Engine::scan_and_plan`]. Lives outside the impl
/// so it can be moved into a `tokio::spawn`d task without lifetime issues.
async fn run_scan_and_plan(
    cfg: EngineConfig,
    registry: ProfileRegistry,
    source: Arc<dyn MediaSource>,
    tx: mpsc::Sender<EngineEvent>,
) -> Result<SyncPlan> {
    // Pre-flight: refuse to run if the destination roots aren't reachable.
    // This catches the common "drive isn't mounted" footgun where every
    // file would otherwise look new, get copied to an empty mount point,
    // and disappear when the real drive mounts. Bare existence check —
    // empty directories are accepted (legitimate first-time setup).
    check_dest_root(&cfg.images_root, "images_root")?;
    check_dest_root(&cfg.videos_root, "videos_root")?;

    let _ = tx
        .send(EngineEvent::ScanStarted {
            source: source.id().clone(),
            display_name: source.display_name().to_string(),
        })
        .await;

    // Detect profile.
    let profile = registry.detect_for_source(source.root_path());

    // Enumerate files.
    let files = source.list_files().await?;
    let _ = tx
        .send(EngineEvent::ScanComplete {
            source: source.id().clone(),
            files: files.len() as u64,
        })
        .await;

    // Classify and split into "needs metadata" vs "skip".
    let mut to_meta: Vec<crate::source::SourceFile> = Vec::new();
    let mut kinds: Vec<classify::MediaKind> = Vec::new();
    for f in files {
        let Some(kind) = classify::classify(profile, &f.extension) else {
            continue; // unknown extension; ignore silently
        };
        to_meta.push(f);
        kinds.push(kind);
    }
    tracing::debug!(
        profile = profile.id,
        count = to_meta.len(),
        "classified files for metadata read"
    );

    // Pre-skip: build a recursive index of the destination roots and skip
    // any non-sidecar file whose (basename, size) already exists somewhere
    // in the library. This avoids running exiftool on files we'd skip
    // anyway, which is the dominant cost on partially-imported cards.
    // Sidecars are always small and rare; we leave them in the metadata
    // path so they can inherit their parent's datetime correctly.
    let dest_index =
        plan::DestIndex::build(&[cfg.images_root.as_path(), cfg.videos_root.as_path()]);
    tracing::debug!(
        indexed = dest_index.len(),
        "built destination index for pre-skip"
    );

    let mut pre_skipped: Vec<PlannedFile> = Vec::new();
    let mut filtered_to_meta: Vec<crate::source::SourceFile> = Vec::new();
    let mut filtered_kinds: Vec<classify::MediaKind> = Vec::new();
    for (f, k) in to_meta.into_iter().zip(kinds) {
        let basename = f
            .rel_path
            .rsplit('/')
            .next()
            .unwrap_or(f.rel_path.as_str());
        if !k.is_sidecar() {
            if let Some(existing) = dest_index.lookup(basename, f.size) {
                pre_skipped.push(PlannedFile {
                    source: source.id().clone(),
                    source_rel_path: f.rel_path.clone(),
                    source_size: f.size,
                    kind: crate::events::MediaKindWire::from(k),
                    action: PlannedAction::SkipExists,
                    dest_path: Some(existing.to_path_buf()),
                    datetime: None,
                    date_source: None,
                    reason: Some("already in library (pre-skip index)".into()),
                });
                continue;
            }
        }
        filtered_to_meta.push(f);
        filtered_kinds.push(k);
    }
    let to_meta = filtered_to_meta;
    let kinds = filtered_kinds;
    tracing::debug!(
        pre_skipped = pre_skipped.len(),
        remaining = to_meta.len(),
        "pre-skip filter applied"
    );

    // Resolve filesystem paths for each.
    let mut paths: Vec<PathBuf> = Vec::with_capacity(to_meta.len());
    for f in &to_meta {
        paths.push(source.full_path(f).await?);
    }

    // Read metadata in batches via a stay_open exiftool process.
    let exif = ExifTool::spawn().await?;
    let batch_size = cfg.performance.metadata_batch_size.max(1);
    let mut metas: Vec<Option<metadata::ResolvedMetadata>> = Vec::with_capacity(to_meta.len());
    let total = to_meta.len() as u64;
    for chunk in paths.chunks(batch_size) {
        let raw = exif.read_batch(chunk).await?;
        let start = metas.len();
        for (i, m) in raw.iter().enumerate() {
            let sf = &to_meta[start + i];
            metas.push(metadata::resolve_one(sf, m));
        }
        let _ = tx
            .send(EngineEvent::MetadataProgress {
                done: metas.len() as u64,
                total,
            })
            .await;
    }
    let _ = exif.shutdown().await;

    // Build ScannedFile list.
    let mut scanned: Vec<ScannedFile> = Vec::with_capacity(to_meta.len());
    for ((f, k), m) in to_meta.into_iter().zip(kinds).zip(metas) {
        scanned.push(ScannedFile {
            source: source.id().clone(),
            file: f,
            kind: k,
            metadata: m,
        });
    }

    // Emit warnings for fallback dates.
    for s in &scanned {
        if let Some(meta) = &s.metadata {
            if meta.source.is_fallback() {
                let _ = tx
                    .send(EngineEvent::Warning {
                        message: format!(
                            "{}: using {} (no DateTimeOriginal)",
                            s.file.rel_path,
                            meta.source.label()
                        ),
                        rel_path: Some(s.file.rel_path.clone()),
                    })
                    .await;
            }
        }
    }

    // Build plan.
    let mut plan = plan::build_plan(&cfg, profile, scanned)?;

    // Merge pre-skipped items into the final plan. Order doesn't matter
    // for correctness; later UI groups by destination dir anyway.
    plan.items.extend(pre_skipped);

    let _ = tx
        .send(EngineEvent::PlanReady {
            copies: plan.copies(),
            skips: plan.skips(),
            errors: plan.errors(),
        })
        .await;

    Ok(plan)
}

async fn run_execute(
    cfg: EngineConfig,
    plan: SyncPlan,
    source: Arc<dyn MediaSource>,
    opts: ExecuteOptions,
    tx: mpsc::Sender<EngineEvent>,
) -> Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::Semaphore;

    // Re-check destination roots before any copy. Defensive: scan_and_plan
    // already checked, but plans can be persisted/replayed and roots can
    // be unmounted between scan and execute.
    if !opts.dry_run {
        check_dest_root(&cfg.images_root, "images_root")?;
        check_dest_root(&cfg.videos_root, "videos_root")?;
    }

    let workers = cfg.performance.copy_workers.max(1);
    let sem = Arc::new(Semaphore::new(workers));
    let mut joins: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    let copied = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let mut skipped: u64 = 0;

    for item in plan.items {
        match item.action {
            PlannedAction::Copy => {
                let permit = match sem.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };
                let tx = tx.clone();
                let source = source.clone();
                let cfg = cfg.clone();
                let dry = opts.dry_run;
                let copied = copied.clone();
                let failed = failed.clone();
                let h = tokio::spawn(async move {
                    let ok = do_copy(item, source, cfg, dry, tx).await;
                    if ok {
                        copied.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                    drop(permit);
                });
                joins.push(h);
            }
            PlannedAction::SkipExists | PlannedAction::SkipFiltered | PlannedAction::SkipNoDate => {
                skipped += 1;
                let reason = item
                    .reason
                    .clone()
                    .unwrap_or_else(|| "skipped".to_string());
                let _ = tx
                    .send(EngineEvent::CopyComplete {
                        file: item,
                        outcome: CopyOutcome::Skipped { reason },
                    })
                    .await;
            }
            PlannedAction::Error => {
                failed.fetch_add(1, Ordering::Relaxed);
                let err = item
                    .reason
                    .clone()
                    .unwrap_or_else(|| "planning error".to_string());
                let _ = tx
                    .send(EngineEvent::CopyComplete {
                        file: item,
                        outcome: CopyOutcome::Failed { error: err },
                    })
                    .await;
            }
        }
    }

    for h in joins {
        let _ = h.await;
    }

    let _ = tx
        .send(EngineEvent::SyncSummary {
            copied: copied.load(Ordering::Relaxed),
            skipped,
            failed: failed.load(Ordering::Relaxed),
        })
        .await;
    Ok(())
}

async fn do_copy(
    item: PlannedFile,
    source: Arc<dyn MediaSource>,
    cfg: EngineConfig,
    dry_run: bool,
    tx: mpsc::Sender<EngineEvent>,
) -> bool {
    let _ = tx
        .send(EngineEvent::CopyStarted { file: item.clone() })
        .await;

    // Resolve source path.
    let src_path = match resolve_full_path(source.as_ref(), &item).await {
        Ok(p) => p,
        Err(e) => {
            let _ = tx
                .send(EngineEvent::CopyComplete {
                    file: item,
                    outcome: CopyOutcome::Failed {
                        error: format!("{e}"),
                    },
                })
                .await;
            return false;
        }
    };
    let Some(dest) = item.dest_path.clone() else {
        let _ = tx
            .send(EngineEvent::CopyComplete {
                file: item,
                outcome: CopyOutcome::Failed {
                    error: "no destination path".into(),
                },
            })
            .await;
        return false;
    };

    if dry_run {
        let _ = tx
            .send(EngineEvent::CopyComplete {
                file: item.clone(),
                outcome: CopyOutcome::Copied {
                    bytes: item.source_size,
                },
            })
            .await;
        return true;
    }

    let bytes_total = item.source_size;
    let rel = item.source_rel_path.clone();
    let tx2 = tx.clone();
    let result = crate::copy::copy_atomic(&src_path, &dest, bytes_total, &cfg.verify, move |done, total| {
        // Best-effort progress; ignore send errors.
        let _ = tx2.try_send(EngineEvent::CopyProgress {
            rel_path: rel.clone(),
            bytes_done: done,
            bytes_total: total,
        });
    })
    .await;

    let (ok, outcome) = match result {
        Ok(bytes) => (true, CopyOutcome::Copied { bytes }),
        Err(e) => (
            false,
            CopyOutcome::Failed {
                error: format!("{e}"),
            },
        ),
    };
    let _ = tx
        .send(EngineEvent::CopyComplete {
            file: item,
            outcome,
        })
        .await;
    ok
}

async fn resolve_full_path(
    source: &dyn MediaSource,
    item: &PlannedFile,
) -> Result<PathBuf> {
    // For FilesystemSource the rel_path resolves directly off `root_path`.
    // Other sources (future PtpSource) will need their own resolution via
    // `MediaSource::full_path`.
    let p = source
        .root_path()
        .join(item.source_rel_path.replace('/', std::path::MAIN_SEPARATOR_STR));
    if p.exists() {
        return Ok(p);
    }
    // Fallback: ask the source via a synthetic SourceFile.
    let sf = crate::source::SourceFile {
        rel_path: item.source_rel_path.clone(),
        size: item.source_size,
        mtime: None,
        extension: String::new(),
        backend_handle: BackendHandle::Path(p.clone()),
    };
    source.full_path(&sf).await
}

/// Verify a configured destination root exists as a directory. Empty
/// directories are accepted (legitimate first-time setup); non-existent
/// paths and non-directories (e.g. a regular file at that path) are
/// rejected. Catches the "drive not mounted" footgun where every source
/// file would otherwise be misclassified as new.
fn check_dest_root(path: &std::path::Path, kind: &'static str) -> Result<()> {
    match std::fs::metadata(path) {
        Ok(m) if m.is_dir() => Ok(()),
        Ok(_) => Err(crate::error::Error::DestRootNotDir {
            path: path.to_path_buf(),
            kind,
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(crate::error::Error::DestRootMissing {
                path: path.to_path_buf(),
                kind,
            })
        }
        Err(e) => Err(crate::error::Error::io(path, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_dest_root_accepts_existing_dir() {
        let tmp = tempfile::tempdir().unwrap();
        check_dest_root(tmp.path(), "images_root").unwrap();
    }

    #[test]
    fn check_dest_root_accepts_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let empty = tmp.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        check_dest_root(&empty, "images_root").unwrap();
    }

    #[test]
    fn check_dest_root_rejects_missing_path() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("does-not-exist");
        let err = check_dest_root(&missing, "videos_root").unwrap_err();
        match err {
            crate::error::Error::DestRootMissing { kind, path } => {
                assert_eq!(kind, "videos_root");
                assert_eq!(path, missing);
            }
            other => panic!("expected DestRootMissing, got {other:?}"),
        }
    }

    #[test]
    fn check_dest_root_rejects_regular_file() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("a-file");
        std::fs::write(&file, b"not a dir").unwrap();
        let err = check_dest_root(&file, "images_root").unwrap_err();
        match err {
            crate::error::Error::DestRootNotDir { kind, path } => {
                assert_eq!(kind, "images_root");
                assert_eq!(path, file);
            }
            other => panic!("expected DestRootNotDir, got {other:?}"),
        }
    }
}
