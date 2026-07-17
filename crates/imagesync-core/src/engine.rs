//! High-level engine: scan → plan → execute, emitting [`EngineEvent`]s.

use fs2::FileExt;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::classify::MediaKind;
use crate::config::EngineConfig;
use crate::error::Result;
use crate::events::{
    CopyOutcome, DateSourceWire, EngineEvent, MediaKindWire, PlannedAction, PlannedFile,
};
use crate::exiftool::{ExifTool, FileMetadata};
use crate::metadata::{self, ResolvedMetadata};
use crate::plan::{self, CopyUnit, DedupeCache, ScannedFile, SyncPlan};
use crate::profiles::ProfileRegistry;
use crate::source::{BackendHandle, MediaSource, SourceFile, SourceId};

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
///
/// This is the **cheap plan**: it classifies, filters, pre-skips already-
/// imported files by `(basename, size)`, and pairs sidecars — all without
/// reading a single byte of EXIF. The capture date (and thus the destination
/// folder) is resolved later, at copy time, on the local staged copy. That
/// makes the preview instant and keeps the slow exiftool reads off the USB bus.
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
    let t_list_start = std::time::Instant::now();
    let files = source.list_files().await?;
    let _ = tx
        .send(EngineEvent::ScanComplete {
            source: source.id().clone(),
            files: files.len() as u64,
        })
        .await;
    tracing::debug!(
        count = files.len(),
        elapsed_ms = t_list_start.elapsed().as_millis() as u64,
        "listed source files"
    );

    // Pre-skip index over destination roots (excludes the staging dir). Skips
    // already-imported files by (basename, size) without touching their EXIF.
    let t_index_start = std::time::Instant::now();
    let dest_index =
        plan::DestIndex::build(&[cfg.images_root.as_path(), cfg.videos_root.as_path()]);
    tracing::debug!(
        indexed = dest_index.len(),
        elapsed_ms = t_index_start.elapsed().as_millis() as u64,
        "built destination index for pre-skip"
    );

    // Cheap plan: classify + filter + pre-skip + sidecar pairing. No exiftool.
    let t_plan_start = std::time::Instant::now();
    let cheap = plan::cheap_plan(&cfg, profile, source.id(), files, &dest_index);
    let plan = cheap.into_sync_plan(source.id());
    tracing::debug!(
        profile = profile.id,
        units = plan.units.len(),
        elapsed_ms = t_plan_start.elapsed().as_millis() as u64,
        "cheap plan built (no exiftool)"
    );

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
    if opts.dry_run {
        return run_execute_dry(cfg, plan, source, tx).await;
    }

    // Re-check destination roots before any copy. Defensive: scan_and_plan
    // already checked, but plans can be persisted/replayed and roots can
    // be unmounted between scan and execute.
    let _destination_locks = lock_destination_roots(&cfg.images_root, &cfg.videos_root)?;
    run_execute_staging(cfg, plan, source, tx).await
}

/// Shared, lock-free counters for the staging executor's worker tasks.
#[derive(Default)]
struct Counters {
    copied: std::sync::atomic::AtomicU64,
    skipped: std::sync::atomic::AtomicU64,
    failed: std::sync::atomic::AtomicU64,
}

/// Real sync via the copy→exif→move pipeline.
///
/// Each unit is handled end-to-end by a worker (up to `copy_workers` at once):
/// copy parent + sidecars to a per-root staging dir, read EXIF from the **local**
/// staged copy, date + dedupe, then `rename` into the final date folder. The USB
/// is read once, sequentially; the seeky EXIF reads land on local disk.
/// `CopyComplete` is emitted as soon as each file is placed, so progress streams
/// smoothly per file instead of jumping per batch.
///
/// If the event consumer goes away (the UI aborts the sync), `send` starts
/// failing; we set `cancel`, stop launching work, and — because this engine task
/// still runs to completion — sweep the staging area on the way out.
async fn run_execute_staging(
    cfg: EngineConfig,
    plan: SyncPlan,
    source: Arc<dyn MediaSource>,
    tx: mpsc::Sender<EngineEvent>,
) -> Result<()> {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use tokio::sync::{Mutex, Semaphore};

    let counters = Arc::new(Counters::default());
    let cancel = Arc::new(AtomicBool::new(false));

    // Emit already-decided (non-copy) rows up front.
    for item in plan
        .items
        .iter()
        .filter(|i| i.action != PlannedAction::Copy)
    {
        let is_err = item.action == PlannedAction::Error;
        let outcome = if is_err {
            CopyOutcome::Failed {
                error: item
                    .reason
                    .clone()
                    .unwrap_or_else(|| "planning error".into()),
            }
        } else {
            CopyOutcome::Skipped {
                reason: item.reason.clone().unwrap_or_else(|| "skipped".into()),
            }
        };
        if tx
            .send(EngineEvent::CopyComplete {
                file: item.clone(),
                outcome,
            })
            .await
            .is_err()
        {
            cancel.store(true, Ordering::Relaxed);
            break;
        }
        if is_err {
            counters.failed.fetch_add(1, Ordering::Relaxed);
        } else {
            counters.skipped.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Sweep any stale staging left by a previous crash or abort.
    sweep_staging(&cfg.images_root.join(plan::STAGING_DIR_NAME));
    sweep_staging(&cfg.videos_root.join(plan::STAGING_DIR_NAME));

    if !cancel.load(Ordering::Relaxed) {
        // One shared exiftool process (local reads). Workers serialize on its
        // internal lock; we fall back to mtime dating if it can't spawn.
        let exif = Arc::new(match ExifTool::spawn().await {
            Ok(e) => Some(e),
            Err(e) => {
                let _ = tx
                    .send(EngineEvent::Warning {
                        message: format!("exiftool unavailable, dating by file mtime only: {e}"),
                        rel_path: None,
                    })
                    .await;
                None
            }
        });
        let dedupe = Arc::new(Mutex::new(DedupeCache::new()));
        let workers = cfg.performance.copy_workers.max(1);
        let sem = Arc::new(Semaphore::new(workers));
        let seq = Arc::new(AtomicU64::new(0));
        let source_id = source.id().clone();
        let t_start = std::time::Instant::now();
        let mut joins = Vec::new();

        for unit in plan.units {
            if cancel.load(Ordering::Relaxed) {
                break;
            }
            let permit = match sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break,
            };
            let cfg = cfg.clone();
            let source = source.clone();
            let source_id = source_id.clone();
            let exif = exif.clone();
            let dedupe = dedupe.clone();
            let tx = tx.clone();
            let counters = counters.clone();
            let cancel = cancel.clone();
            let n = seq.fetch_add(1, Ordering::Relaxed);
            joins.push(tokio::spawn(async move {
                process_unit(
                    &cfg,
                    source.as_ref(),
                    &source_id,
                    exif.as_ref().as_ref(),
                    &dedupe,
                    unit,
                    n,
                    &tx,
                    &counters,
                    &cancel,
                )
                .await;
                drop(permit);
            }));
        }
        for h in joins {
            let _ = h.await;
        }

        if let Some(e) = Arc::into_inner(exif).flatten() {
            let _ = e.shutdown().await;
        }

        let placed = counters.copied.load(Ordering::Relaxed);
        let elapsed_ms = t_start.elapsed().as_millis() as u64;
        let per_file_us = if placed > 0 {
            (elapsed_ms as f64 * 1000.0 / placed as f64) as u64
        } else {
            0
        };
        tracing::debug!(
            files = placed,
            elapsed_ms,
            per_file_us,
            "staging execute complete (exif on local copies)"
        );
    }

    // Always clean up staging — runs even on abort (the UI just drops the
    // event stream; this engine task still completes).
    sweep_staging(&cfg.images_root.join(plan::STAGING_DIR_NAME));
    sweep_staging(&cfg.videos_root.join(plan::STAGING_DIR_NAME));

    if !cancel.load(Ordering::Relaxed) {
        let _ = tx
            .send(EngineEvent::SyncSummary {
                copied: counters.copied.load(Ordering::Relaxed),
                skipped: counters.skipped.load(Ordering::Relaxed),
                failed: counters.failed.load(Ordering::Relaxed),
            })
            .await;
    }
    Ok(())
}

/// Send a CopyComplete; mark `cancel` if the consumer has gone away.
async fn send_complete(
    tx: &mpsc::Sender<EngineEvent>,
    cancel: &std::sync::atomic::AtomicBool,
    file: PlannedFile,
    outcome: CopyOutcome,
) {
    if tx
        .send(EngineEvent::CopyComplete { file, outcome })
        .await
        .is_err()
    {
        cancel.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Handle one unit end-to-end: copy to staging → EXIF the local copy → date +
/// dedupe → move into place, emitting events as it goes.
#[allow(clippy::too_many_arguments)]
async fn process_unit(
    cfg: &EngineConfig,
    source: &dyn MediaSource,
    source_id: &SourceId,
    exif: Option<&ExifTool>,
    dedupe: &tokio::sync::Mutex<DedupeCache>,
    unit: CopyUnit,
    seq: u64,
    tx: &mpsc::Sender<EngineEvent>,
    counters: &Counters,
    cancel: &std::sync::atomic::AtomicBool,
) {
    use std::sync::atomic::Ordering;

    let root = match unit.parent_kind {
        MediaKind::Video => &cfg.videos_root,
        _ => &cfg.images_root,
    };
    let seq_dir = root.join(plan::STAGING_DIR_NAME).join(seq.to_string());

    // Copy the parent first; a sidecar can't be dated without it.
    let parent = match copy_one_to_staging(
        cfg,
        source,
        &unit.parent,
        unit.parent_kind,
        &seq_dir,
        tx,
        cancel,
    )
    .await
    {
        Some(sf) => sf,
        None => {
            if !cancel.load(Ordering::Relaxed) {
                counters.failed.fetch_add(1, Ordering::Relaxed);
                for sidecar in &unit.sidecars {
                    counters.failed.fetch_add(1, Ordering::Relaxed);
                    let reason = "parent copy failed; sidecar not copied".to_string();
                    send_complete(
                        tx,
                        cancel,
                        provisional_file(source_id, sidecar, MediaKind::Sidecar),
                        CopyOutcome::Failed { error: reason },
                    )
                    .await;
                }
            }
            let _ = tokio::fs::remove_dir_all(&seq_dir).await;
            return;
        }
    };

    let mut sidecars = Vec::new();
    for s in &unit.sidecars {
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        match copy_one_to_staging(cfg, source, s, MediaKind::Sidecar, &seq_dir, tx, cancel).await {
            Some(sf) => sidecars.push(sf),
            None => {
                if !cancel.load(Ordering::Relaxed) {
                    counters.failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    if cancel.load(Ordering::Relaxed) {
        let _ = tokio::fs::remove_dir_all(&seq_dir).await;
        return;
    }

    // EXIF the staged parent on local disk (single-file read on the shared
    // stay-open process; cheap because there's no USB seek).
    let meta = match exif {
        Some(e) => e
            .read_batch(std::slice::from_ref(&parent.staged_path))
            .await
            .ok()
            .and_then(|v| v.into_iter().next())
            .unwrap_or_default(),
        None => FileMetadata::default(),
    };
    let synthetic = SourceFile {
        rel_path: parent.rel_path.clone(),
        size: parent.size,
        mtime: parent.mtime,
        extension: String::new(),
        backend_handle: BackendHandle::Path(parent.staged_path.clone()),
    };

    match metadata::resolve_one(&synthetic, &meta) {
        None => {
            place_no_date(source_id, &parent, tx, counters, cancel).await;
            for s in &sidecars {
                place_no_date(source_id, s, tx, counters, cancel).await;
            }
        }
        Some(rm) => {
            if rm.source.is_fallback() {
                let _ = tx
                    .send(EngineEvent::Warning {
                        message: format!(
                            "{}: using {} (no DateTimeOriginal)",
                            parent.rel_path,
                            rm.source.label()
                        ),
                        rel_path: Some(parent.rel_path.clone()),
                    })
                    .await;
            }
            place_one(
                cfg,
                source_id,
                dedupe,
                &parent,
                unit.parent_kind,
                &rm,
                tx,
                counters,
                cancel,
            )
            .await;
            for s in &sidecars {
                place_one(
                    cfg,
                    source_id,
                    dedupe,
                    s,
                    unit.parent_kind,
                    &rm,
                    tx,
                    counters,
                    cancel,
                )
                .await;
            }
        }
    }

    // The staging subdir is now empty (everything moved or deleted).
    let _ = tokio::fs::remove_dir_all(&seq_dir).await;
}

/// Dry run: resolve copy units' dates over USB (the old exiftool path) and
/// report what *would* happen, writing nothing. Keeps exact destinations for
/// the `scan` / `--dry-run` previews.
async fn run_execute_dry(
    cfg: EngineConfig,
    plan: SyncPlan,
    source: Arc<dyn MediaSource>,
    tx: mpsc::Sender<EngineEvent>,
) -> Result<()> {
    let mut tally = Tally::default();
    for item in plan
        .items
        .iter()
        .filter(|i| i.action != PlannedAction::Copy)
    {
        emit_decided(&tx, item.clone(), &mut tally).await;
    }

    let dated = resolve_units_over_usb(&cfg, source.as_ref(), &plan.units, &tx).await?;
    for item in dated.items {
        match item.action {
            PlannedAction::Copy => {
                tally.copied += 1;
                let _ = tx
                    .send(EngineEvent::CopyStarted { file: item.clone() })
                    .await;
                let bytes = item.source_size;
                let _ = tx
                    .send(EngineEvent::CopyComplete {
                        file: item,
                        outcome: CopyOutcome::Copied { bytes },
                    })
                    .await;
            }
            _ => emit_decided(&tx, item, &mut tally).await,
        }
    }

    let _ = tx
        .send(EngineEvent::SyncSummary {
            copied: tally.copied,
            skipped: tally.skipped,
            failed: tally.failed,
        })
        .await;
    Ok(())
}

#[derive(Default)]
struct Tally {
    copied: u64,
    skipped: u64,
    failed: u64,
}

/// Emit a CopyComplete for an already-decided (non-copy) planned item and
/// bump the matching counter.
async fn emit_decided(tx: &mpsc::Sender<EngineEvent>, item: PlannedFile, tally: &mut Tally) {
    match item.action {
        PlannedAction::Error => {
            tally.failed += 1;
            let error = item
                .reason
                .clone()
                .unwrap_or_else(|| "planning error".to_string());
            let _ = tx
                .send(EngineEvent::CopyComplete {
                    file: item,
                    outcome: CopyOutcome::Failed { error },
                })
                .await;
        }
        _ => {
            tally.skipped += 1;
            let reason = item.reason.clone().unwrap_or_else(|| "skipped".to_string());
            let _ = tx
                .send(EngineEvent::CopyComplete {
                    file: item,
                    outcome: CopyOutcome::Skipped { reason },
                })
                .await;
        }
    }
}

/// A file copied into staging, awaiting EXIF + placement.
struct StagedFile {
    staged_path: PathBuf,
    rel_path: String,
    size: u64,
    kind: MediaKind,
    mtime: Option<chrono::NaiveDateTime>,
}

/// Copy a single file into `seq_dir`, emitting CopyStarted/CopyProgress and, on
/// failure, CopyComplete{Failed}. Returns the staged file on success. A failed
/// `CopyStarted` send (consumer gone) sets `cancel` and returns `None`.
async fn copy_one_to_staging(
    cfg: &EngineConfig,
    source: &dyn MediaSource,
    f: &SourceFile,
    kind: MediaKind,
    seq_dir: &std::path::Path,
    tx: &mpsc::Sender<EngineEvent>,
    cancel: &std::sync::atomic::AtomicBool,
) -> Option<StagedFile> {
    if tx
        .send(EngineEvent::CopyStarted {
            file: provisional_file(source.id(), f, kind),
        })
        .await
        .is_err()
    {
        cancel.store(true, std::sync::atomic::Ordering::Relaxed);
        return None;
    }

    let src = match source.full_path(f).await {
        Ok(p) => p,
        Err(e) => {
            let _ = tx
                .send(EngineEvent::CopyComplete {
                    file: provisional_file(source.id(), f, kind),
                    outcome: CopyOutcome::Failed {
                        error: format!("{e}"),
                    },
                })
                .await;
            return None;
        }
    };

    let staged = seq_dir.join(file_name_of(&f.rel_path));
    let rel = f.rel_path.clone();
    let tx2 = tx.clone();
    let res = crate::copy::copy_atomic(&src, &staged, f.size, &cfg.verify, move |done, total| {
        let _ = tx2.try_send(EngineEvent::CopyProgress {
            rel_path: rel.clone(),
            bytes_done: done,
            bytes_total: total,
        });
    })
    .await;

    match res {
        Ok(_) => Some(StagedFile {
            staged_path: staged,
            rel_path: f.rel_path.clone(),
            size: f.size,
            kind,
            mtime: f.mtime,
        }),
        Err(e) => {
            let _ = tx
                .send(EngineEvent::CopyComplete {
                    file: provisional_file(source.id(), f, kind),
                    outcome: CopyOutcome::Failed {
                        error: format!("{e}"),
                    },
                })
                .await;
            None
        }
    }
}

/// Date, dedupe and move one staged file into its final destination, using
/// `route_kind` to pick the root (sidecars route with their parent) and `rm`
/// for the capture date. Updates the shared `counters` and emits CopyComplete.
#[allow(clippy::too_many_arguments)]
async fn place_one(
    cfg: &EngineConfig,
    source_id: &SourceId,
    dedupe: &tokio::sync::Mutex<DedupeCache>,
    f: &StagedFile,
    route_kind: MediaKind,
    rm: &ResolvedMetadata,
    tx: &mpsc::Sender<EngineEvent>,
    counters: &Counters,
    cancel: &std::sync::atomic::AtomicBool,
) {
    use std::sync::atomic::Ordering;

    let name = file_name_of(&f.rel_path);
    let dest = match plan::dest_path_for(cfg, route_kind, &rm.datetime, name) {
        Ok(d) => d,
        Err(e) => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            let _ = tokio::fs::remove_file(&f.staged_path).await;
            send_complete(
                tx,
                cancel,
                placed_file(source_id, f, None, Some(rm), PlannedAction::Error, None),
                CopyOutcome::Failed {
                    error: format!("{e}"),
                },
            )
            .await;
            return;
        }
    };

    // Reserving under the cache lock makes a destination visible to later
    // units before this worker releases the lock to move its staged file.
    let (action, reason) = dedupe.lock().await.reserve(&dest, name, f.size);
    match action {
        PlannedAction::Copy => match crate::copy::finalize_move(&f.staged_path, &dest).await {
            Ok(()) => {
                counters.copied.fetch_add(1, Ordering::Relaxed);
                send_complete(
                    tx,
                    cancel,
                    placed_file(
                        source_id,
                        f,
                        Some(dest),
                        Some(rm),
                        PlannedAction::Copy,
                        None,
                    ),
                    CopyOutcome::Copied { bytes: f.size },
                )
                .await;
            }
            Err(e) => {
                counters.failed.fetch_add(1, Ordering::Relaxed);
                let _ = tokio::fs::remove_file(&f.staged_path).await;
                send_complete(
                    tx,
                    cancel,
                    placed_file(
                        source_id,
                        f,
                        Some(dest),
                        Some(rm),
                        PlannedAction::Error,
                        None,
                    ),
                    CopyOutcome::Failed {
                        error: format!("{e}"),
                    },
                )
                .await;
            }
        },
        PlannedAction::Error => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
            let _ = tokio::fs::remove_file(&f.staged_path).await;
            let error = reason
                .clone()
                .unwrap_or_else(|| "destination conflict".into());
            send_complete(
                tx,
                cancel,
                placed_file(
                    source_id,
                    f,
                    Some(dest),
                    Some(rm),
                    PlannedAction::Error,
                    reason,
                ),
                CopyOutcome::Failed { error },
            )
            .await;
        }
        _ => {
            counters.skipped.fetch_add(1, Ordering::Relaxed);
            let _ = tokio::fs::remove_file(&f.staged_path).await;
            let r = reason.clone().unwrap_or_else(|| "already exists".into());
            send_complete(
                tx,
                cancel,
                placed_file(source_id, f, Some(dest), Some(rm), action, reason),
                CopyOutcome::Skipped { reason: r },
            )
            .await;
        }
    }
}

/// A staged file we couldn't date at all (no EXIF, no mtime). Discard the
/// staged copy and report it skipped.
async fn place_no_date(
    source_id: &SourceId,
    f: &StagedFile,
    tx: &mpsc::Sender<EngineEvent>,
    counters: &Counters,
    cancel: &std::sync::atomic::AtomicBool,
) {
    counters
        .skipped
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _ = tokio::fs::remove_file(&f.staged_path).await;
    let reason = "no usable date (no EXIF and no mtime)".to_string();
    send_complete(
        tx,
        cancel,
        placed_file(
            source_id,
            f,
            None,
            None,
            PlannedAction::SkipNoDate,
            Some(reason.clone()),
        ),
        CopyOutcome::Skipped { reason },
    )
    .await;
}

/// Resolve copy units' dates over USB and date them via [`plan::build_plan`].
/// Used by the dry-run / scan preview, which must show exact destinations
/// without copying anything.
async fn resolve_units_over_usb(
    cfg: &EngineConfig,
    source: &dyn MediaSource,
    units: &[CopyUnit],
    tx: &mpsc::Sender<EngineEvent>,
) -> Result<SyncPlan> {
    let mut flat: Vec<(SourceFile, MediaKind)> = Vec::new();
    for u in units {
        flat.push((u.parent.clone(), u.parent_kind));
        for s in &u.sidecars {
            flat.push((s.clone(), MediaKind::Sidecar));
        }
    }

    let mut paths: Vec<PathBuf> = Vec::with_capacity(flat.len());
    for (f, _) in &flat {
        paths.push(source.full_path(f).await?);
    }

    let exif = ExifTool::spawn().await?;
    let batch_size = cfg.performance.metadata_batch_size.max(1);
    let total = flat.len() as u64;
    let mut metas: Vec<Option<ResolvedMetadata>> = Vec::with_capacity(flat.len());
    for chunk in paths.chunks(batch_size) {
        let raw = exif.read_batch(chunk).await?;
        for m in &raw {
            let idx = metas.len();
            metas.push(metadata::resolve_one(&flat[idx].0, m));
        }
        let _ = tx
            .send(EngineEvent::MetadataProgress {
                done: metas.len() as u64,
                total,
            })
            .await;
    }
    let _ = exif.shutdown().await;

    let scanned: Vec<ScannedFile> = flat
        .into_iter()
        .zip(metas)
        .map(|((file, kind), metadata)| ScannedFile {
            source: source.id().clone(),
            file,
            kind,
            metadata,
        })
        .collect();
    plan::build_plan(cfg, scanned)
}

/// Provisional planned row for in-flight copy events (dest unknown until placed).
fn provisional_file(source_id: &SourceId, f: &SourceFile, kind: MediaKind) -> PlannedFile {
    PlannedFile {
        source: source_id.clone(),
        source_rel_path: f.rel_path.clone(),
        source_size: f.size,
        kind: MediaKindWire::from(kind),
        action: PlannedAction::Copy,
        dest_path: None,
        datetime: f.mtime,
        date_source: f
            .mtime
            .map(|_| DateSourceWire::from(crate::metadata::DateSource::FileMtime)),
        reason: None,
    }
}

/// Final planned row for a placed (or skipped/errored) staged file.
fn placed_file(
    source_id: &SourceId,
    f: &StagedFile,
    dest: Option<PathBuf>,
    rm: Option<&ResolvedMetadata>,
    action: PlannedAction,
    reason: Option<String>,
) -> PlannedFile {
    PlannedFile {
        source: source_id.clone(),
        source_rel_path: f.rel_path.clone(),
        source_size: f.size,
        kind: MediaKindWire::from(f.kind),
        action,
        dest_path: dest,
        datetime: rm.map(|r| r.datetime),
        date_source: rm.map(|r| DateSourceWire::from(r.source)),
        reason,
    }
}

fn file_name_of(rel_path: &str) -> &str {
    rel_path.rsplit('/').next().unwrap_or(rel_path)
}

/// Remove any stale contents of a staging directory (crash recovery). The dir
/// itself is recreated lazily by the first copy.
fn sweep_staging(dir: &std::path::Path) {
    if dir.exists() {
        let _ = std::fs::remove_dir_all(dir);
    }
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

/// Acquire non-blocking advisory locks for both destination roots. Holding the
/// returned files prevents a second ImageSync process from sweeping or sharing
/// either staging directory during this import.
fn lock_destination_roots(
    images_root: &std::path::Path,
    videos_root: &std::path::Path,
) -> Result<Vec<std::fs::File>> {
    let mut roots = vec![images_root.to_path_buf(), videos_root.to_path_buf()];
    roots.sort();
    roots.dedup();

    roots
        .into_iter()
        .map(|root| {
            let lock_path = root.join(".imagesync.lock");
            let file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&lock_path)
                .map_err(|e| crate::error::Error::io(&lock_path, e))?;
            file.try_lock_exclusive().map_err(|e| {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    crate::error::Error::DestinationLocked { path: root }
                } else {
                    crate::error::Error::io(&lock_path, e)
                }
            })?;
            Ok(file)
        })
        .collect()
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

    #[tokio::test]
    async fn finalize_move_renames_and_creates_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let staged = tmp.path().join("staging").join("a.bin");
        std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
        std::fs::write(&staged, b"payload").unwrap();
        let dest = tmp.path().join("2026").join("2026-05-03").join("a.bin");

        crate::copy::finalize_move(&staged, &dest).await.unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"payload");
        assert!(!staged.exists(), "staged file should be gone after move");
    }

    #[test]
    fn exdev_is_recognised_as_cross_device() {
        // The EXDEV fallback in finalize_move keys off this predicate.
        let err = std::io::Error::from_raw_os_error(18);
        assert!(crate::copy::is_cross_device_for_test(&err));
        let other = std::io::Error::from_raw_os_error(2); // ENOENT
        assert!(!crate::copy::is_cross_device_for_test(&other));
    }

    /// End-to-end: cheap scan → staging execute. Dates fall back to mtime (no
    /// real EXIF needed), so this runs with or without exiftool installed.
    #[tokio::test]
    async fn staging_sync_places_dedupes_and_cleans_up() {
        use crate::config::{FiltersConfig, PerformanceConfig, VerifyConfig};
        use crate::source::FilesystemSource;
        use tokio_stream::StreamExt;

        let tmp = tempfile::tempdir().unwrap();
        let src_root = tmp.path().join("card");
        let images = tmp.path().join("images");
        let videos = tmp.path().join("videos");
        std::fs::create_dir_all(src_root.join("DCIM")).unwrap();
        std::fs::create_dir_all(&images).unwrap();
        std::fs::create_dir_all(&videos).unwrap();

        let jpg = src_root.join("DCIM/IMG001.JPG");
        let xmp = src_root.join("DCIM/IMG001.XMP");
        let mp4 = src_root.join("DCIM/VID001.MP4");
        std::fs::write(&jpg, vec![1u8; 100]).unwrap();
        std::fs::write(&xmp, vec![2u8; 20]).unwrap();
        std::fs::write(&mp4, vec![3u8; 500]).unwrap();

        let cfg = EngineConfig {
            images_root: images.clone(),
            videos_root: videos.clone(),
            images_template: "{yyyy}/{yyyy}-{mm}-{dd}".to_string(),
            videos_template: "{yyyy}/{yyyy}-{mm}-{dd}".to_string(),
            filters: FiltersConfig::default(),
            performance: PerformanceConfig::default(),
            verify: VerifyConfig::default(),
            template_warnings: Vec::new(),
        };
        let registry = ProfileRegistry::with_builtins().unwrap();

        let run = |cfg: EngineConfig| {
            let registry = registry.clone();
            let src_root = src_root.clone();
            async move {
                let engine = Engine::new(cfg, registry);
                let source = FilesystemSource::new("card", "card", src_root).into_arc();
                let (handle, mut stream) = engine.scan_and_plan(source.clone());
                let drain = tokio::spawn(async move { while stream.next().await.is_some() {} });
                let plan = handle.await.unwrap().unwrap();
                drain.await.unwrap();

                let mut exec = engine.execute(plan, source, ExecuteOptions::default());
                let mut summary = (0u64, 0u64, 0u64);
                while let Some(ev) = exec.next().await {
                    if let EngineEvent::SyncSummary {
                        copied,
                        skipped,
                        failed,
                    } = ev
                    {
                        summary = (copied, skipped, failed);
                    }
                }
                summary
            }
        };

        // First run: 3 files copied, none failed.
        let (copied, _skipped, failed) = run(cfg.clone()).await;
        assert_eq!(copied, 3, "expected IMG, XMP, MP4 copied");
        assert_eq!(failed, 0);

        // Files landed in date folders under the right roots.
        let img_idx = plan::DestIndex::build(&[images.as_path()]);
        let vid_idx = plan::DestIndex::build(&[videos.as_path()]);
        let jpg_dest = img_idx.lookup("IMG001.JPG", 100).expect("jpg placed");
        let xmp_dest = img_idx.lookup("IMG001.XMP", 20).expect("xmp placed");
        assert!(vid_idx.lookup("VID001.MP4", 500).is_some(), "mp4 placed");
        // Sidecar lands beside its parent image.
        assert_eq!(jpg_dest.parent(), xmp_dest.parent());

        // Staging areas are clean (no leftover files).
        for root in [&images, &videos] {
            let staging = root.join(plan::STAGING_DIR_NAME);
            if staging.exists() {
                let n = std::fs::read_dir(&staging).unwrap().count();
                assert_eq!(n, 0, "staging dir should be empty: {}", staging.display());
            }
        }

        // Second run: everything dedupes; nothing copied.
        let (copied2, _skipped2, failed2) = run(cfg).await;
        assert_eq!(copied2, 0, "re-run should copy nothing (deduped)");
        assert_eq!(failed2, 0);
    }

    /// Aborting a sync (the UI drops the event stream) must make the engine
    /// stop and leave no files in the staging area.
    #[tokio::test]
    async fn parent_copy_failure_reports_each_sidecar() {
        use crate::config::{FiltersConfig, PerformanceConfig, VerifyConfig};
        use crate::source::FilesystemSource;
        use std::sync::atomic::AtomicBool;
        use tokio::sync::Mutex;

        let tmp = tempfile::tempdir().unwrap();
        let images = tmp.path().join("images");
        let videos = tmp.path().join("videos");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::create_dir_all(&videos).unwrap();
        let cfg = EngineConfig {
            images_root: images,
            videos_root: videos,
            images_template: "{yyyy}/{yyyy}-{mm}-{dd}".to_string(),
            videos_template: "{yyyy}/{yyyy}-{mm}-{dd}".to_string(),
            filters: FiltersConfig::default(),
            performance: PerformanceConfig::default(),
            verify: VerifyConfig::default(),
            template_warnings: Vec::new(),
        };
        let source = FilesystemSource::new("card", "card", tmp.path().to_path_buf()).into_arc();
        let invalid = |rel_path: &str, size| SourceFile {
            rel_path: rel_path.to_string(),
            size,
            mtime: None,
            extension: "jpg".to_string(),
            backend_handle: BackendHandle::PtpObject(1),
        };
        let unit = CopyUnit {
            parent: invalid("DCIM/IMG001.JPG", 100),
            parent_kind: MediaKind::Image { raw: false },
            sidecars: vec![invalid("DCIM/IMG001.XMP", 10)],
        };
        let (tx, mut rx) = mpsc::channel(8);
        let dedupe = Mutex::new(DedupeCache::new());
        let counters = Counters::default();
        let cancel = AtomicBool::new(false);

        process_unit(
            &cfg,
            source.as_ref(),
            source.id(),
            None,
            &dedupe,
            unit,
            0,
            &tx,
            &counters,
            &cancel,
        )
        .await;
        drop(tx);

        let failed: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
            .filter_map(|event| match event {
                EngineEvent::CopyComplete {
                    file,
                    outcome: CopyOutcome::Failed { .. },
                } => Some(file.source_rel_path),
                _ => None,
            })
            .collect();
        assert_eq!(failed, vec!["DCIM/IMG001.JPG", "DCIM/IMG001.XMP"]);
        assert_eq!(
            counters.failed.load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    #[tokio::test]
    async fn aborted_sync_cleans_up_staging() {
        use crate::config::{FiltersConfig, PerformanceConfig, VerifyConfig};
        use crate::source::FilesystemSource;

        let tmp = tempfile::tempdir().unwrap();
        let src_root = tmp.path().join("card");
        let images = tmp.path().join("images");
        let videos = tmp.path().join("videos");
        std::fs::create_dir_all(src_root.join("DCIM")).unwrap();
        std::fs::create_dir_all(&images).unwrap();
        std::fs::create_dir_all(&videos).unwrap();
        for i in 0..40 {
            std::fs::write(
                src_root.join(format!("DCIM/IMG{i:04}.JPG")),
                vec![7u8; 4096],
            )
            .unwrap();
        }

        let cfg = EngineConfig {
            images_root: images.clone(),
            videos_root: videos.clone(),
            images_template: "{yyyy}/{yyyy}-{mm}-{dd}".to_string(),
            videos_template: "{yyyy}/{yyyy}-{mm}-{dd}".to_string(),
            filters: FiltersConfig::default(),
            performance: PerformanceConfig::default(), // copy_workers = 1
            verify: VerifyConfig::default(),
            template_warnings: Vec::new(),
        };
        let registry = ProfileRegistry::with_builtins().unwrap();
        let engine = Engine::new(cfg, registry);
        let source = FilesystemSource::new("card", "card", src_root).into_arc();

        // Build the plan, then start executing and immediately drop the event
        // stream to simulate the UI aborting.
        let (handle, mut stream) = engine.scan_and_plan(source.clone());
        {
            use tokio_stream::StreamExt;
            while stream.next().await.is_some() {}
        }
        let plan = handle.await.unwrap().unwrap();

        let exec = engine.execute(plan, source, ExecuteOptions::default());
        drop(exec); // consumer gone → engine should cancel and clean up

        // The detached engine task finishes within a copy or two; poll for the
        // staging area to be emptied.
        let staging_clean = |root: &std::path::Path| {
            let s = root.join(plan::STAGING_DIR_NAME);
            !s.exists()
                || std::fs::read_dir(&s)
                    .map(|d| d.count() == 0)
                    .unwrap_or(true)
        };
        let mut cleaned = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if staging_clean(&images) && staging_clean(&videos) {
                cleaned = true;
                break;
            }
        }
        assert!(cleaned, "staging area was not cleaned up after abort");

        // Abort should have stopped early — not all 40 files placed.
        let placed = plan::DestIndex::build(&[images.as_path()]).len();
        assert!(
            placed < 40,
            "abort should stop early, but placed {placed}/40"
        );
    }
}
