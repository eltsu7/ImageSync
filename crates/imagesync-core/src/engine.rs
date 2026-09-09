//! High-level engine: scan → plan → execute, emitting [`EngineEvent`]s.

use fs2::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::classify::MediaKind;
use crate::config::EngineConfig;
use crate::error::{Error, Result};
use crate::events::{
    DateSourceWire, DetectedProfile, EngineEvent, FileId, FileOutcome, FilePhase, MediaKindWire,
    OperationErrorKind, PlanSummary, PlannedAction, PlannedFile, SyncSummary, WarningKind,
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
/// A cooperative cancellation signal shared by an operation and its caller.
#[derive(Debug, Clone)]
pub struct CancellationHandle {
    cancelled: Arc<AtomicBool>,
}

impl CancellationHandle {
    fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Request cancellation. The operation completes after its current safe
    /// boundary and any required staging cleanup.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// A running engine operation. Completion is authoritative; event-stream
/// closure alone never indicates whether the operation succeeded.
pub struct Operation<T> {
    pub events: ReceiverStream<EngineEvent>,
    pub completion: JoinHandle<Result<T>>,
    pub cancellation: CancellationHandle,
}

/// The scan output used to begin execution.
#[derive(Debug, Clone)]
pub struct ScanResult {
    pub plan: SyncPlan,
    pub profile: DetectedProfile,
}

fn operation_error_kind(error: &Error) -> OperationErrorKind {
    match error {
        Error::Cancelled => OperationErrorKind::Cancelled,
        Error::Config(_)
        | Error::Template { .. }
        | Error::Profile(_)
        | Error::TomlDe(_)
        | Error::TomlSer(_) => OperationErrorKind::Configuration,
        Error::DestRootMissing { .. }
        | Error::DestRootNotDir { .. }
        | Error::DestinationLocked { .. } => OperationErrorKind::Destination,
        Error::Source(_) => OperationErrorKind::Source,
        Error::ExiftoolMissing | Error::Exiftool(_) | Error::Metadata { .. } => {
            OperationErrorKind::ExifTool
        }
        Error::Io { .. } | Error::IoBare(_) => OperationErrorKind::Io,
        Error::Json(_) | Error::Internal(_) => OperationErrorKind::Internal,
    }
}

async fn emit_event(
    tx: &mpsc::Sender<EngineEvent>,
    cancellation: &CancellationHandle,
    event: EngineEvent,
) -> bool {
    if tx.send(event).await.is_err() {
        cancellation.cancel();
        false
    } else {
        true
    }
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

    /// Spawn a scan + plan operation. Consume events concurrently with awaiting
    /// [`Operation::completion`] so event backpressure cannot delay completion.
    pub fn scan_and_plan(&self, source: Arc<dyn MediaSource>) -> Operation<ScanResult> {
        let (tx, rx) = mpsc::channel::<EngineEvent>(256);
        let cfg = self.config.clone();
        let registry = self.registry.clone();
        let cancellation = CancellationHandle::new();
        let task_cancellation = cancellation.clone();
        let tx2 = tx.clone();
        let completion = tokio::spawn(async move {
            let result = run_scan_and_plan(
                cfg,
                registry,
                source,
                tx2.clone(),
                task_cancellation.clone(),
            )
            .await;
            if let Err(error) = &result {
                emit_event(
                    &tx2,
                    &task_cancellation,
                    EngineEvent::Error {
                        kind: operation_error_kind(error),
                        message: error.to_string(),
                        file: None,
                    },
                )
                .await;
            }
            result
        });
        drop(tx);
        Operation {
            events: ReceiverStream::new(rx),
            completion,
            cancellation,
        }
    }

    /// Execute a plan and return an operation whose completion contains the
    /// authoritative final summary.
    pub fn execute(
        &self,
        plan: SyncPlan,
        source: Arc<dyn MediaSource>,
        opts: ExecuteOptions,
    ) -> Operation<SyncSummary> {
        let (tx, rx) = mpsc::channel::<EngineEvent>(256);
        let cfg = self.config.clone();
        let cancellation = CancellationHandle::new();
        let task_cancellation = cancellation.clone();
        let tx2 = tx.clone();
        let completion = tokio::spawn(async move {
            let result = run_execute(
                cfg,
                plan,
                source,
                opts,
                tx2.clone(),
                task_cancellation.clone(),
            )
            .await;
            if let Err(error) = &result {
                emit_event(
                    &tx2,
                    &task_cancellation,
                    EngineEvent::Error {
                        kind: operation_error_kind(error),
                        message: error.to_string(),
                        file: None,
                    },
                )
                .await;
            }
            result
        });
        drop(tx);
        Operation {
            events: ReceiverStream::new(rx),
            completion,
            cancellation,
        }
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
    cancellation: CancellationHandle,
) -> Result<ScanResult> {
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }

    // Pre-flight: refuse to run if the destination roots aren't reachable.
    // This catches the common "drive isn't mounted" footgun where every
    // file would otherwise look new, get copied to an empty mount point,
    // and disappear when the real drive mounts. Bare existence check —
    // empty directories are accepted (legitimate first-time setup).
    check_dest_root(&cfg.images_root, "images_root")?;
    check_dest_root(&cfg.videos_root, "videos_root")?;
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }

    if !emit_event(
        &tx,
        &cancellation,
        EngineEvent::ScanStarted {
            source: source.id().clone(),
            display_name: source.display_name().to_string(),
        },
    )
    .await
    {
        return Err(Error::Cancelled);
    }

    // Detect profile before enumeration so frontends can identify the source
    // while a potentially slow source listing is in flight.
    let profile = registry.detect_for_source(source.root_path());
    let detected_profile = DetectedProfile {
        id: profile.id.clone(),
        display_name: profile.display_name.clone(),
    };
    if !emit_event(
        &tx,
        &cancellation,
        EngineEvent::ProfileDetected {
            profile: detected_profile.clone(),
        },
    )
    .await
        || cancellation.is_cancelled()
    {
        return Err(Error::Cancelled);
    }

    // Enumerate files.
    let t_list_start = std::time::Instant::now();
    let files = source.list_files().await?;
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    let file_count = files.len() as u64;
    if !emit_event(
        &tx,
        &cancellation,
        EngineEvent::ScanProgress {
            source: source.id().clone(),
            files_seen: file_count,
        },
    )
    .await
        || !emit_event(
            &tx,
            &cancellation,
            EngineEvent::ScanComplete {
                source: source.id().clone(),
                files: file_count,
            },
        )
        .await
        || cancellation.is_cancelled()
    {
        return Err(Error::Cancelled);
    }
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
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    tracing::debug!(
        indexed = dest_index.len(),
        elapsed_ms = t_index_start.elapsed().as_millis() as u64,
        "built destination index for pre-skip"
    );

    // Cheap plan: classify + filter + pre-skip + sidecar pairing. No exiftool.
    let t_plan_start = std::time::Instant::now();
    let cheap = plan::cheap_plan(&cfg, profile, source.id(), files, &dest_index);
    let plan = cheap.into_sync_plan(source.id());
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    tracing::debug!(
        profile = profile.id,
        units = plan.units.len(),
        elapsed_ms = t_plan_start.elapsed().as_millis() as u64,
        "cheap plan built (no exiftool)"
    );

    let summary = PlanSummary {
        copies: plan.copies(),
        skips: plan.skips(),
        errors: plan.errors(),
    };
    if !emit_event(&tx, &cancellation, EngineEvent::PlanReady { summary }).await
        || cancellation.is_cancelled()
    {
        return Err(Error::Cancelled);
    }

    Ok(ScanResult {
        plan,
        profile: detected_profile,
    })
}

async fn run_execute(
    cfg: EngineConfig,
    plan: SyncPlan,
    source: Arc<dyn MediaSource>,
    opts: ExecuteOptions,
    tx: mpsc::Sender<EngineEvent>,
    cancellation: CancellationHandle,
) -> Result<SyncSummary> {
    let total = plan.items.len() as u64;
    let total_bytes = plan
        .items
        .iter()
        .filter(|item| item.action == PlannedAction::Copy)
        .map(|item| item.source_size)
        .sum();

    // Plans can be persisted/replayed and roots can be unmounted between scan
    // and execute. Validate before taking locks so we never create a lock file
    // under an unintended or missing destination.
    check_dest_root(&cfg.images_root, "images_root")?;
    check_dest_root(&cfg.videos_root, "videos_root")?;
    let _destination_locks = if opts.dry_run {
        None
    } else {
        Some(lock_destination_roots(&cfg.images_root, &cfg.videos_root)?)
    };

    emit_event(
        &tx,
        &cancellation,
        EngineEvent::ExecutionStarted {
            total_files: total,
            total_bytes,
            dry_run: opts.dry_run,
        },
    )
    .await;

    if !cancellation.is_cancelled() {
        for message in &cfg.template_warnings {
            if !emit_event(
                &tx,
                &cancellation,
                EngineEvent::Warning {
                    kind: WarningKind::Template,
                    message: message.clone(),
                    file: None,
                },
            )
            .await
            {
                break;
            }
        }
    }

    if opts.dry_run {
        run_execute_dry(cfg, plan, source, tx, cancellation, total).await
    } else {
        run_execute_staging(cfg, plan, source, tx, cancellation, total).await
    }
}

/// Shared, lock-free counters for the staging executor's worker tasks.
#[derive(Default)]
struct Counters {
    copied: AtomicU64,
    skipped: AtomicU64,
    failed: AtomicU64,
}

impl Counters {
    fn summary(&self, total: u64, cancelled: bool) -> SyncSummary {
        SyncSummary {
            total,
            copied: self.copied.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            cancelled,
        }
    }
}

/// Real sync via the copy→exif→move pipeline.
///
/// Each unit is handled end-to-end by a worker (up to `copy_workers` at once):
/// copy parent + sidecars to a per-root staging dir, read EXIF from the **local**
/// staged copy, date + dedupe, then `rename` into the final date folder. The USB
/// is read once, sequentially; the seeky EXIF reads land on local disk.
async fn run_execute_staging(
    cfg: EngineConfig,
    plan: SyncPlan,
    source: Arc<dyn MediaSource>,
    tx: mpsc::Sender<EngineEvent>,
    cancellation: CancellationHandle,
    total: u64,
) -> Result<SyncSummary> {
    use tokio::sync::{Mutex, Semaphore};

    let counters = Arc::new(Counters::default());

    // Sweep stale staging before starting, including when cancellation was
    // already requested.
    sweep_staging(&cfg.images_root.join(plan::STAGING_DIR_NAME));
    sweep_staging(&cfg.videos_root.join(plan::STAGING_DIR_NAME));

    // Emit already-decided (non-copy) rows up front.
    for item in plan
        .items
        .iter()
        .filter(|item| item.action != PlannedAction::Copy)
    {
        if cancellation.is_cancelled() {
            break;
        }
        emit_decided(&tx, item.clone(), &counters, total, &cancellation).await;
    }

    let mut worker_error = None;

    if !cancellation.is_cancelled() {
        // One shared exiftool process (local reads). Workers serialize on its
        // internal lock; we fall back to mtime dating if it can't spawn.
        let exif = Arc::new(match ExifTool::spawn().await {
            Ok(exif) => Some(exif),
            Err(error) => {
                emit_event(
                    &tx,
                    &cancellation,
                    EngineEvent::Warning {
                        kind: WarningKind::ExifToolUnavailable,
                        message: format!(
                            "exiftool unavailable, dating by file mtime only: {error}"
                        ),
                        file: None,
                    },
                )
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
            if cancellation.is_cancelled() {
                break;
            }
            let permit = match sem.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            if cancellation.is_cancelled() {
                drop(permit);
                break;
            }
            let cfg = cfg.clone();
            let source = source.clone();
            let source_id = source_id.clone();
            let exif = exif.clone();
            let dedupe = dedupe.clone();
            let tx = tx.clone();
            let counters = counters.clone();
            let cancellation = cancellation.clone();
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
                    &cancellation,
                    total,
                )
                .await;
                drop(permit);
            }));
        }
        for join in joins {
            if let Err(error) = join.await {
                tracing::error!(%error, "staging worker terminated unexpectedly");
                worker_error.get_or_insert(error);
            }
        }

        if let Some(exif) = Arc::into_inner(exif).flatten() {
            let _ = exif.shutdown().await;
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

    // Always clean up staging — receiver loss and explicit cancellation both
    // wait for the workers before this cleanup and final completion result.
    sweep_staging(&cfg.images_root.join(plan::STAGING_DIR_NAME));
    sweep_staging(&cfg.videos_root.join(plan::STAGING_DIR_NAME));

    if let Some(error) = worker_error {
        return Err(Error::Internal(format!(
            "staging worker terminated unexpectedly: {error}"
        )));
    }

    let summary = counters.summary(total, cancellation.is_cancelled());
    emit_event(
        &tx,
        &cancellation,
        EngineEvent::ExecutionFinished { summary },
    )
    .await;
    Ok(summary)
}

async fn send_file_started(
    tx: &mpsc::Sender<EngineEvent>,
    cancellation: &CancellationHandle,
    file: PlannedFile,
) -> bool {
    emit_event(tx, cancellation, EngineEvent::FileStarted { file }).await
}

async fn send_phase(
    tx: &mpsc::Sender<EngineEvent>,
    cancellation: &CancellationHandle,
    file: FileId,
    phase: FilePhase,
) -> bool {
    emit_event(
        tx,
        cancellation,
        EngineEvent::FilePhaseChanged { file, phase },
    )
    .await
}

/// Emit a terminal file outcome and the matching authoritative aggregate
/// progress update. Terminal outcomes are counted even if cancellation arrives
/// while a worker is completing its current safe unit.
async fn send_file_finished(
    tx: &mpsc::Sender<EngineEvent>,
    cancellation: &CancellationHandle,
    counters: &Counters,
    total: u64,
    file: PlannedFile,
    outcome: FileOutcome,
) {
    match &outcome {
        FileOutcome::Copied { .. } => {
            counters.copied.fetch_add(1, Ordering::Relaxed);
        }
        FileOutcome::Skipped { .. } => {
            counters.skipped.fetch_add(1, Ordering::Relaxed);
        }
        FileOutcome::Failed { .. } => {
            counters.failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    if !emit_event(
        tx,
        cancellation,
        EngineEvent::FileFinished { file, outcome },
    )
    .await
    {
        return;
    }

    let summary = counters.summary(total, cancellation.is_cancelled());
    emit_event(tx, cancellation, EngineEvent::ExecutionProgress { summary }).await;
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
    cancellation: &CancellationHandle,
    total: u64,
) {
    let root = match unit.parent_kind {
        MediaKind::Video => &cfg.videos_root,
        _ => &cfg.images_root,
    };
    let seq_dir = root.join(plan::STAGING_DIR_NAME).join(seq.to_string());

    // Copy the parent first; a sidecar cannot be dated without it.
    let parent = match copy_one_to_staging(
        cfg,
        source,
        &unit.parent,
        unit.parent_kind,
        &seq_dir,
        tx,
        counters,
        total,
        cancellation,
    )
    .await
    {
        Some(staged) => staged,
        None => {
            if !cancellation.is_cancelled() {
                for sidecar in &unit.sidecars {
                    let file = provisional_file(source_id, sidecar, MediaKind::Sidecar);
                    if !send_file_started(tx, cancellation, file.clone()).await {
                        break;
                    }
                    send_file_finished(
                        tx,
                        cancellation,
                        counters,
                        total,
                        file,
                        FileOutcome::Failed {
                            error: "parent copy failed; sidecar not copied".to_string(),
                        },
                    )
                    .await;
                    if cancellation.is_cancelled() {
                        break;
                    }
                }
            }
            let _ = tokio::fs::remove_dir_all(&seq_dir).await;
            return;
        }
    };

    let mut sidecars = Vec::new();
    for sidecar in &unit.sidecars {
        if cancellation.is_cancelled() {
            break;
        }
        if let Some(staged) = copy_one_to_staging(
            cfg,
            source,
            sidecar,
            MediaKind::Sidecar,
            &seq_dir,
            tx,
            counters,
            total,
            cancellation,
        )
        .await
        {
            sidecars.push(staged);
        }
    }

    if cancellation.is_cancelled() {
        let _ = tokio::fs::remove_dir_all(&seq_dir).await;
        return;
    }

    // EXIF the staged parent on local disk (single-file read on the shared
    // stay-open process; cheap because there is no USB seek).
    let parent_id = file_id(source_id, &parent.rel_path);
    if !send_phase(
        tx,
        cancellation,
        parent_id.clone(),
        FilePhase::ReadingMetadata,
    )
    .await
    {
        let _ = tokio::fs::remove_dir_all(&seq_dir).await;
        return;
    }
    let meta = match exif {
        Some(exif) => match exif
            .read_batch(std::slice::from_ref(&parent.staged_path))
            .await
        {
            Ok(mut metadata) => metadata.pop().unwrap_or_default(),
            Err(error) => {
                emit_event(
                    tx,
                    cancellation,
                    EngineEvent::Warning {
                        kind: WarningKind::MetadataFallback,
                        message: format!(
                            "{}: metadata read failed; using file mtime: {error}",
                            parent.rel_path
                        ),
                        file: Some(parent_id.clone()),
                    },
                )
                .await;
                FileMetadata::default()
            }
        },
        None => FileMetadata::default(),
    };
    if cancellation.is_cancelled() {
        let _ = tokio::fs::remove_dir_all(&seq_dir).await;
        return;
    }
    let synthetic = SourceFile {
        rel_path: parent.rel_path.clone(),
        size: parent.size,
        mtime: parent.mtime,
        extension: String::new(),
        backend_handle: BackendHandle::Path(parent.staged_path.clone()),
    };

    match metadata::resolve_one(&synthetic, &meta) {
        None => {
            place_no_date(source_id, &parent, tx, counters, total, cancellation).await;
            for sidecar in &sidecars {
                if cancellation.is_cancelled() {
                    break;
                }
                place_no_date(source_id, sidecar, tx, counters, total, cancellation).await;
            }
        }
        Some(metadata) => {
            if metadata.source.is_fallback() {
                emit_event(
                    tx,
                    cancellation,
                    EngineEvent::Warning {
                        kind: WarningKind::MetadataFallback,
                        message: format!(
                            "{}: using {} (no DateTimeOriginal)",
                            parent.rel_path,
                            metadata.source.label()
                        ),
                        file: Some(parent_id),
                    },
                )
                .await;
            }
            if !cancellation.is_cancelled() {
                place_one(
                    cfg,
                    source_id,
                    dedupe,
                    &parent,
                    unit.parent_kind,
                    &metadata,
                    tx,
                    counters,
                    total,
                    cancellation,
                )
                .await;
            }
            for sidecar in &sidecars {
                if cancellation.is_cancelled() {
                    break;
                }
                place_one(
                    cfg,
                    source_id,
                    dedupe,
                    sidecar,
                    unit.parent_kind,
                    &metadata,
                    tx,
                    counters,
                    total,
                    cancellation,
                )
                .await;
            }
        }
    }

    // The staging subdir is now empty (everything moved or deleted).
    let _ = tokio::fs::remove_dir_all(&seq_dir).await;
}

/// Dry run: resolve copy units' dates over USB and report what would happen,
/// writing nothing.
async fn run_execute_dry(
    cfg: EngineConfig,
    plan: SyncPlan,
    source: Arc<dyn MediaSource>,
    tx: mpsc::Sender<EngineEvent>,
    cancellation: CancellationHandle,
    total: u64,
) -> Result<SyncSummary> {
    let counters = Counters::default();
    for item in plan
        .items
        .iter()
        .filter(|item| item.action != PlannedAction::Copy)
    {
        if cancellation.is_cancelled() {
            break;
        }
        emit_decided(&tx, item.clone(), &counters, total, &cancellation).await;
    }

    if !cancellation.is_cancelled() {
        if let Some(dated) =
            resolve_units_over_usb(&cfg, source.as_ref(), &plan.units, &tx, &cancellation).await?
        {
            for item in dated.items {
                if cancellation.is_cancelled() {
                    break;
                }
                match item.action {
                    PlannedAction::Copy => {
                        if !send_file_started(&tx, &cancellation, item.clone()).await {
                            break;
                        }
                        send_file_finished(
                            &tx,
                            &cancellation,
                            &counters,
                            total,
                            item.clone(),
                            FileOutcome::Copied {
                                bytes: item.source_size,
                            },
                        )
                        .await;
                    }
                    _ => emit_decided(&tx, item, &counters, total, &cancellation).await,
                }
            }
        }
    }

    let summary = counters.summary(total, cancellation.is_cancelled());
    emit_event(
        &tx,
        &cancellation,
        EngineEvent::ExecutionFinished { summary },
    )
    .await;
    Ok(summary)
}

/// Emit a terminal outcome for an already-decided planned item.
async fn emit_decided(
    tx: &mpsc::Sender<EngineEvent>,
    item: PlannedFile,
    counters: &Counters,
    total: u64,
    cancellation: &CancellationHandle,
) {
    if !send_file_started(tx, cancellation, item.clone()).await {
        return;
    }
    let outcome = match item.action {
        PlannedAction::Error => FileOutcome::Failed {
            error: item
                .reason
                .clone()
                .unwrap_or_else(|| "planning error".to_string()),
        },
        _ => FileOutcome::Skipped {
            reason: item.reason.clone().unwrap_or_else(|| "skipped".to_string()),
        },
    };
    send_file_finished(tx, cancellation, counters, total, item, outcome).await;
}

/// A file copied into staging, awaiting EXIF + placement.
struct StagedFile {
    staged_path: PathBuf,
    rel_path: String,
    size: u64,
    kind: MediaKind,
    mtime: Option<chrono::NaiveDateTime>,
}

/// Copy a single file into `seq_dir`, emitting file lifecycle events. A closed
/// event receiver is treated as defensive cancellation.
#[allow(clippy::too_many_arguments)]
async fn copy_one_to_staging(
    cfg: &EngineConfig,
    source: &dyn MediaSource,
    f: &SourceFile,
    kind: MediaKind,
    seq_dir: &std::path::Path,
    tx: &mpsc::Sender<EngineEvent>,
    counters: &Counters,
    total: u64,
    cancellation: &CancellationHandle,
) -> Option<StagedFile> {
    if cancellation.is_cancelled() {
        return None;
    }

    let file = provisional_file(source.id(), f, kind);
    if !send_file_started(tx, cancellation, file.clone()).await
        || !send_phase(tx, cancellation, file.id(), FilePhase::Copying).await
        || cancellation.is_cancelled()
    {
        return None;
    }

    let source_path = match source.full_path(f).await {
        Ok(path) => path,
        Err(error) => {
            send_file_finished(
                tx,
                cancellation,
                counters,
                total,
                file,
                FileOutcome::Failed {
                    error: error.to_string(),
                },
            )
            .await;
            return None;
        }
    };
    if cancellation.is_cancelled() {
        return None;
    }

    let staged = seq_dir.join(file_name_of(&f.rel_path));
    let file_id = file.id();
    let tx2 = tx.clone();
    let callback_cancellation = cancellation.clone();
    let result =
        crate::copy::copy_atomic(&source_path, &staged, f.size, &cfg.verify, move |event| {
            let event = match event {
                crate::copy::CopyEvent::Progress {
                    bytes_done,
                    bytes_total,
                } => EngineEvent::FileProgress {
                    file: file_id.clone(),
                    bytes_done,
                    bytes_total,
                },
                crate::copy::CopyEvent::Verifying => EngineEvent::FilePhaseChanged {
                    file: file_id.clone(),
                    phase: FilePhase::Verifying,
                },
            };
            match tx2.try_send(event) {
                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => callback_cancellation.cancel(),
            }
        })
        .await;

    match result {
        Ok(_) => Some(StagedFile {
            staged_path: staged,
            rel_path: f.rel_path.clone(),
            size: f.size,
            kind,
            mtime: f.mtime,
        }),
        Err(error) => {
            send_file_finished(
                tx,
                cancellation,
                counters,
                total,
                file,
                FileOutcome::Failed {
                    error: error.to_string(),
                },
            )
            .await;
            None
        }
    }
}

/// Date, dedupe and move one staged file into its final destination, using
/// `route_kind` to pick the root (sidecars route with their parent) and `rm`
/// for the capture date.
#[allow(clippy::too_many_arguments)]
async fn place_one(
    cfg: &EngineConfig,
    source_id: &SourceId,
    dedupe: &tokio::sync::Mutex<DedupeCache>,
    f: &StagedFile,
    route_kind: MediaKind,
    metadata: &ResolvedMetadata,
    tx: &mpsc::Sender<EngineEvent>,
    counters: &Counters,
    total: u64,
    cancellation: &CancellationHandle,
) {
    if cancellation.is_cancelled()
        || !send_phase(
            tx,
            cancellation,
            file_id(source_id, &f.rel_path),
            FilePhase::ResolvingDestination,
        )
        .await
    {
        return;
    }

    let name = file_name_of(&f.rel_path);
    let destination = match plan::dest_path_for(cfg, route_kind, &metadata.datetime, name) {
        Ok(destination) => destination,
        Err(error) => {
            let _ = tokio::fs::remove_file(&f.staged_path).await;
            send_file_finished(
                tx,
                cancellation,
                counters,
                total,
                placed_file(
                    source_id,
                    f,
                    None,
                    Some(metadata),
                    PlannedAction::Error,
                    None,
                ),
                FileOutcome::Failed {
                    error: error.to_string(),
                },
            )
            .await;
            return;
        }
    };

    if cancellation.is_cancelled() {
        return;
    }
    // Reserving under the cache lock makes a destination visible to later
    // units before this worker releases the lock to move its staged file.
    let (action, reason) = dedupe.lock().await.reserve(&destination, name, f.size);
    match action {
        PlannedAction::Copy => {
            if !send_phase(
                tx,
                cancellation,
                file_id(source_id, &f.rel_path),
                FilePhase::Finalizing,
            )
            .await
            {
                return;
            }
            match crate::copy::finalize_move(&f.staged_path, &destination).await {
                Ok(()) => {
                    send_file_finished(
                        tx,
                        cancellation,
                        counters,
                        total,
                        placed_file(
                            source_id,
                            f,
                            Some(destination),
                            Some(metadata),
                            PlannedAction::Copy,
                            None,
                        ),
                        FileOutcome::Copied { bytes: f.size },
                    )
                    .await;
                }
                Err(error) => {
                    let _ = tokio::fs::remove_file(&f.staged_path).await;
                    send_file_finished(
                        tx,
                        cancellation,
                        counters,
                        total,
                        placed_file(
                            source_id,
                            f,
                            Some(destination),
                            Some(metadata),
                            PlannedAction::Error,
                            None,
                        ),
                        FileOutcome::Failed {
                            error: error.to_string(),
                        },
                    )
                    .await;
                }
            }
        }
        PlannedAction::Error => {
            let _ = tokio::fs::remove_file(&f.staged_path).await;
            let error = reason
                .clone()
                .unwrap_or_else(|| "destination conflict".to_string());
            send_file_finished(
                tx,
                cancellation,
                counters,
                total,
                placed_file(
                    source_id,
                    f,
                    Some(destination),
                    Some(metadata),
                    PlannedAction::Error,
                    reason,
                ),
                FileOutcome::Failed { error },
            )
            .await;
        }
        _ => {
            let _ = tokio::fs::remove_file(&f.staged_path).await;
            let skipped = reason
                .clone()
                .unwrap_or_else(|| "already exists".to_string());
            send_file_finished(
                tx,
                cancellation,
                counters,
                total,
                placed_file(
                    source_id,
                    f,
                    Some(destination),
                    Some(metadata),
                    action,
                    reason,
                ),
                FileOutcome::Skipped { reason: skipped },
            )
            .await;
        }
    }
}

/// A staged file we could not date at all (no EXIF, no mtime). Discard the
/// staged copy and report it skipped.
async fn place_no_date(
    source_id: &SourceId,
    f: &StagedFile,
    tx: &mpsc::Sender<EngineEvent>,
    counters: &Counters,
    total: u64,
    cancellation: &CancellationHandle,
) {
    if cancellation.is_cancelled() {
        return;
    }
    let _ = tokio::fs::remove_file(&f.staged_path).await;
    let reason = "no usable date (no EXIF and no mtime)".to_string();
    send_file_finished(
        tx,
        cancellation,
        counters,
        total,
        placed_file(
            source_id,
            f,
            None,
            None,
            PlannedAction::SkipNoDate,
            Some(reason.clone()),
        ),
        FileOutcome::Skipped { reason },
    )
    .await;
}

/// Resolve copy units' dates over USB and date them via [`plan::build_plan`].
/// Used by dry-run preview, which must show exact destinations without writing.
async fn resolve_units_over_usb(
    cfg: &EngineConfig,
    source: &dyn MediaSource,
    units: &[CopyUnit],
    tx: &mpsc::Sender<EngineEvent>,
    cancellation: &CancellationHandle,
) -> Result<Option<SyncPlan>> {
    let mut flat: Vec<(SourceFile, MediaKind)> = Vec::new();
    for unit in units {
        if cancellation.is_cancelled() {
            return Ok(None);
        }
        flat.push((unit.parent.clone(), unit.parent_kind));
        for sidecar in &unit.sidecars {
            flat.push((sidecar.clone(), MediaKind::Sidecar));
        }
    }

    let mut paths = Vec::with_capacity(flat.len());
    for (file, _) in &flat {
        if cancellation.is_cancelled() {
            return Ok(None);
        }
        paths.push(source.full_path(file).await?);
    }
    if cancellation.is_cancelled() {
        return Ok(None);
    }

    let exif = ExifTool::spawn().await?;
    let batch_size = cfg.performance.metadata_batch_size.max(1);
    let total = flat.len() as u64;
    let mut metadata = Vec::with_capacity(flat.len());
    for chunk in paths.chunks(batch_size) {
        if cancellation.is_cancelled() {
            let _ = exif.shutdown().await;
            return Ok(None);
        }
        let raw = match exif.read_batch(chunk).await {
            Ok(raw) => raw,
            Err(error) => {
                let _ = exif.shutdown().await;
                return Err(error);
            }
        };
        for raw_metadata in &raw {
            let index = metadata.len();
            metadata.push(metadata::resolve_one(&flat[index].0, raw_metadata));
        }
        if !emit_event(
            tx,
            cancellation,
            EngineEvent::MetadataProgress {
                done: metadata.len() as u64,
                total,
            },
        )
        .await
            || cancellation.is_cancelled()
        {
            let _ = exif.shutdown().await;
            return Ok(None);
        }
    }
    let _ = exif.shutdown().await;

    if cancellation.is_cancelled() {
        return Ok(None);
    }
    let scanned = flat
        .into_iter()
        .zip(metadata)
        .map(|((file, kind), metadata)| ScannedFile {
            source: source.id().clone(),
            file,
            kind,
            metadata,
        })
        .collect();
    Ok(Some(plan::build_plan(cfg, scanned)?))
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

fn file_id(source: &SourceId, rel_path: &str) -> FileId {
    FileId {
        source: source.clone(),
        rel_path: rel_path.to_string(),
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
pub(crate) fn check_dest_root(path: &std::path::Path, kind: &'static str) -> Result<()> {
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
                .truncate(false)
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
                let Operation {
                    mut events,
                    completion,
                    ..
                } = engine.scan_and_plan(source.clone());
                let drain = tokio::spawn(async move { while events.next().await.is_some() {} });
                let scan = completion.await.unwrap().unwrap();
                drain.await.unwrap();

                let Operation {
                    mut events,
                    completion,
                    ..
                } = engine.execute(scan.plan, source, ExecuteOptions::default());
                let drain = tokio::spawn(async move { while events.next().await.is_some() {} });
                let summary = completion.await.unwrap().unwrap();
                drain.await.unwrap();
                (summary.copied, summary.skipped, summary.failed)
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
        let cancellation = CancellationHandle::new();

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
            &cancellation,
            2,
        )
        .await;
        drop(tx);

        let failed: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
            .filter_map(|event| match event {
                EngineEvent::FileFinished {
                    file,
                    outcome: FileOutcome::Failed { .. },
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
    async fn execution_setup_error_is_emitted_and_completed() {
        use crate::config::{FiltersConfig, PerformanceConfig, VerifyConfig};
        use crate::source::FilesystemSource;
        use tokio_stream::StreamExt;

        let tmp = tempfile::tempdir().unwrap();
        let cfg = EngineConfig {
            images_root: tmp.path().join("missing-images"),
            videos_root: tmp.path().join("missing-videos"),
            images_template: "{yyyy}/{yyyy}-{mm}-{dd}".to_string(),
            videos_template: "{yyyy}/{yyyy}-{mm}-{dd}".to_string(),
            filters: FiltersConfig::default(),
            performance: PerformanceConfig::default(),
            verify: VerifyConfig::default(),
            template_warnings: Vec::new(),
        };
        let engine = Engine::new(cfg, ProfileRegistry::with_builtins().unwrap());
        let source = FilesystemSource::new("card", "card", tmp.path().to_path_buf()).into_arc();

        let Operation {
            mut events,
            completion,
            ..
        } = engine.execute(SyncPlan::default(), source, ExecuteOptions::default());
        let mut saw_destination_error = false;
        while let Some(event) = events.next().await {
            if matches!(
                event,
                EngineEvent::Error {
                    kind: OperationErrorKind::Destination,
                    ..
                }
            ) {
                saw_destination_error = true;
            }
        }
        let error = completion.await.unwrap().unwrap_err();
        assert!(matches!(error, Error::DestRootMissing { .. }));
        assert!(saw_destination_error, "setup error was not emitted");
    }

    #[tokio::test]
    async fn explicit_cancellation_cleans_up_staging_and_completes() {
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
        for index in 0..40 {
            std::fs::write(
                src_root.join(format!("DCIM/IMG{index:04}.JPG")),
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
            performance: PerformanceConfig::default(),
            verify: VerifyConfig::default(),
            template_warnings: Vec::new(),
        };
        let engine = Engine::new(cfg, ProfileRegistry::with_builtins().unwrap());
        let source = FilesystemSource::new("card", "card", src_root).into_arc();

        let Operation {
            mut events,
            completion,
            ..
        } = engine.scan_and_plan(source.clone());
        let scan_events = tokio::spawn(async move { while events.next().await.is_some() {} });
        let scan = completion.await.unwrap().unwrap();
        scan_events.await.unwrap();

        let expected_total = scan.plan.items.len() as u64;
        let Operation {
            mut events,
            completion,
            cancellation,
        } = engine.execute(scan.plan, source, ExecuteOptions::default());
        let mut cancelled = false;
        while let Some(event) = events.next().await {
            if matches!(event, EngineEvent::ExecutionStarted { .. }) {
                cancellation.cancel();
                cancelled = true;
            }
        }
        assert!(cancelled, "execution did not announce its start");
        let summary = completion.await.unwrap().unwrap();
        assert!(summary.cancelled);
        assert_eq!(summary.total, expected_total);

        for root in [&images, &videos] {
            let staging = root.join(plan::STAGING_DIR_NAME);
            assert!(
                !staging.exists() || std::fs::read_dir(&staging).unwrap().next().is_none(),
                "staging area was not cleaned: {}",
                staging.display()
            );
        }
    }
}
