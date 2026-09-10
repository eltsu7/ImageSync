//! `imagesync-tui` — ratatui frontend for imagesync.
//!
//! Single-window app flow:
//!
//!   SourceSelect → CatalogueSelect/Create → Confirm → Scan → Review → Sync → Summary
//!
//! Architecture: the TUI runs the render loop on the main task and drives
//! the engine operations directly. Engine events are polled non-blockingly
//! each frame while the completion handles remain authoritative. Crossterm
//! input events are polled with a short timeout so we can interleave rendering
//! and engine progress without spawning a separate input thread.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use imagesync_core::config::{AppConfig, CatalogueConfig, RawMode};
use imagesync_core::engine::ExecuteOptions;
use imagesync_core::events::{
    EngineEvent, FileOutcome, FilePhase, MediaKindWire, PlannedAction, PlannedFile, SyncSummary,
};
use imagesync_core::mount::{detect_mounts, DetectedMount, MountRank};
use imagesync_core::plan::SyncPlan;
use imagesync_core::source::{FilesystemSource, MediaSource};
use imagesync_core::{
    CancellationHandle, Engine, EngineConfig, Operation, ProfileRegistry, ScanResult,
};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Gauge, List, ListItem, ListState, Paragraph, Scrollbar,
    ScrollbarOrientation, ScrollbarState, Wrap,
};
use ratatui::{DefaultTerminal, Frame};
use tokio_stream::wrappers::ReceiverStream;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    pub config_path: Option<PathBuf>,
    pub source: Option<PathBuf>,
}

/// Run the TUI. Sets up the terminal, runs the app loop, restores on exit.
pub async fn run(options: RunOptions) -> Result<()> {
    // Probe ExifTool early so users see a clear error instead of a cryptic
    // failure mid-scan.
    if let Err(e) = imagesync_core::exiftool::probe_version() {
        anyhow::bail!(
            "{e}\n\nInstall ExifTool:\n  Linux:   sudo apt install libimage-exiftool-perl\n  macOS:   brew install exiftool\n  Windows: winget install OliverBetz.ExifTool"
        );
    }

    let (app_cfg, cfg_path) = load_app_config(options.config_path)?;
    let registry = ProfileRegistry::with_builtins().context("loading built-in camera profiles")?;
    let mut app = App::new(app_cfg, cfg_path, registry);
    if let Some(source) = options.source {
        if !source.is_dir() {
            anyhow::bail!("source is not a directory: {}", source.display());
        }
        app.choose_source(source);
    }

    let mut terminal = ratatui::try_init().context("initialising terminal")?;
    let result = app.run(&mut terminal).await;
    ratatui::restore();
    result
}

fn load_app_config(config_path: Option<PathBuf>) -> Result<(AppConfig, PathBuf)> {
    let path = match config_path {
        Some(path) => path,
        None => AppConfig::default_path()
            .context("could not determine default config path on this platform")?,
    };
    let cfg = AppConfig::load_or_default(&path)
        .with_context(|| format!("loading config from {}", path.display()))?;
    Ok((cfg, path))
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Screen {
    SourceSelect,
    CatalogueSelect,
    CatalogueCreate,
    SaveConfigPrompt,
    Confirm,
    Scan,
    Review,
    Sync,
    Summary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatalogueCreateItem {
    Name,
    ImagesRoot,
    VideosRoot,
    ImagesTemplate,
    VideosTemplate,
    Create,
    Back,
}

impl CatalogueCreateItem {
    fn next(self) -> Self {
        match self {
            Self::Name => Self::ImagesRoot,
            Self::ImagesRoot => Self::VideosRoot,
            Self::VideosRoot => Self::ImagesTemplate,
            Self::ImagesTemplate => Self::VideosTemplate,
            Self::VideosTemplate => Self::Create,
            Self::Create => Self::Back,
            Self::Back => Self::Back,
        }
    }

    fn prev(self) -> Self {
        match self {
            Self::Name => Self::Name,
            Self::ImagesRoot => Self::Name,
            Self::VideosRoot => Self::ImagesRoot,
            Self::ImagesTemplate => Self::VideosRoot,
            Self::VideosTemplate => Self::ImagesTemplate,
            Self::Create => Self::VideosTemplate,
            Self::Back => Self::Create,
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Name => 0,
            Self::ImagesRoot => 1,
            Self::VideosRoot => 2,
            Self::ImagesTemplate => 3,
            Self::VideosTemplate => 4,
            Self::Create => 5,
            Self::Back => 6,
        }
    }
}

type ScanHandle = tokio::task::JoinHandle<imagesync_core::Result<ScanResult>>;
type SyncHandle = tokio::task::JoinHandle<imagesync_core::Result<SyncSummary>>;

/// Hard cap on rendered worker lanes. Beyond this we'd eat the whole
/// screen with progress bars; high-worker-count users still see the
/// total-progress gauge and the log.
const MAX_VISIBLE_SLOTS: usize = 8;

/// One active file operation in the sync screen. Identified by `rel_path` so
/// progress and phase events can update the right lane.
#[derive(Debug, Clone)]
struct CopySlot {
    rel_path: String,
    bytes_done: u64,
    bytes_total: u64,
    phase: Option<FilePhase>,
}

struct App {
    cfg: AppConfig,
    cfg_path: PathBuf,
    registry: ProfileRegistry,

    screen: Screen,
    should_quit: bool,
    status: Option<String>,

    // SourceSelect
    mounts: Vec<DetectedMount>,
    source_selection: usize,
    manual_path: String,
    manual_editing: bool,

    // Catalogue selection and creation
    catalogue_selection: usize,
    selected_catalogue: Option<String>,
    catalogue_name: String,
    catalogue_images_root: String,
    catalogue_videos_root: String,
    catalogue_images_template: String,
    catalogue_videos_template: String,
    catalogue_create_item: CatalogueCreateItem,
    catalogue_cursor: usize,
    confirm_action: usize,
    save_action: usize,
    review_action: usize,
    summary_action: usize,

    // Filters (raw_mode picker on Confirm)
    raw_mode_disk: RawMode,

    // After Confirm
    chosen_source: Option<PathBuf>,
    chosen_label: Option<String>,
    detected_profile_name: Option<String>,

    // Scan
    scan_rx: Option<ReceiverStream<EngineEvent>>,
    scan_handle: Option<ScanHandle>,
    scan_cancellation: Option<CancellationHandle>,
    scan_cancelling: bool,
    scan_status: String,
    scan_files_total: u64,
    meta_done: u64,
    meta_total: u64,
    /// Set when a scan fails; keeps the Scan screen up showing the error
    /// instead of silently flashing back to source select.
    scan_error: Option<String>,

    // Review
    plan: Option<SyncPlan>,
    plan_source: Option<Arc<dyn MediaSource>>,
    review_state: ListState,

    // Sync
    sync_rx: Option<ReceiverStream<EngineEvent>>,
    sync_handle: Option<SyncHandle>,
    sync_cancellation: Option<CancellationHandle>,
    sync_cancelling: bool,
    sync_progress: SyncSummary,
    /// Number of copy candidates in the executed plan. Pre-existing/runtime
    /// skips are deliberately excluded from the import-progress denominator.
    sync_copy_total: u64,
    sync_log: Vec<String>,
    /// Structured warnings/errors retained independently of the bounded live
    /// log so terminal summaries never lose diagnostics.
    sync_diagnostics: Vec<String>,
    /// One slot per copy worker (lane). `Some` when that lane is
    /// actively processing a file, `None` when idle. Sized at sync-start
    /// from `cfg.performance.copy_workers`, capped at `MAX_VISIBLE_SLOTS`.
    sync_slots: Vec<Option<CopySlot>>,
    dry_run: bool,

    // Final summary, supplied only by the authoritative execution completion.
    sync_summary: Option<SyncSummary>,
    sync_error: Option<String>,
}

impl App {
    fn new(cfg: AppConfig, cfg_path: PathBuf, registry: ProfileRegistry) -> Self {
        let mounts = detect_mounts();
        let raw_mode_disk = cfg.filters.raw_mode;
        Self {
            cfg,
            cfg_path,
            registry,
            screen: Screen::SourceSelect,
            should_quit: false,
            status: None,
            mounts,
            source_selection: 0,
            manual_path: String::new(),
            manual_editing: false,
            catalogue_selection: 0,
            selected_catalogue: None,
            catalogue_name: String::new(),
            catalogue_images_root: String::new(),
            catalogue_videos_root: String::new(),
            catalogue_images_template: "{yyyy}/{yyyy}-{mm}-{dd}".into(),
            catalogue_videos_template: "{yyyy}/{yyyy}-{mm}-{dd}".into(),
            catalogue_create_item: CatalogueCreateItem::Name,
            catalogue_cursor: 0,
            confirm_action: 0,
            save_action: 0,
            review_action: 0,
            summary_action: 1,
            raw_mode_disk,
            chosen_source: None,
            chosen_label: None,
            detected_profile_name: None,
            scan_rx: None,
            scan_handle: None,
            scan_cancellation: None,
            scan_cancelling: false,
            scan_status: String::new(),
            scan_error: None,
            scan_files_total: 0,
            meta_done: 0,
            meta_total: 0,
            plan: None,
            plan_source: None,
            review_state: ListState::default(),
            sync_rx: None,
            sync_handle: None,
            sync_cancellation: None,
            sync_cancelling: false,
            sync_progress: SyncSummary::default(),
            sync_copy_total: 0,
            sync_log: Vec::new(),
            sync_diagnostics: Vec::new(),
            sync_slots: Vec::new(),
            dry_run: false,
            sync_summary: None,
            sync_error: None,
        }
    }

    async fn run(mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        // Render at ~30 FPS while waiting on input/engine progress.
        let tick = Duration::from_millis(33);
        let mut last_tick = Instant::now();

        while !self.should_quit {
            terminal.draw(|f| self.render(f))?;

            // Pump engine events first.
            self.pump_engine_events();

            // Wait for input with a short timeout so engine progress redraws.
            let timeout = tick.saturating_sub(last_tick.elapsed());
            if event::poll(timeout)? {
                if let Event::Key(k) = event::read()? {
                    if k.kind == KeyEventKind::Press {
                        self.on_key(k).await;
                    }
                }
            }
            if last_tick.elapsed() >= tick {
                last_tick = Instant::now();
            }

            // Detect terminal completion of scan/sync tasks even if their
            // streams already drained.
            self.poll_handles().await;
        }

        // Request cancellation, keep draining bounded event streams, and wait
        // for authoritative completion so staging cleanup finishes before the
        // Tokio runtime and process exit.
        if let Some(cancellation) = &self.scan_cancellation {
            cancellation.cancel();
        }
        if let Some(cancellation) = &self.sync_cancellation {
            cancellation.cancel();
        }
        while self.scan_handle.is_some() || self.sync_handle.is_some() {
            self.pump_engine_events();
            self.poll_handles().await;
            if self.scan_handle.is_some() || self.sync_handle.is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Engine plumbing
    fn pump_engine_events(&mut self) {
        self.drain_scan_events();
        self.drain_sync_events();
    }

    fn drain_scan_events(&mut self) {
        let mut events = Vec::new();
        if let Some(rx) = self.scan_rx.as_mut() {
            while let Ok(event) = rx.as_mut().try_recv() {
                events.push(event);
            }
        }
        for event in events {
            self.on_scan_event(event);
        }
    }

    fn drain_sync_events(&mut self) {
        let mut events = Vec::new();
        if let Some(rx) = self.sync_rx.as_mut() {
            while let Ok(event) = rx.as_mut().try_recv() {
                events.push(event);
            }
        }
        for event in events {
            self.on_sync_event(event);
        }
    }

    async fn poll_handles(&mut self) {
        if self
            .scan_handle
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            let completion = self.scan_handle.take().unwrap().await;
            self.drain_scan_events();
            self.scan_rx = None;
            self.scan_cancellation = None;
            match completion {
                Ok(Ok(ScanResult { plan, profile })) => {
                    if self.scan_cancelling {
                        self.reset_for_new_run();
                    } else {
                        self.plan = Some(plan);
                        self.detected_profile_name = Some(profile.display_name);
                        self.review_state.select(Some(0));
                        self.review_action = 0;
                        self.screen = Screen::Review;
                    }
                }
                Ok(Err(_error)) if self.scan_cancelling => {
                    self.reset_for_new_run();
                }
                Ok(Err(error)) => {
                    self.scan_error = Some(error.to_string());
                }
                Err(error) => {
                    self.scan_error = Some(format!("scan task panicked: {error}"));
                }
            }
        }

        if self
            .sync_handle
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            let completion = self.sync_handle.take().unwrap().await;
            self.drain_sync_events();
            self.sync_rx = None;
            self.sync_cancellation = None;
            self.sync_cancelling = false;
            match completion {
                Ok(Ok(summary)) => {
                    self.sync_progress = summary;
                    self.sync_summary = Some(summary);
                }
                Ok(Err(error)) => {
                    self.sync_error = Some(format!("sync failed: {error}"));
                }
                Err(error) => {
                    let message = format!("sync task panicked: {error}");
                    let diagnostic = format!(" ERROR {message}");
                    push_log(&mut self.sync_log, diagnostic.clone());
                    self.sync_diagnostics.push(diagnostic);
                    self.sync_error = Some(message);
                }
            }
            self.summary_action = 1;
            self.screen = Screen::Summary;
        }
    }

    fn on_scan_event(&mut self, event: EngineEvent) {
        match event {
            EngineEvent::ScanStarted { display_name, .. } => {
                self.scan_status = format!("scanning {display_name}…");
            }
            EngineEvent::ScanProgress { files_seen, .. } => {
                self.scan_status = format!("scanning… {files_seen} files");
            }
            EngineEvent::ScanComplete { files, .. } => {
                self.scan_files_total = files;
                self.scan_status = format!("{files} files enumerated");
            }
            EngineEvent::ProfileDetected { profile } => {
                self.detected_profile_name = Some(profile.display_name);
            }
            EngineEvent::MetadataProgress { done, total } => {
                self.meta_done = done;
                self.meta_total = total;
            }
            EngineEvent::PlanReady { summary } => {
                self.scan_status = format!(
                    "plan ready: {} copies, {} skips, {} errors",
                    summary.copies, summary.skips, summary.errors
                );
            }
            EngineEvent::Warning { message, .. } => {
                self.status = Some(format!("warn: {message}"));
            }
            EngineEvent::Error { message, .. } => {
                self.status = Some(format!("error: {message}"));
            }
            _ => {}
        }
    }

    fn on_sync_event(&mut self, event: EngineEvent) {
        match event {
            EngineEvent::ExecutionStarted { total_files, .. } => {
                self.sync_progress = SyncSummary {
                    total: total_files,
                    ..SyncSummary::default()
                };
            }
            EngineEvent::FileStarted { file } => {
                self.start_slot(file.source_rel_path, file.source_size);
            }
            EngineEvent::FilePhaseChanged { file, phase } => {
                self.update_slot_phase(&file.rel_path, phase);
            }
            EngineEvent::FileProgress {
                file,
                bytes_done,
                bytes_total,
            } => {
                self.update_slot(&file.rel_path, bytes_done, bytes_total);
            }
            EngineEvent::FileFinished { file, outcome } => {
                self.free_slot(&file.source_rel_path);
                match outcome {
                    FileOutcome::Copied { .. } => {
                        push_log(
                            &mut self.sync_log,
                            format!(" OK   {}", file.source_rel_path),
                        );
                    }
                    FileOutcome::Skipped { reason } => {
                        push_log(
                            &mut self.sync_log,
                            format!(" SKIP {} ({reason})", file.source_rel_path),
                        );
                    }
                    FileOutcome::Failed { error } => {
                        push_log(
                            &mut self.sync_log,
                            format!(" FAIL {} ({error})", file.source_rel_path),
                        );
                    }
                }
            }
            EngineEvent::ExecutionProgress { summary }
            | EngineEvent::ExecutionFinished { summary } => {
                self.sync_progress = summary;
            }
            EngineEvent::Warning { message, file, .. } => {
                let suffix = file
                    .as_ref()
                    .map(|file| format!(" ({})", file.rel_path))
                    .unwrap_or_default();
                let diagnostic = format!(" WARN  {message}{suffix}");
                push_log(&mut self.sync_log, diagnostic.clone());
                self.sync_diagnostics.push(diagnostic);
                self.status = Some(format!("warn: {message}"));
            }
            EngineEvent::Error { message, file, .. } => {
                let suffix = file
                    .as_ref()
                    .map(|file| format!(" ({})", file.rel_path))
                    .unwrap_or_default();
                let diagnostic = format!(" ERROR {message}{suffix}");
                push_log(&mut self.sync_log, diagnostic.clone());
                self.sync_diagnostics.push(diagnostic);
                self.status = Some(format!("error: {message}"));
            }
            _ => {}
        }
    }

    /// Assign a starting copy to a free lane. If `rel_path` is already
    /// shown (shouldn't happen but be defensive), reuse that lane.
    /// Falls back to overwriting the oldest-looking lane if all are
    /// busy — the engine semaphore caps in-flight copies at
    /// `copy_workers`, so this only fires if `MAX_VISIBLE_SLOTS` was
    /// hit on a high-worker-count config.
    fn start_slot(&mut self, rel_path: String, bytes_total: u64) {
        if let Some(slot) = self
            .sync_slots
            .iter_mut()
            .flatten()
            .find(|s| s.rel_path == rel_path)
        {
            slot.bytes_done = 0;
            slot.bytes_total = bytes_total;
            slot.phase = Some(FilePhase::Copying);
            return;
        }
        if let Some(empty) = self.sync_slots.iter_mut().find(|s| s.is_none()) {
            *empty = Some(CopySlot {
                rel_path,
                bytes_done: 0,
                bytes_total,
                phase: Some(FilePhase::Copying),
            });
            return;
        }
        // All lanes full. Replace the slot whose copy is closest to
        // done — it'll free itself imminently anyway and this keeps
        // the screen showing fresh activity.
        if let Some(victim) = self
            .sync_slots
            .iter_mut()
            .max_by_key(|s| s.as_ref().map(pct_x1000).unwrap_or(0))
        {
            *victim = Some(CopySlot {
                rel_path,
                bytes_done: 0,
                bytes_total,
                phase: Some(FilePhase::Copying),
            });
        }
    }

    fn update_slot(&mut self, rel_path: &str, bytes_done: u64, bytes_total: u64) {
        if let Some(slot) = self
            .sync_slots
            .iter_mut()
            .flatten()
            .find(|s| s.rel_path == rel_path)
        {
            slot.bytes_done = bytes_done;
            slot.bytes_total = bytes_total;
        }
    }

    fn update_slot_phase(&mut self, rel_path: &str, phase: FilePhase) {
        if let Some(slot) = self
            .sync_slots
            .iter_mut()
            .flatten()
            .find(|slot| slot.rel_path == rel_path)
        {
            slot.phase = Some(phase);
        }
    }

    fn free_slot(&mut self, rel_path: &str) {
        for slot in self.sync_slots.iter_mut() {
            if slot.as_ref().is_some_and(|c| c.rel_path == rel_path) {
                *slot = None;
                return;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Input handling
    // -----------------------------------------------------------------------

    async fn on_key(&mut self, key: KeyEvent) {
        // Global: Ctrl-C / Ctrl-Q.
        if key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('q'))
        {
            self.should_quit = true;
            return;
        }
        match self.screen {
            Screen::SourceSelect => self.on_key_source_select(key),
            Screen::CatalogueSelect => self.on_key_catalogue_select(key),
            Screen::CatalogueCreate => self.on_key_catalogue_create(key),
            Screen::SaveConfigPrompt => self.on_key_save_prompt(key),
            Screen::Confirm => self.on_key_confirm(key).await,
            Screen::Scan => self.on_key_scan(key),
            Screen::Review => self.on_key_review(key).await,
            Screen::Sync => self.on_key_sync(key),
            Screen::Summary => self.on_key_summary(key),
        }
    }

    fn on_key_source_select(&mut self, key: KeyEvent) {
        if self.manual_editing {
            match key.code {
                KeyCode::Esc => self.manual_editing = false,
                KeyCode::Enter => {
                    let path = PathBuf::from(self.manual_path.trim());
                    if path.as_os_str().is_empty() {
                        self.status = Some("path is empty".into());
                    } else if !path.is_dir() {
                        self.status = Some(format!("not a directory: {}", path.display()));
                    } else {
                        self.choose_source(path);
                    }
                }
                KeyCode::Backspace => {
                    self.manual_path.pop();
                }
                KeyCode::Char(character) => self.manual_path.push(character),
                _ => {}
            }
            return;
        }

        let manual_index = self.mounts.len();
        let refresh_index = manual_index + 1;
        let quit_index = manual_index + 2;
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
            KeyCode::Up => self.source_selection = self.source_selection.saturating_sub(1),
            KeyCode::Down => {
                self.source_selection = (self.source_selection + 1).min(quit_index);
            }
            KeyCode::Enter if self.source_selection < self.mounts.len() => {
                if let Some(mount) = self.mounts.get(self.source_selection).cloned() {
                    self.choose_source(mount.path);
                }
            }
            KeyCode::Enter if self.source_selection == manual_index => {
                self.manual_editing = true;
                self.status = None;
            }
            KeyCode::Enter if self.source_selection == refresh_index => {
                self.mounts = detect_mounts();
                self.source_selection = self.mounts.len() + 1;
                self.status = Some("rescanned mounts".into());
            }
            KeyCode::Enter => self.should_quit = true,
            _ => {}
        }
    }

    fn choose_source(&mut self, path: PathBuf) {
        let label = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| path.to_string_lossy().to_string());

        self.detected_profile_name = Some("auto-detected during scan".into());
        self.chosen_label = Some(label);
        self.chosen_source = Some(path);
        self.selected_catalogue = None;
        self.catalogue_selection = 0;
        self.status = None;
        self.manual_editing = false;
        self.screen = Screen::CatalogueSelect;
    }

    fn on_key_catalogue_select(&mut self, key: KeyEvent) {
        let create_index = self.cfg.catalogues.len();
        let back_index = create_index + 1;
        match key.code {
            KeyCode::Esc => self.screen = Screen::SourceSelect,
            KeyCode::Up => {
                self.catalogue_selection = self.catalogue_selection.saturating_sub(1);
            }
            KeyCode::Down => {
                self.catalogue_selection = (self.catalogue_selection + 1).min(back_index);
            }
            KeyCode::Enter if self.catalogue_selection < create_index => {
                if let Some(name) = self
                    .cfg
                    .catalogues
                    .keys()
                    .nth(self.catalogue_selection)
                    .cloned()
                {
                    self.selected_catalogue = Some(name);
                    self.confirm_action = 0;
                    self.status = None;
                    self.screen = Screen::Confirm;
                }
            }
            KeyCode::Enter if self.catalogue_selection == create_index => {
                self.reset_catalogue_form();
                self.screen = Screen::CatalogueCreate;
            }
            KeyCode::Enter => self.screen = Screen::SourceSelect,
            _ => {}
        }
    }

    async fn on_key_confirm(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => self.screen = Screen::CatalogueSelect,
            KeyCode::Up => {
                self.confirm_action = self.confirm_action.saturating_sub(1);
            }
            KeyCode::Down => {
                self.confirm_action = (self.confirm_action + 1).min(2);
            }
            KeyCode::Left if self.confirm_action == 1 => {
                self.cfg.filters.raw_mode = previous_raw_mode(self.cfg.filters.raw_mode);
            }
            KeyCode::Right if self.confirm_action == 1 => {
                self.cfg.filters.raw_mode = next_raw_mode(self.cfg.filters.raw_mode);
            }
            KeyCode::Enter => match self.confirm_action {
                0 => {
                    if self.cfg.filters.raw_mode != self.raw_mode_disk {
                        self.save_action = 0;
                        self.screen = Screen::SaveConfigPrompt;
                    } else {
                        self.start_scan();
                    }
                }
                2 => self.screen = Screen::CatalogueSelect,
                _ => {}
            },
            _ => {}
        }
    }

    fn on_key_catalogue_create(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.reset_catalogue_form();
                self.status = None;
                self.screen = Screen::CatalogueSelect;
            }
            KeyCode::Down => {
                self.select_catalogue_create_item(self.catalogue_create_item.next());
            }
            KeyCode::Up => {
                self.select_catalogue_create_item(self.catalogue_create_item.prev());
            }
            KeyCode::Left => self.move_catalogue_cursor_left(),
            KeyCode::Right => self.move_catalogue_cursor_right(),
            KeyCode::Enter if self.catalogue_create_item == CatalogueCreateItem::Create => {
                self.try_create_catalogue();
            }
            KeyCode::Enter if self.catalogue_create_item == CatalogueCreateItem::Back => {
                self.reset_catalogue_form();
                self.status = None;
                self.screen = Screen::CatalogueSelect;
            }
            KeyCode::Backspace => self.delete_before_catalogue_cursor(),
            KeyCode::Delete => self.delete_at_catalogue_cursor(),
            KeyCode::Char(character) => self.insert_at_catalogue_cursor(character),
            _ => {}
        }
    }

    fn catalogue_buffer_mut(&mut self) -> Option<&mut String> {
        match self.catalogue_create_item {
            CatalogueCreateItem::Name => Some(&mut self.catalogue_name),
            CatalogueCreateItem::ImagesRoot => Some(&mut self.catalogue_images_root),
            CatalogueCreateItem::VideosRoot => Some(&mut self.catalogue_videos_root),
            CatalogueCreateItem::ImagesTemplate => Some(&mut self.catalogue_images_template),
            CatalogueCreateItem::VideosTemplate => Some(&mut self.catalogue_videos_template),
            CatalogueCreateItem::Create | CatalogueCreateItem::Back => None,
        }
    }

    fn select_catalogue_create_item(&mut self, item: CatalogueCreateItem) {
        self.catalogue_create_item = item;
        self.catalogue_cursor = match item {
            CatalogueCreateItem::Name => self.catalogue_name.len(),
            CatalogueCreateItem::ImagesRoot => self.catalogue_images_root.len(),
            CatalogueCreateItem::VideosRoot => self.catalogue_videos_root.len(),
            CatalogueCreateItem::ImagesTemplate => self.catalogue_images_template.len(),
            CatalogueCreateItem::VideosTemplate => self.catalogue_videos_template.len(),
            CatalogueCreateItem::Create | CatalogueCreateItem::Back => 0,
        };
    }

    fn move_catalogue_cursor_left(&mut self) {
        let cursor = self.catalogue_cursor;
        let Some(value) = self.catalogue_buffer_mut() else {
            return;
        };
        self.catalogue_cursor = value[..cursor]
            .char_indices()
            .last()
            .map_or(0, |(index, _)| index);
    }

    fn move_catalogue_cursor_right(&mut self) {
        let cursor = self.catalogue_cursor;
        let Some(value) = self.catalogue_buffer_mut() else {
            return;
        };
        self.catalogue_cursor = if cursor < value.len() {
            cursor + value[cursor..].chars().next().unwrap().len_utf8()
        } else {
            cursor
        };
    }

    fn insert_at_catalogue_cursor(&mut self, character: char) {
        let cursor = self.catalogue_cursor;
        let Some(value) = self.catalogue_buffer_mut() else {
            return;
        };
        value.insert(cursor, character);
        self.catalogue_cursor += character.len_utf8();
    }

    fn delete_before_catalogue_cursor(&mut self) {
        let cursor = self.catalogue_cursor;
        let Some(value) = self.catalogue_buffer_mut() else {
            return;
        };
        if let Some((start, _)) = value[..cursor].char_indices().last() {
            value.drain(start..cursor);
            self.catalogue_cursor = start;
        }
    }

    fn delete_at_catalogue_cursor(&mut self) {
        let cursor = self.catalogue_cursor;
        let Some(value) = self.catalogue_buffer_mut() else {
            return;
        };
        if cursor < value.len() {
            let end = cursor + value[cursor..].chars().next().unwrap().len_utf8();
            value.drain(cursor..end);
        }
    }

    fn reset_catalogue_form(&mut self) {
        self.catalogue_name.clear();
        self.catalogue_images_root.clear();
        self.catalogue_videos_root.clear();
        self.catalogue_images_template = "{yyyy}/{yyyy}-{mm}-{dd}".into();
        self.catalogue_videos_template = "{yyyy}/{yyyy}-{mm}-{dd}".into();
        self.catalogue_create_item = CatalogueCreateItem::Name;
        self.catalogue_cursor = 0;
    }

    fn try_create_catalogue(&mut self) {
        let catalogue = CatalogueConfig {
            images_root: PathBuf::from(self.catalogue_images_root.trim()),
            videos_root: PathBuf::from(self.catalogue_videos_root.trim()),
            images_template: self.catalogue_images_template.trim().to_string(),
            videos_template: self.catalogue_videos_template.trim().to_string(),
        };
        let mut candidate = self.cfg.clone();
        let key = match candidate.insert_catalogue(&self.catalogue_name, catalogue) {
            Ok(key) => key,
            Err(error) => {
                self.status = Some(error.to_string());
                return;
            }
        };
        if let Err(error) = candidate.save(&self.cfg_path) {
            self.status = Some(error.to_string());
            return;
        }

        self.cfg = candidate;
        let index = self
            .cfg
            .catalogues
            .keys()
            .position(|name| name == &key)
            .unwrap_or(0);
        self.reset_catalogue_form();
        self.catalogue_selection = index;
        self.status = Some(format!("saved catalogue \"{key}\""));
        self.screen = Screen::CatalogueSelect;
    }

    fn on_key_save_prompt(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.confirm_action = 0;
                self.screen = Screen::Confirm;
            }
            KeyCode::Up => {
                self.save_action = self.save_action.saturating_sub(1);
            }
            KeyCode::Down => {
                self.save_action = (self.save_action + 1).min(2);
            }
            KeyCode::Enter if self.save_action == 0 => match self.cfg.save(&self.cfg_path) {
                Ok(()) => {
                    self.status = Some(format!("saved config to {}", self.cfg_path.display()));
                    self.raw_mode_disk = self.cfg.filters.raw_mode;
                    self.start_scan();
                }
                Err(error) => self.status = Some(format!("save failed: {error}")),
            },
            KeyCode::Enter if self.save_action == 1 => {
                self.status = Some("using values for this session only".into());
                self.raw_mode_disk = self.cfg.filters.raw_mode;
                self.start_scan();
            }
            KeyCode::Enter => {
                self.confirm_action = 0;
                self.screen = Screen::Confirm;
            }
            _ => {}
        }
    }

    fn on_key_scan(&mut self, key: KeyEvent) {
        if self.scan_error.is_some() {
            if matches!(key.code, KeyCode::Enter | KeyCode::Esc) {
                self.reset_for_new_run();
            }
            return;
        }
        if matches!(key.code, KeyCode::Enter | KeyCode::Esc | KeyCode::Char('q'))
            && !self.scan_cancelling
        {
            if let Some(cancellation) = &self.scan_cancellation {
                cancellation.cancel();
                self.scan_cancelling = true;
                self.scan_status = "cancelling scan…".into();
            }
        }
    }

    async fn on_key_review(&mut self, key: KeyEvent) {
        let item_count = self
            .plan
            .as_ref()
            .map(|plan| build_review_items(plan).len())
            .unwrap_or(0);
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => self.reset_for_new_run(),
            KeyCode::Down => {
                self.review_action = (self.review_action + 1).min(2);
            }
            KeyCode::Up => {
                self.review_action = self.review_action.saturating_sub(1);
            }
            KeyCode::Enter => match self.review_action {
                0 => self.start_sync(false),
                1 => self.start_sync(true),
                _ => self.reset_for_new_run(),
            },
            KeyCode::PageDown if item_count > 0 => {
                let index = self.review_state.selected().unwrap_or(0);
                self.review_state
                    .select(Some((index + 10).min(item_count - 1)));
            }
            KeyCode::PageUp if item_count > 0 => {
                let index = self.review_state.selected().unwrap_or(0);
                self.review_state.select(Some(index.saturating_sub(10)));
            }
            _ => {}
        }
    }

    fn on_key_sync(&mut self, key: KeyEvent) {
        if matches!(key.code, KeyCode::Enter | KeyCode::Esc | KeyCode::Char('q'))
            && !self.sync_cancelling
        {
            if let Some(cancellation) = &self.sync_cancellation {
                cancellation.cancel();
                self.sync_cancelling = true;
                self.status = Some("cancelling sync…".into());
            }
        }
    }

    fn on_key_summary(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Up => {
                self.summary_action = self.summary_action.saturating_sub(1);
            }
            KeyCode::Down => {
                self.summary_action = (self.summary_action + 1).min(1);
            }
            KeyCode::Enter if self.summary_action == 0 => self.reset_for_new_run(),
            KeyCode::Enter | KeyCode::Esc => self.should_quit = true,
            _ => {}
        }
    }

    fn reset_for_new_run(&mut self) {
        self.screen = Screen::SourceSelect;
        self.chosen_source = None;
        self.selected_catalogue = None;
        self.chosen_label = None;
        self.detected_profile_name = None;
        self.plan = None;
        self.plan_source = None;
        self.scan_error = None;
        self.scan_status.clear();
        self.scan_files_total = 0;
        self.meta_done = 0;
        self.meta_total = 0;
        self.scan_cancelling = false;
        self.sync_progress = SyncSummary::default();
        self.sync_copy_total = 0;
        self.sync_log.clear();
        self.sync_diagnostics.clear();
        self.sync_slots.clear();
        self.sync_cancelling = false;
        self.sync_summary = None;
        self.sync_error = None;
        self.dry_run = false;
        self.status = None;
        self.mounts = detect_mounts();
        self.source_selection = 0;
        self.manual_editing = false;
        self.summary_action = 1;
    }

    // -----------------------------------------------------------------------
    // Engine kickoff
    // -----------------------------------------------------------------------

    fn start_scan(&mut self) {
        let Some(src_path) = self.chosen_source.clone() else {
            return;
        };
        let Some(catalogue_name) = self.selected_catalogue.as_deref() else {
            self.status = Some("no catalogue selected".into());
            self.screen = Screen::CatalogueSelect;
            return;
        };
        let label = self
            .chosen_label
            .clone()
            .unwrap_or_else(|| src_path.to_string_lossy().to_string());

        let engine_cfg = match EngineConfig::try_from_catalogue(&self.cfg, catalogue_name) {
            Ok(config) => config,
            Err(error) => {
                self.status = Some(error.to_string());
                self.screen = Screen::CatalogueSelect;
                return;
            }
        };
        let registry = self.registry.clone();
        let source: Arc<dyn MediaSource> =
            FilesystemSource::new(src_path.to_string_lossy().to_string(), label, src_path)
                .into_arc();

        let engine = Engine::new(engine_cfg, registry);
        let Operation {
            events,
            completion,
            cancellation,
        } = engine.scan_and_plan(source.clone());
        self.plan_source = Some(source);
        self.scan_rx = Some(events);
        self.scan_handle = Some(completion);
        self.scan_cancellation = Some(cancellation);
        self.scan_cancelling = false;
        self.scan_status = "starting…".into();
        self.scan_error = None;
        self.scan_files_total = 0;
        self.meta_done = 0;
        self.meta_total = 0;
        self.screen = Screen::Scan;
    }

    fn start_sync(&mut self, dry_run: bool) {
        let Some(catalogue_name) = self.selected_catalogue.as_deref() else {
            self.status = Some("no catalogue selected".into());
            self.screen = Screen::CatalogueSelect;
            return;
        };
        let (Some(plan), Some(source)) = (self.plan.take(), self.plan_source.clone()) else {
            return;
        };
        let engine_cfg = match EngineConfig::try_from_catalogue(&self.cfg, catalogue_name) {
            Ok(config) => config,
            Err(error) => {
                self.plan = Some(plan);
                self.status = Some(error.to_string());
                self.screen = Screen::CatalogueSelect;
                return;
            }
        };

        self.dry_run = dry_run;
        self.sync_copy_total = plan.copies();
        self.sync_progress = SyncSummary::default();
        self.sync_log.clear();
        self.sync_diagnostics.clear();
        let lanes = self
            .cfg
            .performance
            .copy_workers
            .clamp(1, MAX_VISIBLE_SLOTS);
        self.sync_slots = vec![None; lanes];
        self.sync_summary = None;
        self.sync_error = None;
        self.sync_cancelling = false;
        self.screen = Screen::Sync;

        let engine = Engine::new(engine_cfg, self.registry.clone());
        let Operation {
            events,
            completion,
            cancellation,
        } = engine.execute(plan, source, ExecuteOptions { dry_run });
        self.sync_rx = Some(events);
        self.sync_handle = Some(completion);
        self.sync_cancellation = Some(cancellation);
    }

    // -----------------------------------------------------------------------
    // Rendering
    // -----------------------------------------------------------------------

    fn render(&mut self, f: &mut Frame) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Min(3),
                Constraint::Length(1),
                Constraint::Length(1),
            ])
            .split(f.area());

        self.render_header(f, rows[0]);
        match self.screen {
            Screen::SourceSelect => self.render_source_select(f, rows[1]),
            Screen::CatalogueSelect => self.render_catalogue_select(f, rows[1]),
            Screen::CatalogueCreate => self.render_catalogue_create(f, rows[1]),
            Screen::SaveConfigPrompt => self.render_save_prompt(f, rows[1]),
            Screen::Confirm => self.render_confirm(f, rows[1]),
            Screen::Scan => self.render_scan(f, rows[1]),
            Screen::Review => self.render_review(f, rows[1]),
            Screen::Sync => self.render_sync(f, rows[1]),
            Screen::Summary => self.render_summary(f, rows[1]),
        }
        self.render_status(f, rows[2]);
        self.render_help(f, rows[3]);
    }

    fn render_header(&self, f: &mut Frame, area: Rect) {
        let current = match self.screen {
            Screen::SourceSelect => 0,
            Screen::CatalogueSelect | Screen::CatalogueCreate => 1,
            Screen::Confirm | Screen::SaveConfigPrompt => 2,
            Screen::Scan | Screen::Review => 3,
            Screen::Sync | Screen::Summary => 4,
        };
        let stages = ["SOURCE", "CATALOGUE", "CONFIRM", "REVIEW", "IMPORT"];
        let mut spans = vec![Span::styled(
            " IMAGESYNC     ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )];
        for (index, stage) in stages.iter().enumerate() {
            if index > 0 {
                spans.push(Span::styled("  ─  ", Style::default().fg(Color::DarkGray)));
            }
            let style = if index == current {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
            } else if index < current {
                Style::default().fg(Color::Gray)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            spans.push(Span::styled(*stage, style));
        }
        f.render_widget(Paragraph::new(Line::from(spans)), area);
    }

    fn render_status(&self, f: &mut Frame, area: Rect) {
        let s = self.status.as_deref().unwrap_or("");
        f.render_widget(
            Paragraph::new(s).style(Style::default().fg(Color::DarkGray)),
            area,
        );
    }
    fn render_help(&self, f: &mut Frame, area: Rect) {
        let help = match self.screen {
            Screen::SourceSelect if self.manual_editing => "Enter accept   Esc cancel",
            Screen::SourceSelect => "↑/↓ choose   Enter activate   Esc quit",
            Screen::CatalogueSelect => "↑/↓ choose   Enter activate   Esc back",
            Screen::CatalogueCreate => "↑/↓ choose   ←/→ move cursor   Enter activate   Esc back",
            Screen::SaveConfigPrompt => "↑/↓ choose   Enter activate   Esc back",
            Screen::Confirm => "↑/↓ choose   ←/→ change value   Enter activate   Esc back",
            Screen::Scan if self.scan_error.is_some() => "Enter/Esc back",
            Screen::Scan => "Enter/Esc cancel",
            Screen::Review => "↑/↓ choose   PgUp/PgDn scroll preview   Enter activate   Esc back",
            Screen::Sync => "Enter/Esc cancel",
            Screen::Summary => "↑/↓ choose   Enter activate   Esc quit",
        };
        f.render_widget(
            Paragraph::new(help).style(Style::default().fg(Color::DarkGray)),
            area,
        );
    }

    // ------- Screen: SourceSelect -------

    fn render_source_select(&mut self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Min(10),
                Constraint::Length(5),
            ])
            .split(area);
        f.render_widget(
            Paragraph::new("Select a camera or storage device")
                .style(Style::default().add_modifier(Modifier::BOLD)),
            rows[0],
        );

        let mut items = self
            .mounts
            .iter()
            .map(|mount| {
                let (kind, color) = match mount.rank {
                    MountRank::CameraCard => ("CAMERA", Color::Green),
                    MountRank::Removable => ("REMOVABLE", Color::Cyan),
                    MountRank::Other => ("OTHER", Color::DarkGray),
                };
                ListItem::new(Line::from(vec![
                    Span::styled(
                        format!("{:<30}", trim_label(&mount.label, 30)),
                        Style::default().add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(kind, Style::default().fg(color)),
                ]))
            })
            .collect::<Vec<_>>();
        items.push(ListItem::new(""));
        items.push(if self.manual_editing {
            ListItem::new(Line::from(vec![
                Span::styled("Enter a path…  ", Style::default().fg(Color::Yellow)),
                Span::styled(
                    format!("{}_", self.manual_path),
                    Style::default().fg(Color::Yellow),
                ),
            ]))
        } else {
            ListItem::new("Enter a path…")
        });
        items.push(ListItem::new("Refresh sources"));
        items.push(ListItem::new(Line::styled(
            "Quit",
            Style::default().fg(Color::Red),
        )));

        let display_selection = if self.source_selection < self.mounts.len() {
            self.source_selection
        } else {
            self.source_selection + 1
        };
        render_menu(f, rows[1], " Sources ", items, display_selection);

        let details = if let Some(mount) = self.mounts.get(self.source_selection) {
            let kind = match mount.rank {
                MountRank::CameraCard => "Camera card",
                MountRank::Removable => "Removable storage",
                MountRank::Other => "Other mount",
            };
            vec![
                Line::from(vec![
                    Span::styled(" Type   ", Style::default().fg(Color::Cyan)),
                    Span::raw(kind),
                ]),
                Line::from(vec![
                    Span::styled(" Path   ", Style::default().fg(Color::Cyan)),
                    Span::raw(mount.path.display().to_string()),
                ]),
            ]
        } else if self.source_selection == self.mounts.len() {
            vec![Line::raw(
                "Enter an absolute path to a mounted camera or storage directory.",
            )]
        } else if self.source_selection == self.mounts.len() + 1 {
            vec![Line::raw("Rescan the system for mounted storage devices.")]
        } else {
            vec![Line::raw("Exit ImageSync.")]
        };
        f.render_widget(
            Paragraph::new(details).block(panel(" Source details ", false)),
            rows[2],
        );
    }

    // ------- Screens: CatalogueSelect and CatalogueCreate -------

    fn render_catalogue_select(&mut self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Min(10),
                Constraint::Length(7),
            ])
            .split(area);
        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(
                    "Choose a catalogue",
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!(
                        "    Source: {}",
                        self.chosen_label.as_deref().unwrap_or("(none)")
                    ),
                    Style::default().fg(Color::DarkGray),
                ),
            ])),
            rows[0],
        );

        let create_index = self.cfg.catalogues.len();
        let back_index = create_index + 1;
        let mut items = self
            .cfg
            .catalogues
            .keys()
            .map(|name| ListItem::new(name.clone()))
            .collect::<Vec<_>>();
        items.push(ListItem::new(Line::styled(
            "+ New catalogue…",
            Style::default().fg(Color::Green),
        )));
        items.push(ListItem::new(""));
        items.push(ListItem::new("Back"));
        let display_selection = if self.catalogue_selection == back_index {
            back_index + 1
        } else {
            self.catalogue_selection
        };
        render_menu(f, rows[1], " Catalogues ", items, display_selection);

        let preview = if self.catalogue_selection < create_index {
            self.cfg
                .catalogues
                .iter()
                .nth(self.catalogue_selection)
                .map(catalogue_details)
                .unwrap_or_default()
        } else if self.catalogue_selection == create_index {
            vec![
                Line::raw("Create another destination catalogue."),
                Line::styled(
                    "You will choose image/video roots and folder templates.",
                    Style::default().fg(Color::DarkGray),
                ),
            ]
        } else {
            self.selected_catalogue
                .as_ref()
                .and_then(|name| self.cfg.catalogues.get_key_value(name))
                .map(catalogue_details)
                .or_else(|| self.cfg.catalogues.iter().next().map(catalogue_details))
                .unwrap_or_else(|| vec![Line::raw("Return to source selection.")])
        };
        f.render_widget(
            Paragraph::new(preview)
                .block(panel(" Catalogue details ", false))
                .wrap(Wrap { trim: false }),
            rows[2],
        );
    }

    fn render_catalogue_create(&self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Length(10),
                Constraint::Min(4),
            ])
            .split(area);
        f.render_widget(
            Paragraph::new("Create a catalogue")
                .style(Style::default().add_modifier(Modifier::BOLD)),
            rows[0],
        );

        let items = vec![
            ListItem::new(catalogue_field_line(
                self.catalogue_create_item == CatalogueCreateItem::Name,
                self.catalogue_cursor,
                "Name",
                &self.catalogue_name,
            )),
            ListItem::new(catalogue_field_line(
                self.catalogue_create_item == CatalogueCreateItem::ImagesRoot,
                self.catalogue_cursor,
                "Images root",
                &self.catalogue_images_root,
            )),
            ListItem::new(catalogue_field_line(
                self.catalogue_create_item == CatalogueCreateItem::VideosRoot,
                self.catalogue_cursor,
                "Videos root",
                &self.catalogue_videos_root,
            )),
            ListItem::new(catalogue_field_line(
                self.catalogue_create_item == CatalogueCreateItem::ImagesTemplate,
                self.catalogue_cursor,
                "Images folders",
                &self.catalogue_images_template,
            )),
            ListItem::new(catalogue_field_line(
                self.catalogue_create_item == CatalogueCreateItem::VideosTemplate,
                self.catalogue_cursor,
                "Videos folders",
                &self.catalogue_videos_template,
            )),
            ListItem::new(""),
            ListItem::new(Line::styled(
                "Create catalogue",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            )),
            ListItem::new("Back"),
        ];
        let display_selection = match self.catalogue_create_item {
            CatalogueCreateItem::Create => 6,
            CatalogueCreateItem::Back => 7,
            item => item.index(),
        };
        render_menu(
            f,
            rows[1],
            " Catalogue fields and actions ",
            items,
            display_selection,
        );

        f.render_widget(
            Paragraph::new(vec![
                Line::raw("Typing edits the selected field. Left/Right moves its text cursor."),
                Line::styled(
                    "Folder tokens: {yyyy}, {mm}, {dd}, {month}, {HH}, {MM}, {SS}",
                    Style::default().fg(Color::DarkGray),
                ),
            ])
            .block(panel(" Field help ", false))
            .wrap(Wrap { trim: false }),
            rows[2],
        );
    }

    // ------- Screen: SaveConfigPrompt -------

    fn render_save_prompt(&self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Length(6),
                Constraint::Min(5),
            ])
            .split(area);
        f.render_widget(
            Paragraph::new("Save this RAW setting?")
                .style(Style::default().add_modifier(Modifier::BOLD)),
            rows[0],
        );
        let actions = vec![
            ListItem::new(Line::styled(
                "Save to config",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            )),
            ListItem::new("Use this session only"),
            ListItem::new(""),
            ListItem::new("Back"),
        ];
        render_menu(
            f,
            rows[1],
            " Actions ",
            actions,
            [0, 1, 3][self.save_action],
        );
        f.render_widget(
            Paragraph::new(vec![
                Line::from(vec![
                    Span::styled(" Config path   ", Style::default().fg(Color::Cyan)),
                    Span::raw(self.cfg_path.display().to_string()),
                ]),
                Line::from(vec![
                    Span::styled(" RAW files     ", Style::default().fg(Color::Cyan)),
                    Span::raw(raw_mode_label(self.cfg.filters.raw_mode)),
                ]),
            ])
            .block(panel(" Setting details ", false))
            .wrap(Wrap { trim: false }),
            rows[2],
        );
    }

    // ------- Screen: Confirm -------

    fn render_confirm(&self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Length(7),
                Constraint::Min(9),
            ])
            .split(area);
        f.render_widget(
            Paragraph::new("Confirm import").style(Style::default().add_modifier(Modifier::BOLD)),
            rows[0],
        );
        let raw_focused = self.confirm_action == 1;
        let actions = vec![
            ListItem::new(Line::styled(
                "Scan & review",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            )),
            ListItem::new(Line::from(vec![
                Span::raw("RAW files    "),
                cycle_value(raw_mode_label(self.cfg.filters.raw_mode), raw_focused),
                Span::styled("    ←/→ change", Style::default().fg(Color::DarkGray)),
            ])),
            ListItem::new(""),
            ListItem::new("Back"),
        ];
        render_menu(
            f,
            rows[1],
            " Actions ",
            actions,
            [0, 1, 3][self.confirm_action],
        );

        let catalogue_name = self.selected_catalogue.as_deref().unwrap_or("(none)");
        let catalogue = self
            .selected_catalogue
            .as_ref()
            .and_then(|name| self.cfg.catalogues.get(name));
        let details = vec![
            Line::from(vec![
                Span::styled(" Source      ", Style::default().fg(Color::Cyan)),
                Span::raw(
                    self.chosen_label
                        .as_deref()
                        .unwrap_or_else(|| self.chosen_source.as_ref().map_or("", |_| "(source)")),
                ),
            ]),
            Line::styled(
                format!(
                    "             {}",
                    self.chosen_source
                        .as_ref()
                        .map(|path| path.display().to_string())
                        .unwrap_or_default()
                ),
                Style::default().fg(Color::DarkGray),
            ),
            Line::raw(""),
            Line::from(vec![
                Span::styled(" Catalogue   ", Style::default().fg(Color::Cyan)),
                Span::raw(catalogue_name),
            ]),
            Line::styled(
                format!(
                    " Images      {} / {}",
                    catalogue
                        .map(|value| value.images_root.display().to_string())
                        .unwrap_or_default(),
                    catalogue
                        .map(|value| value.images_template.as_str())
                        .unwrap_or_default()
                ),
                Style::default().fg(Color::DarkGray),
            ),
            Line::styled(
                format!(
                    " Videos      {} / {}",
                    catalogue
                        .map(|value| value.videos_root.display().to_string())
                        .unwrap_or_default(),
                    catalogue
                        .map(|value| value.videos_template.as_str())
                        .unwrap_or_default()
                ),
                Style::default().fg(Color::DarkGray),
            ),
        ];
        f.render_widget(
            Paragraph::new(details)
                .block(panel(" Import details ", false))
                .wrap(Wrap { trim: false }),
            rows[2],
        );
    }

    // ------- Screen: Scan -------

    fn render_scan(&self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Length(3),
                Constraint::Min(5),
            ])
            .split(area);
        f.render_widget(
            Paragraph::new("Planning import").style(Style::default().add_modifier(Modifier::BOLD)),
            rows[0],
        );

        let action = if self.scan_error.is_some() {
            ListItem::new("Back to source")
        } else if self.scan_cancelling {
            ListItem::new(Line::styled(
                "Cancelling scan…",
                Style::default().fg(Color::DarkGray),
            ))
        } else {
            ListItem::new(Line::styled("Cancel scan", Style::default().fg(Color::Red)))
        };
        render_menu(f, rows[1], " Actions ", vec![action], 0);

        if let Some(error) = &self.scan_error {
            f.render_widget(
                Paragraph::new(error.clone())
                    .style(Style::default().fg(Color::Red))
                    .wrap(Wrap { trim: false })
                    .block(panel(" Scan failed ", false)),
                rows[2],
            );
            return;
        }

        let details = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Length(3),
                Constraint::Min(3),
            ])
            .split(rows[2]);
        f.render_widget(
            Paragraph::new(self.scan_status.clone()).block(panel(" Status ", false)),
            details[0],
        );
        f.render_widget(
            Gauge::default()
                .block(panel(" Planning ", false))
                .gauge_style(Style::default().fg(Color::Cyan))
                .ratio(if self.scan_files_total > 0 { 1.0 } else { 0.0 })
                .label("building plan…"),
            details[1],
        );
        f.render_widget(
            Paragraph::new(format!("{} files enumerated.", self.scan_files_total))
                .style(Style::default().fg(Color::DarkGray))
                .block(panel(" Scan details ", false)),
            details[2],
        );
    }

    // ------- Screen: Review -------

    fn render_review(&mut self, f: &mut Frame, area: Rect) {
        let plan = match &self.plan {
            Some(plan) => plan,
            None => return,
        };
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Length(6),
                Constraint::Min(5),
            ])
            .split(area);
        f.render_widget(
            Paragraph::new("Ready to import").style(Style::default().add_modifier(Modifier::BOLD)),
            rows[0],
        );
        let actions = vec![
            ListItem::new(Line::styled(
                format!("Import {} files", plan.copies()),
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            )),
            ListItem::new("Dry run"),
            ListItem::new(""),
            ListItem::new("Back"),
        ];
        render_menu(
            f,
            rows[1],
            " Actions ",
            actions,
            [0, 1, 3][self.review_action],
        );

        let items = build_review_items(plan);
        let item_count = items.len();
        let list = List::new(items).block(panel(" Import preview ", false));
        f.render_stateful_widget(list, rows[2], &mut self.review_state);
        if item_count > rows[2].height.saturating_sub(2) as usize {
            let mut scrollbar_state =
                ScrollbarState::new(item_count).position(self.review_state.selected().unwrap_or(0));
            f.render_stateful_widget(
                Scrollbar::new(ScrollbarOrientation::VerticalRight),
                rows[2],
                &mut scrollbar_state,
            );
        }
    }

    // ------- Screen: Sync -------

    fn render_sync(&self, f: &mut Frame, area: Rect) {
        let outer = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Length(3),
                Constraint::Min(6),
            ])
            .split(area);
        f.render_widget(
            Paragraph::new(if self.dry_run {
                "Checking import (dry run)"
            } else {
                "Importing files"
            })
            .style(Style::default().add_modifier(Modifier::BOLD)),
            outer[0],
        );
        let action = if self.sync_cancelling {
            ListItem::new(Line::styled(
                "Cancelling import…",
                Style::default().fg(Color::DarkGray),
            ))
        } else {
            ListItem::new(Line::styled(
                "Cancel import",
                Style::default().fg(Color::Red),
            ))
        };
        render_menu(f, outer[1], " Actions ", vec![action], 0);

        let body_area = outer[2];
        let lanes_requested = self.sync_slots.len();
        let max_fit = (body_area.height as usize).saturating_sub(3 + 3) / 3;
        let lanes_visible = lanes_requested.min(max_fit).max(1);
        let lanes_hidden = lanes_requested.saturating_sub(lanes_visible);

        let mut constraints = Vec::with_capacity(lanes_visible + 2);
        constraints.push(Constraint::Length(3));
        for _ in 0..lanes_visible {
            constraints.push(Constraint::Length(3));
        }
        constraints.push(Constraint::Min(3));
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints(constraints)
            .split(body_area);

        let completed = self.sync_progress.copied + self.sync_progress.failed;
        let percentage = if self.sync_copy_total > 0 {
            (completed as f64 / self.sync_copy_total as f64).min(1.0)
        } else {
            1.0
        };
        let mut total_label = format!(
            "{completed}/{} imported · {} copied · {} failed · {} skipped",
            self.sync_copy_total,
            self.sync_progress.copied,
            self.sync_progress.failed,
            self.sync_progress.skipped
        );
        if lanes_hidden > 0 {
            total_label.push_str(&format!(" · +{lanes_hidden} more lanes hidden"));
        }
        f.render_widget(
            Gauge::default()
                .block(panel(
                    if self.dry_run {
                        " Progress (dry run) "
                    } else {
                        " Progress "
                    },
                    false,
                ))
                .gauge_style(Style::default().fg(Color::Green))
                .ratio(percentage)
                .label(total_label),
            rows[0],
        );

        for index in 0..lanes_visible {
            let title = format!(" Worker {} ", index + 1);
            let (lane_percentage, lane_label) =
                match self.sync_slots.get(index).and_then(|slot| slot.as_ref()) {
                    Some(slot) if slot.bytes_total > 0 => (
                        (slot.bytes_done as f64 / slot.bytes_total as f64).min(1.0),
                        format!(
                            "{} — {}  {}/{}",
                            slot.rel_path,
                            file_phase_label(slot.phase),
                            human_bytes(slot.bytes_done),
                            human_bytes(slot.bytes_total)
                        ),
                    ),
                    Some(slot) => (
                        0.0,
                        format!("{} — {}", slot.rel_path, file_phase_label(slot.phase)),
                    ),
                    None => (0.0, "(idle)".to_string()),
                };
            f.render_widget(
                Gauge::default()
                    .block(panel(title, false))
                    .gauge_style(Style::default().fg(Color::Cyan))
                    .ratio(lane_percentage)
                    .label(lane_label),
                rows[1 + index],
            );
        }

        let log_area = rows[1 + lanes_visible];
        let log_height = log_area.height.saturating_sub(2) as usize;
        let first_line = self.sync_log.len().saturating_sub(log_height);
        let body = self.sync_log[first_line..]
            .iter()
            .map(|line| {
                let style = if line.contains("FAIL") || line.contains("ERROR") {
                    Style::default().fg(Color::Red)
                } else if line.contains("WARN") {
                    Style::default().fg(Color::Yellow)
                } else if line.contains("SKIP") {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default().fg(Color::Green)
                };
                Line::styled(line.clone(), style)
            })
            .collect::<Vec<_>>();
        f.render_widget(
            Paragraph::new(body).block(panel(" Recent activity ", false)),
            log_area,
        );
    }

    // ------- Screen: Summary -------

    fn render_summary(&self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Length(4),
                Constraint::Min(6),
            ])
            .split(area);
        let summary = self.sync_summary.unwrap_or_default();
        let (heading, heading_color) = if let Some(error) = &self.sync_error {
            (format!("Import did not complete: {error}"), Color::Red)
        } else if summary.cancelled {
            ("Import cancelled".into(), Color::Yellow)
        } else if self.dry_run {
            ("Dry run complete".into(), Color::Green)
        } else {
            ("Import complete".into(), Color::Green)
        };
        f.render_widget(
            Paragraph::new(heading).style(
                Style::default()
                    .fg(heading_color)
                    .add_modifier(Modifier::BOLD),
            ),
            rows[0],
        );
        render_menu(
            f,
            rows[1],
            " Actions ",
            vec![
                ListItem::new(Line::styled(
                    "Import another source",
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD),
                )),
                ListItem::new("Quit"),
            ],
            self.summary_action,
        );

        let mut details = Vec::new();
        if self.dry_run && self.sync_error.is_none() && !summary.cancelled {
            details.push(Line::raw("No files were written."));
            details.push(Line::raw(""));
        }
        if self.sync_summary.is_some() {
            details.extend([
                Line::from(vec![
                    Span::styled(" Completed   ", Style::default().fg(Color::Cyan)),
                    Span::raw(format!("{}/{}", summary.completed(), summary.total)),
                ]),
                Line::from(vec![
                    Span::styled(" Copied      ", Style::default().fg(Color::Green)),
                    Span::raw(summary.copied.to_string()),
                ]),
                Line::from(vec![
                    Span::styled(" Skipped     ", Style::default().fg(Color::Cyan)),
                    Span::raw(summary.skipped.to_string()),
                ]),
                Line::from(vec![
                    Span::styled(" Failed      ", Style::default().fg(Color::Red)),
                    Span::raw(summary.failed.to_string()),
                ]),
            ]);
        }
        if !self.sync_diagnostics.is_empty() {
            details.push(Line::raw(""));
            details.push(Line::styled(
                "Warnings and errors",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ));
            details.extend(
                self.sync_diagnostics
                    .iter()
                    .map(|diagnostic| Line::raw(diagnostic.clone())),
            );
        }
        f.render_widget(
            Paragraph::new(details)
                .block(panel(" Summary ", false))
                .wrap(Wrap { trim: false }),
            rows[2],
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn panel(title: impl Into<String>, active: bool) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(title.into())
        .border_style(Style::default().fg(if active { Color::Cyan } else { Color::DarkGray }))
}

fn render_menu<'a>(
    frame: &mut Frame,
    area: Rect,
    title: impl Into<String>,
    items: Vec<ListItem<'a>>,
    selected_display_row: usize,
) {
    let mut state = ListState::default();
    state.select(Some(selected_display_row));
    frame.render_stateful_widget(
        List::new(items)
            .block(panel(title, true))
            .highlight_symbol("› ")
            .highlight_style(
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        area,
        &mut state,
    );
}
fn catalogue_field_line(focused: bool, cursor: usize, label: &str, value: &str) -> Line<'static> {
    let mut shown = value.to_string();
    if focused {
        shown.insert(cursor, '_');
    }
    Line::from(vec![
        Span::styled(format!("{label:<16}"), Style::default().fg(Color::Cyan)),
        Span::raw(shown),
    ])
}

fn catalogue_details((name, catalogue): (&String, &CatalogueConfig)) -> Vec<Line<'static>> {
    vec![
        Line::styled(name.clone(), Style::default().add_modifier(Modifier::BOLD)),
        Line::from(vec![
            Span::styled(" Images   ", Style::default().fg(Color::Cyan)),
            Span::raw(catalogue.images_root.display().to_string()),
        ]),
        Line::styled(
            format!("           {}", catalogue.images_template),
            Style::default().fg(Color::DarkGray),
        ),
        Line::from(vec![
            Span::styled(" Videos   ", Style::default().fg(Color::Cyan)),
            Span::raw(catalogue.videos_root.display().to_string()),
        ]),
        Line::styled(
            format!("           {}", catalogue.videos_template),
            Style::default().fg(Color::DarkGray),
        ),
    ]
}

fn cycle_value(value: &str, focused: bool) -> Span<'static> {
    Span::styled(
        format!("‹ {value} ›"),
        Style::default().fg(if focused { Color::Black } else { Color::Yellow }),
    )
}

fn push_log(buf: &mut Vec<String>, s: String) {
    const MAX: usize = 500;
    buf.push(s);
    if buf.len() > MAX {
        let drop = buf.len() - MAX;
        buf.drain(..drop);
    }
}

fn file_phase_label(phase: Option<FilePhase>) -> &'static str {
    match phase {
        Some(FilePhase::Copying) => "copying",
        Some(FilePhase::Verifying) => "verifying",
        Some(FilePhase::ReadingMetadata) => "reading metadata",
        Some(FilePhase::ResolvingDestination) => "resolving destination",
        Some(FilePhase::Finalizing) => "finalizing",
        None => "starting",
    }
}

/// Slot completion percentage scaled to 0..=1000 for ordinal compare
/// (used to pick the most-done slot to evict when over-subscribed).
fn pct_x1000(slot: &CopySlot) -> u64 {
    match slot
        .bytes_done
        .saturating_mul(1000)
        .checked_div(slot.bytes_total)
    {
        Some(v) => v.min(1000),
        None => 0,
    }
}

fn trim_label(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

fn raw_mode_label(m: RawMode) -> &'static str {
    match m {
        RawMode::All => "All (RAW + JPEG)",
        RawMode::RawOnly => "RAW only",
        RawMode::NonRawOnly => "Non-RAW only",
    }
}
fn previous_raw_mode(mode: RawMode) -> RawMode {
    match mode {
        RawMode::All => RawMode::NonRawOnly,
        RawMode::RawOnly => RawMode::All,
        RawMode::NonRawOnly => RawMode::RawOnly,
    }
}

fn next_raw_mode(mode: RawMode) -> RawMode {
    match mode {
        RawMode::All => RawMode::RawOnly,
        RawMode::RawOnly => RawMode::NonRawOnly,
        RawMode::NonRawOnly => RawMode::All,
    }
}

fn build_review_items(plan: &SyncPlan) -> Vec<ListItem<'static>> {
    let mut items = vec![
        ListItem::new(Line::from(vec![
            Span::styled(" Copy      ", Style::default().fg(Color::Green)),
            Span::raw(format!(
                "{} files · {}",
                plan.copies(),
                human_bytes(
                    plan.items
                        .iter()
                        .filter(|item| item.action == PlannedAction::Copy)
                        .map(|item| item.source_size)
                        .sum()
                )
            )),
        ])),
        ListItem::new(Line::from(vec![
            Span::styled(" Skip      ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{} files", plan.skips())),
        ])),
        ListItem::new(""),
        ListItem::new(Line::styled(
            "Planned source groups",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )),
    ];
    items.extend(build_review_tree(plan));
    items
}

/// Build the grouped tree view of files that would be copied: one section
/// per destination directory, with raw/non-raw/video counts and at most
/// 3 sample filenames per directory. Directories that don't exist on disk
/// are marked with a leading `+`.
fn build_review_tree(plan: &SyncPlan) -> Vec<ListItem<'static>> {
    // Group COPY candidates by their SOURCE directory. The destination date
    // folder isn't known until copy time (EXIF is read on the local staged
    // copy), so the preview is organised by where files come from, with
    // per-group counts and total size.
    let mut groups: BTreeMap<String, Vec<&PlannedFile>> = BTreeMap::new();
    for it in &plan.items {
        if it.action != PlannedAction::Copy {
            continue;
        }
        let dir = match it.source_rel_path.rsplit_once('/') {
            Some((d, _)) => d.to_string(),
            None => "(root)".to_string(),
        };
        groups.entry(dir).or_default().push(it);
    }

    let mut out: Vec<ListItem<'static>> = Vec::new();

    if groups.is_empty() {
        out.push(ListItem::new(Line::from(Span::styled(
            "(no new files to copy)",
            Style::default().fg(Color::DarkGray),
        ))));
        return out;
    }

    for (dir, files) in &groups {
        let mut raw = 0u32;
        let mut img = 0u32;
        let mut vid = 0u32;
        let mut other = 0u32;
        let mut bytes = 0u64;
        for f in files {
            match f.kind {
                MediaKindWire::RawImage => raw += 1,
                MediaKindWire::Image => img += 1,
                MediaKindWire::Video => vid += 1,
                MediaKindWire::Sidecar => other += 1,
            }
            bytes += f.source_size;
        }

        let dir_style = Style::default().fg(Color::Cyan).bold();

        // Header: "  DCIM/100MSDCF/   RAW: 12  IMG: 8  VID: 1   (1.2 GiB)"
        let mut spans = vec![
            Span::styled("  ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{dir}/"), dir_style),
            Span::raw("   "),
        ];
        if raw > 0 {
            spans.push(Span::styled(
                format!("RAW: {raw}  "),
                Style::default().fg(Color::Magenta),
            ));
        }
        if img > 0 {
            spans.push(Span::styled(
                format!("IMG: {img}  "),
                Style::default().fg(Color::Yellow),
            ));
        }
        if vid > 0 {
            spans.push(Span::styled(
                format!("VID: {vid}  "),
                Style::default().fg(Color::Blue),
            ));
        }
        if other > 0 {
            spans.push(Span::styled(
                format!("SIDE: {other}  "),
                Style::default().fg(Color::DarkGray),
            ));
        }
        spans.push(Span::styled(
            format!("({})", human_bytes(bytes)),
            Style::default().fg(Color::DarkGray),
        ));
        out.push(ListItem::new(Line::from(spans)));

        // Preserve every filename; the viewport scrolls when the terminal
        // cannot display the complete plan.
        let mut sorted = files.clone();
        sorted.sort_by(|a, b| basename_of(&a.source_rel_path).cmp(basename_of(&b.source_rel_path)));
        for f in &sorted {
            let name = basename_of(&f.source_rel_path).to_string();
            let kind_tag = match f.kind {
                MediaKindWire::RawImage => Span::styled("RAW", Style::default().fg(Color::Magenta)),
                MediaKindWire::Image => Span::styled("IMG", Style::default().fg(Color::Yellow)),
                MediaKindWire::Video => Span::styled("VID", Style::default().fg(Color::Blue)),
                MediaKindWire::Sidecar => {
                    Span::styled("SIDE", Style::default().fg(Color::DarkGray))
                }
            };
            out.push(ListItem::new(Line::from(vec![
                Span::raw("    ├─ "),
                kind_tag,
                Span::raw("  "),
                Span::raw(name),
            ])));
        }
        // Blank spacer between groups.
        out.push(ListItem::new(Line::raw("")));
    }

    out
}

fn basename_of(rel_path: &str) -> &str {
    rel_path.rsplit('/').next().unwrap_or(rel_path)
}

fn human_bytes(b: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if b >= GB {
        format!("{:.1} GiB", b as f64 / GB as f64)
    } else if b >= MB {
        format!("{:.1} MiB", b as f64 / MB as f64)
    } else if b >= KB {
        format!("{:.1} KiB", b as f64 / KB as f64)
    } else {
        format!("{b} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_bytes_formats() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(2048), "2.0 KiB");
        assert!(human_bytes(5 * 1024 * 1024).contains("MiB"));
        assert!(human_bytes(2 * 1024 * 1024 * 1024).contains("GiB"));
    }

    #[test]
    fn trim_label_truncates() {
        assert_eq!(trim_label("short", 10), "short");
        let long = "a".repeat(50);
        let t = trim_label(&long, 10);
        assert_eq!(t.chars().count(), 10);
        assert!(t.ends_with('…'));
    }

    #[test]
    fn push_log_caps_history() {
        let mut v = Vec::new();
        for i in 0..600 {
            push_log(&mut v, format!("{i}"));
        }
        assert_eq!(v.len(), 500);
        assert_eq!(v[0], "100");
        assert_eq!(v[499], "599");
    }

    fn app(config_path: PathBuf) -> App {
        App::new(
            AppConfig::default(),
            config_path,
            ProfileRegistry::with_builtins().unwrap(),
        )
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn fill_valid_catalogue(app: &mut App, root: &std::path::Path, name: &str) {
        let images = root.join("images");
        let videos = root.join("videos");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::create_dir_all(&videos).unwrap();
        app.catalogue_name = name.into();
        app.catalogue_images_root = images.display().to_string();
        app.catalogue_videos_root = videos.display().to_string();
        app.catalogue_images_template = "{yyyy}/images".into();
        app.catalogue_videos_template = "{yyyy}/videos".into();
    }

    #[test]
    fn source_selection_always_opens_catalogue_selection() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let mut app = app(dir.path().join("config.toml"));

        assert_eq!(app.screen, Screen::SourceSelect);
        app.choose_source(source);
        assert_eq!(app.screen, Screen::CatalogueSelect);
        assert_eq!(app.catalogue_selection, 0);
        assert!(app.selected_catalogue.is_none());
    }

    #[test]
    fn catalogue_creation_cancel_resets_form_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let mut app = app(config_path.clone());
        app.screen = Screen::CatalogueCreate;
        app.catalogue_name = "Discarded".into();
        app.catalogue_images_root = "/tmp/images".into();
        app.catalogue_images_template = "changed".into();

        app.on_key_catalogue_create(key(KeyCode::Esc));

        assert_eq!(app.screen, Screen::CatalogueSelect);
        assert!(app.catalogue_name.is_empty());
        assert!(app.catalogue_images_root.is_empty());
        assert_eq!(app.catalogue_images_template, "{yyyy}/{yyyy}-{mm}-{dd}");
        assert!(!config_path.exists());
        assert!(app.cfg.catalogues.is_empty());
    }

    #[test]
    fn invalid_catalogue_values_stay_in_form_and_write_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let mut app = app(config_path.clone());
        app.screen = Screen::CatalogueCreate;

        fill_valid_catalogue(&mut app, dir.path(), " ");
        app.try_create_catalogue();
        assert_eq!(
            app.status.as_deref(),
            Some("config error: catalogue name must not be empty")
        );

        app.catalogue_name = "Missing".into();
        app.catalogue_images_root = dir.path().join("missing").display().to_string();
        app.try_create_catalogue();
        assert!(app
            .status
            .as_deref()
            .unwrap()
            .contains("destination root not found"));

        fill_valid_catalogue(&mut app, dir.path(), "Bad template");
        app.catalogue_images_template = "{unknown}".into();
        app.try_create_catalogue();
        assert!(app.status.as_deref().unwrap().contains("invalid"));

        assert_eq!(app.screen, Screen::CatalogueCreate);
        assert!(app.cfg.catalogues.is_empty());
        assert!(!config_path.exists());
    }

    #[test]
    fn catalogue_save_failure_keeps_live_config_and_form_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config-target");
        std::fs::create_dir(&config_path).unwrap();
        let mut app = app(config_path);
        app.screen = Screen::CatalogueCreate;
        fill_valid_catalogue(&mut app, dir.path(), "Photos");

        app.try_create_catalogue();

        assert_eq!(app.screen, Screen::CatalogueCreate);
        assert!(app.cfg.catalogues.is_empty());
        assert_eq!(app.catalogue_name, "Photos");
        assert!(app.status.as_deref().unwrap().contains("io error"));
    }

    #[tokio::test]
    async fn successful_creation_selection_and_new_import_flow() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let source = dir.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let mut app = app(config_path.clone());
        app.choose_source(source.clone());
        app.on_key_catalogue_select(key(KeyCode::Enter));
        assert_eq!(app.screen, Screen::CatalogueCreate);
        fill_valid_catalogue(&mut app, dir.path(), "Photos");

        app.try_create_catalogue();

        assert_eq!(app.screen, Screen::CatalogueSelect);
        assert_eq!(app.catalogue_selection, 0);
        assert!(app.selected_catalogue.is_none());
        assert_eq!(
            AppConfig::load_or_default(&config_path)
                .unwrap()
                .catalogues
                .len(),
            1
        );

        app.on_key_catalogue_select(key(KeyCode::Enter));
        assert_eq!(app.screen, Screen::Confirm);
        assert_eq!(app.selected_catalogue.as_deref(), Some("Photos"));

        app.confirm_action = 2;
        app.on_key_confirm(key(KeyCode::Enter)).await;
        assert_eq!(app.screen, Screen::CatalogueSelect);
        assert_eq!(app.selected_catalogue.as_deref(), Some("Photos"));

        app.screen = Screen::Summary;
        app.summary_action = 0;
        app.on_key_summary(key(KeyCode::Enter));
        assert_eq!(app.screen, Screen::SourceSelect);
        assert!(app.chosen_source.is_none());
        assert!(app.selected_catalogue.is_none());
        assert!(app.plan.is_none());
        assert!(app.plan_source.is_none());
    }
    #[tokio::test]
    async fn confirm_value_changes_only_with_horizontal_arrows() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = app(dir.path().join("config.toml"));
        app.screen = Screen::Confirm;
        app.confirm_action = 1;

        app.on_key_confirm(key(KeyCode::Enter)).await;
        assert_eq!(app.cfg.filters.raw_mode, RawMode::All);
        assert_eq!(app.screen, Screen::Confirm);

        app.on_key_confirm(key(KeyCode::Right)).await;
        assert_eq!(app.cfg.filters.raw_mode, RawMode::RawOnly);
        app.on_key_confirm(key(KeyCode::Left)).await;
        assert_eq!(app.cfg.filters.raw_mode, RawMode::All);
    }

    #[test]
    fn catalogue_fields_are_edited_without_entering_a_focus_mode() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = app(dir.path().join("config.toml"));
        app.screen = Screen::CatalogueCreate;

        app.on_key_catalogue_create(key(KeyCode::Char('P')));
        app.on_key_catalogue_create(key(KeyCode::Enter));
        app.on_key_catalogue_create(key(KeyCode::Char('h')));

        assert_eq!(app.catalogue_name, "Ph");
        assert_eq!(app.screen, Screen::CatalogueCreate);
    }

    #[test]
    fn summary_defaults_to_quit() {
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path().join("config.toml"));

        assert_eq!(app.summary_action, 1);
    }
}
