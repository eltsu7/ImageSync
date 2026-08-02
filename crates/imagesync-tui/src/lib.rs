//! `imagesync-tui` — ratatui frontend for imagesync.
//!
//! Single-window app with five screens reached in order:
//!
//!   SourceSelect  →  Confirm  →  Scan  →  Review  →  Sync  →  Summary
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
use imagesync_core::config::{AppConfig, RawMode};
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
    Block, Borders, Gauge, List, ListItem, ListState, Paragraph, Scrollbar, ScrollbarOrientation,
    ScrollbarState, Wrap,
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
    Destination,
    SaveConfigPrompt,
    Confirm,
    Scan,
    Review,
    Sync,
    Summary,
}

/// Which field of the destination editor is focused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SavePromptNext {
    Confirm,
    StartScan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DestField {
    ImagesRoot,
    VideosRoot,
    ImagesTemplate,
    VideosTemplate,
}

impl DestField {
    fn next(self) -> Self {
        match self {
            DestField::ImagesRoot => DestField::VideosRoot,
            DestField::VideosRoot => DestField::ImagesTemplate,
            DestField::ImagesTemplate => DestField::VideosTemplate,
            DestField::VideosTemplate => DestField::ImagesRoot,
        }
    }
    fn prev(self) -> Self {
        match self {
            DestField::ImagesRoot => DestField::VideosTemplate,
            DestField::VideosRoot => DestField::ImagesRoot,
            DestField::ImagesTemplate => DestField::VideosRoot,
            DestField::VideosTemplate => DestField::ImagesTemplate,
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
    sources_state: ListState,
    manual_path: String,
    manual_focused: bool,
    source_action: usize,
    source_actions_focused: bool,

    // Destination editor (live buffers; copied to cfg on accept)
    dest_images_root: String,
    dest_videos_root: String,
    dest_images_template: String,
    dest_videos_template: String,
    dest_field: DestField,
    dest_cursor: usize,
    confirm_action: usize,
    save_action: usize,
    review_action: usize,
    review_actions_focused: bool,
    summary_action: usize,

    // Filters (raw_mode picker on Confirm)
    raw_mode_disk: RawMode,
    save_prompt_next: SavePromptNext,

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
        let mut sources_state = ListState::default();
        if !mounts.is_empty() {
            sources_state.select(Some(0));
        }
        let dest_images_root = cfg
            .paths
            .images_root
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        let dest_videos_root = cfg
            .paths
            .videos_root
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        let dest_images_template = cfg.paths.images_template.clone();
        let dest_videos_template = cfg.paths.videos_template.clone();
        let raw_mode_disk = cfg.filters.raw_mode;
        Self {
            cfg,
            cfg_path,
            registry,
            screen: Screen::SourceSelect,
            should_quit: false,
            status: None,
            mounts,
            sources_state,
            manual_path: String::new(),
            manual_focused: false,
            source_action: 0,
            source_actions_focused: false,
            dest_images_root,
            dest_videos_root,
            dest_images_template,
            dest_videos_template,
            dest_field: DestField::ImagesRoot,
            dest_cursor: 0,
            confirm_action: 0,
            save_action: 0,
            review_action: 0,
            review_actions_focused: false,
            summary_action: 0,
            raw_mode_disk,
            save_prompt_next: SavePromptNext::Confirm,
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
                        self.plan_source = None;
                        self.scan_cancelling = false;
                        self.screen = Screen::SourceSelect;
                    } else {
                        self.plan = Some(plan);
                        self.detected_profile_name = Some(profile.display_name);
                        self.review_state.select(Some(0));
                        self.review_actions_focused = true;
                        self.screen = Screen::Review;
                    }
                }
                Ok(Err(_error)) if self.scan_cancelling => {
                    self.plan_source = None;
                    self.scan_cancelling = false;
                    self.screen = Screen::SourceSelect;
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
            Screen::Destination => self.on_key_destination(key),
            Screen::SaveConfigPrompt => self.on_key_save_prompt(key),
            Screen::Confirm => self.on_key_confirm(key).await,
            Screen::Scan => self.on_key_scan(key),
            Screen::Review => self.on_key_review(key).await,
            Screen::Sync => self.on_key_sync(key),
            Screen::Summary => self.on_key_summary(key),
        }
    }

    fn on_key_source_select(&mut self, key: KeyEvent) {
        if self.manual_focused {
            match key.code {
                KeyCode::Esc => self.manual_focused = false,
                KeyCode::Tab => {
                    self.manual_focused = false;
                    self.source_actions_focused = true;
                }
                KeyCode::Enter => {
                    let p = PathBuf::from(self.manual_path.trim());
                    if p.as_os_str().is_empty() {
                        self.status = Some("path is empty".into());
                    } else if !p.is_dir() {
                        self.status = Some(format!("not a directory: {}", p.display()));
                    } else {
                        self.choose_source(p);
                    }
                }
                KeyCode::Backspace => {
                    self.manual_path.pop();
                }
                KeyCode::Char(c) => self.manual_path.push(c),
                _ => {}
            }
            return;
        }

        if self.source_actions_focused {
            match key.code {
                KeyCode::Esc => self.source_actions_focused = false,
                KeyCode::Tab => self.source_actions_focused = false,
                KeyCode::Up | KeyCode::Char('k') => {
                    self.source_action = self.source_action.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.source_action = (self.source_action + 1).min(2);
                }
                KeyCode::Enter => match self.source_action {
                    0 => self.manual_focused = true,
                    1 => {
                        self.mounts = detect_mounts();
                        self.sources_state
                            .select((!self.mounts.is_empty()).then_some(0));
                        self.status = Some("rescanned mounts".into());
                    }
                    _ => self.should_quit = true,
                },
                _ => {}
            }
            return;
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
            KeyCode::Tab => self.manual_focused = true,
            KeyCode::Down | KeyCode::Char('j') if !self.mounts.is_empty() => {
                let i = self.sources_state.selected().unwrap_or(0);
                self.sources_state.select(Some((i + 1) % self.mounts.len()));
            }
            KeyCode::Up | KeyCode::Char('k') if !self.mounts.is_empty() => {
                let i = self.sources_state.selected().unwrap_or(0);
                self.sources_state
                    .select(Some(if i == 0 { self.mounts.len() - 1 } else { i - 1 }));
            }
            KeyCode::Enter => {
                if let Some(i) = self.sources_state.selected() {
                    if let Some(m) = self.mounts.get(i).cloned() {
                        self.choose_source(m.path);
                    }
                }
            }
            _ => {}
        }
    }

    fn choose_source(&mut self, path: PathBuf) {
        let label = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| path.to_string_lossy().to_string());

        // Profile selection is part of the engine scan, not a frontend
        // heuristic. Show that honestly until the scan reports its result.
        self.detected_profile_name = Some("auto-detected during scan".into());
        self.chosen_label = Some(label);
        self.chosen_source = Some(path);
        self.status = None;

        // If destination roots are missing, force the user to fill them in
        // before showing the Confirm screen.
        if self.cfg.paths.images_root.is_none() || self.cfg.paths.videos_root.is_none() {
            let field = if self.cfg.paths.images_root.is_none() {
                DestField::ImagesRoot
            } else {
                DestField::VideosRoot
            };
            self.select_dest_field(field);
            self.screen = Screen::Destination;
        } else {
            self.screen = Screen::Confirm;
        }
    }

    async fn on_key_confirm(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => self.screen = Screen::SourceSelect,
            KeyCode::Up | KeyCode::Char('k') => {
                self.confirm_action = self.confirm_action.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.confirm_action = (self.confirm_action + 1).min(3);
            }
            KeyCode::Enter => match self.confirm_action {
                0 => {
                    if self.cfg.filters.raw_mode != self.raw_mode_disk {
                        self.save_prompt_next = SavePromptNext::StartScan;
                        self.screen = Screen::SaveConfigPrompt;
                    } else {
                        self.start_scan();
                    }
                }
                1 => {
                    self.select_dest_field(DestField::ImagesRoot);
                    self.screen = Screen::Destination;
                }
                2 => {
                    self.cfg.filters.raw_mode = match self.cfg.filters.raw_mode {
                        RawMode::All => RawMode::RawOnly,
                        RawMode::RawOnly => RawMode::NonRawOnly,
                        RawMode::NonRawOnly => RawMode::All,
                    };
                }
                _ => self.screen = Screen::SourceSelect,
            },
            _ => {}
        }
    }

    // ------- Destination editor -------

    fn on_key_destination(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.reset_destination_buffers();
                if self.cfg.paths.images_root.is_none() || self.cfg.paths.videos_root.is_none() {
                    self.screen = Screen::SourceSelect;
                } else {
                    self.screen = Screen::Confirm;
                }
            }
            KeyCode::Tab | KeyCode::Down => self.select_dest_field(self.dest_field.next()),
            KeyCode::BackTab | KeyCode::Up => self.select_dest_field(self.dest_field.prev()),
            KeyCode::Left => self.move_dest_cursor_left(),
            KeyCode::Right => self.move_dest_cursor_right(),
            KeyCode::Enter => self.try_apply_destination(),
            KeyCode::Backspace => self.delete_before_dest_cursor(),
            KeyCode::Delete => self.delete_at_dest_cursor(),
            KeyCode::Char(c) => self.insert_at_dest_cursor(c),
            _ => {}
        }
    }

    fn dest_buffer_mut(&mut self) -> &mut String {
        match self.dest_field {
            DestField::ImagesRoot => &mut self.dest_images_root,
            DestField::VideosRoot => &mut self.dest_videos_root,
            DestField::ImagesTemplate => &mut self.dest_images_template,
            DestField::VideosTemplate => &mut self.dest_videos_template,
        }
    }

    fn select_dest_field(&mut self, field: DestField) {
        self.dest_field = field;
        self.dest_cursor = self.dest_buffer_mut().len();
    }

    fn move_dest_cursor_left(&mut self) {
        let cursor = self.dest_cursor;
        let next = {
            let value = self.dest_buffer_mut();
            value[..cursor].char_indices().last().map_or(0, |(i, _)| i)
        };
        self.dest_cursor = next;
    }

    fn move_dest_cursor_right(&mut self) {
        let cursor = self.dest_cursor;
        let next = {
            let value = self.dest_buffer_mut();
            if cursor < value.len() {
                cursor + value[cursor..].chars().next().unwrap().len_utf8()
            } else {
                cursor
            }
        };
        self.dest_cursor = next;
    }

    fn insert_at_dest_cursor(&mut self, c: char) {
        let cursor = self.dest_cursor;
        self.dest_buffer_mut().insert(cursor, c);
        self.dest_cursor += c.len_utf8();
    }

    fn delete_before_dest_cursor(&mut self) {
        let cursor = self.dest_cursor;
        let value = self.dest_buffer_mut();
        if let Some((start, _)) = value[..cursor].char_indices().last() {
            value.drain(start..cursor);
            self.dest_cursor = start;
        }
    }

    fn delete_at_dest_cursor(&mut self) {
        let cursor = self.dest_cursor;
        let value = self.dest_buffer_mut();
        if cursor < value.len() {
            let end = cursor + value[cursor..].chars().next().unwrap().len_utf8();
            value.drain(cursor..end);
        }
    }

    fn reset_destination_buffers(&mut self) {
        self.dest_images_root = self
            .cfg
            .paths
            .images_root
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default();
        self.dest_videos_root = self
            .cfg
            .paths
            .videos_root
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default();
        self.dest_images_template = self.cfg.paths.images_template.clone();
        self.dest_videos_template = self.cfg.paths.videos_template.clone();
        self.dest_cursor = self.dest_buffer_mut().len();
    }

    /// Validate the editor buffers; on success copy them into `self.cfg`
    /// and either ask whether to save (if changed from the on-disk config)
    /// or jump straight to Confirm.
    fn try_apply_destination(&mut self) {
        let images = self.dest_images_root.trim();
        let videos = self.dest_videos_root.trim();
        if images.is_empty() {
            self.status = Some("images path is empty".into());
            self.dest_field = DestField::ImagesRoot;
            return;
        }
        if videos.is_empty() {
            self.status = Some("videos path is empty".into());
            self.dest_field = DestField::VideosRoot;
            return;
        }
        // Validate templates by parsing.
        if let Err(e) =
            imagesync_core::template::PathTemplate::parse(self.dest_images_template.trim())
        {
            self.status = Some(format!("images template invalid: {e}"));
            self.dest_field = DestField::ImagesTemplate;
            return;
        }
        if let Err(e) =
            imagesync_core::template::PathTemplate::parse(self.dest_videos_template.trim())
        {
            self.status = Some(format!("videos template invalid: {e}"));
            self.dest_field = DestField::VideosTemplate;
            return;
        }

        let new_images = PathBuf::from(images);
        let new_videos = PathBuf::from(videos);
        let new_img_tmpl = self.dest_images_template.trim().to_string();
        let new_vid_tmpl = self.dest_videos_template.trim().to_string();

        let changed = self.cfg.paths.images_root.as_ref() != Some(&new_images)
            || self.cfg.paths.videos_root.as_ref() != Some(&new_videos)
            || self.cfg.paths.images_template != new_img_tmpl
            || self.cfg.paths.videos_template != new_vid_tmpl;

        self.cfg.paths.images_root = Some(new_images);
        self.cfg.paths.videos_root = Some(new_videos);
        self.cfg.paths.images_template = new_img_tmpl;
        self.cfg.paths.videos_template = new_vid_tmpl;
        self.status = None;

        if changed {
            self.save_prompt_next = SavePromptNext::Confirm;
            self.screen = Screen::SaveConfigPrompt;
        } else {
            self.screen = Screen::Confirm;
        }
    }

    fn on_key_save_prompt(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => self.screen = Screen::Confirm,
            KeyCode::Up | KeyCode::Char('k') => {
                self.save_action = self.save_action.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.save_action = (self.save_action + 1).min(1);
            }
            KeyCode::Enter => {
                if self.save_action == 0 {
                    match self.cfg.save(&self.cfg_path) {
                        Ok(()) => {
                            self.status =
                                Some(format!("saved config to {}", self.cfg_path.display()));
                            self.raw_mode_disk = self.cfg.filters.raw_mode;
                        }
                        Err(e) => self.status = Some(format!("save failed: {e}")),
                    }
                } else {
                    self.status = Some("using values for this session only".into());
                    self.raw_mode_disk = self.cfg.filters.raw_mode;
                }
                self.finish_save_prompt();
            }
            _ => {}
        }
    }

    fn finish_save_prompt(&mut self) {
        match self.save_prompt_next {
            SavePromptNext::Confirm => {
                self.screen = Screen::Confirm;
            }
            SavePromptNext::StartScan => {
                self.screen = Screen::Confirm;
                self.save_prompt_next = SavePromptNext::Confirm;
                self.start_scan();
            }
        }
    }

    fn on_key_scan(&mut self, key: KeyEvent) {
        if self.scan_error.is_some() {
            self.scan_error = None;
            self.plan_source = None;
            self.screen = Screen::SourceSelect;
        } else if matches!(key.code, KeyCode::Esc | KeyCode::Char('q')) && !self.scan_cancelling {
            if let Some(cancellation) = &self.scan_cancellation {
                cancellation.cancel();
                self.scan_cancelling = true;
                self.scan_status = "cancelling scan…".into();
            }
        }
    }

    async fn on_key_review(&mut self, key: KeyEvent) {
        let n = self
            .plan
            .as_ref()
            .map(|p| build_review_tree(p).len())
            .unwrap_or(0);
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.plan = None;
                self.plan_source = None;
                self.screen = Screen::SourceSelect;
            }
            KeyCode::Tab => self.review_actions_focused = !self.review_actions_focused,
            KeyCode::Down | KeyCode::Char('j') if self.review_actions_focused => {
                self.review_action = (self.review_action + 1).min(2);
            }
            KeyCode::Up | KeyCode::Char('k') if self.review_actions_focused => {
                self.review_action = self.review_action.saturating_sub(1);
            }
            KeyCode::Enter if self.review_actions_focused => match self.review_action {
                0 => self.start_sync(false),
                1 => self.start_sync(true),
                _ => {
                    self.plan = None;
                    self.plan_source = None;
                    self.screen = Screen::SourceSelect;
                }
            },
            KeyCode::Down | KeyCode::Char('j') if n > 0 => {
                let i = self.review_state.selected().unwrap_or(0);
                self.review_state.select(Some((i + 1).min(n - 1)));
            }
            KeyCode::Up | KeyCode::Char('k') if n > 0 => {
                let i = self.review_state.selected().unwrap_or(0);
                self.review_state.select(Some(i.saturating_sub(1)));
            }
            KeyCode::PageDown if n > 0 => {
                let i = self.review_state.selected().unwrap_or(0);
                self.review_state.select(Some((i + 10).min(n - 1)));
            }
            KeyCode::PageUp if n > 0 => {
                let i = self.review_state.selected().unwrap_or(0);
                self.review_state.select(Some(i.saturating_sub(10)));
            }
            _ => {}
        }
    }

    fn on_key_sync(&mut self, key: KeyEvent) {
        if matches!(key.code, KeyCode::Esc | KeyCode::Char('q')) && !self.sync_cancelling {
            if let Some(cancellation) = &self.sync_cancellation {
                cancellation.cancel();
                self.sync_cancelling = true;
                self.status = Some("cancelling sync…".into());
            }
        }
    }

    fn on_key_summary(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.summary_action = self.summary_action.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.summary_action = (self.summary_action + 1).min(1);
            }
            KeyCode::Enter => {
                if self.summary_action == 0 {
                    self.reset_for_new_run();
                } else {
                    self.should_quit = true;
                }
            }
            KeyCode::Esc => self.should_quit = true,
            _ => {}
        }
    }

    fn reset_for_new_run(&mut self) {
        self.screen = Screen::SourceSelect;
        self.chosen_source = None;
        self.chosen_label = None;
        self.detected_profile_name = None;
        self.plan = None;
        self.plan_source = None;
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
    }

    // -----------------------------------------------------------------------
    // Engine kickoff
    // -----------------------------------------------------------------------

    fn start_scan(&mut self) {
        let Some(src_path) = self.chosen_source.clone() else {
            return;
        };
        let label = self
            .chosen_label
            .clone()
            .unwrap_or_else(|| src_path.to_string_lossy().to_string());

        // Build engine config; bail on error with a status message.
        let engine_cfg = match EngineConfig::try_from_app(&self.cfg) {
            Ok(c) => c,
            Err(e) => {
                self.status = Some(format!(
                    "{e}\nSet images_root and videos_root in your config first."
                ));
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
        let (Some(plan), Some(source)) = (self.plan.take(), self.plan_source.clone()) else {
            return;
        };
        let engine_cfg = match EngineConfig::try_from_app(&self.cfg) {
            Ok(config) => config,
            Err(error) => {
                self.plan = Some(plan);
                self.status = Some(format!("config error: {error}"));
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
        let area = f.area();
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // header
                Constraint::Min(3),    // body
                Constraint::Length(1), // status
                Constraint::Length(1), // help
            ])
            .split(area);

        self.render_header(f, rows[0]);
        match self.screen {
            Screen::SourceSelect => self.render_source_select(f, rows[1]),
            Screen::Destination => self.render_destination(f, rows[1]),
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
        let title = match self.screen {
            Screen::SourceSelect => "select source",
            Screen::Destination => "destination paths",
            Screen::SaveConfigPrompt => "save to config?",
            Screen::Confirm => "confirm",
            Screen::Scan => "scanning",
            Screen::Review => "review plan",
            Screen::Sync => {
                if self.dry_run {
                    "syncing (dry run)"
                } else {
                    "syncing"
                }
            }
            Screen::Summary => "summary",
        };
        let line = Line::from(vec![
            Span::styled(
                "imagesync",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(title, Style::default().fg(Color::Yellow)),
        ]);
        f.render_widget(Paragraph::new(line), area);
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
            Screen::SourceSelect => "Tab switch focus   ↑/↓ select   Enter activate   Esc quit",
            Screen::Destination => "Tab/↑↓ field   Enter accept   Esc cancel",
            Screen::SaveConfigPrompt => "↑/↓ select action   Enter activate   Esc cancel",
            Screen::Confirm => "↑/↓ select action   Enter activate   Esc back   q quit",
            Screen::Scan => "Esc cancel",
            Screen::Review => "Tab switch focus   ↑/↓ select or scroll   Enter activate   Esc back",
            Screen::Sync => "Esc abort",
            Screen::Summary => "↑/↓ select action   Enter activate   Esc quit",
        };
        f.render_widget(
            Paragraph::new(help).style(Style::default().fg(Color::DarkGray)),
            area,
        );
    }

    // ------- Screen: SourceSelect -------

    fn render_source_select(&mut self, f: &mut Frame, area: Rect) {
        let cols = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(3),
                Constraint::Length(3),
                Constraint::Length(5),
            ])
            .split(area);

        // List
        let items: Vec<ListItem> = if self.mounts.is_empty() {
            vec![ListItem::new(
                "  (no mounts detected — press 'm' to enter a path manually)",
            )]
        } else {
            self.mounts
                .iter()
                .map(|m| {
                    let tag = match m.rank {
                        MountRank::CameraCard => {
                            Span::styled("[CAMERA]", Style::default().fg(Color::Green).bold())
                        }
                        MountRank::Removable => {
                            Span::styled("[remov.]", Style::default().fg(Color::Cyan))
                        }
                        MountRank::Other => {
                            Span::styled("[other ]", Style::default().fg(Color::DarkGray))
                        }
                    };
                    ListItem::new(Line::from(vec![
                        Span::raw(" "),
                        tag,
                        Span::raw(" "),
                        Span::raw(format!("{:<24}", trim_label(&m.label, 24))),
                        Span::raw(" "),
                        Span::styled(
                            m.path.display().to_string(),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]))
                })
                .collect()
        };

        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" detected sources "),
            )
            .highlight_style(
                Style::default()
                    .bg(Color::Blue)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("▶ ");
        f.render_stateful_widget(list, cols[0], &mut self.sources_state);

        let title = if self.manual_focused {
            " manual path (Enter to accept, Tab actions) "
        } else {
            " manual path (Tab to edit) "
        };
        let style = if self.manual_focused {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let display = if self.manual_focused {
            format!("{}_", self.manual_path)
        } else {
            self.manual_path.clone()
        };
        let para = Paragraph::new(display)
            .style(style)
            .block(Block::default().borders(Borders::ALL).title(title));
        f.render_widget(para, cols[1]);

        let actions = vec![
            action_line(
                self.source_actions_focused && self.source_action == 0,
                "Enter a path manually",
            ),
            action_line(
                self.source_actions_focused && self.source_action == 1,
                "Refresh sources",
            ),
            action_line(
                self.source_actions_focused && self.source_action == 2,
                "Quit",
            ),
        ];
        f.render_widget(
            Paragraph::new(actions)
                .block(Block::default().borders(Borders::ALL).title(" actions ")),
            cols[2],
        );
    }

    // ------- Screen: Destination -------

    fn render_destination(&self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Length(3),
                Constraint::Length(3),
                Constraint::Length(3),
                Constraint::Min(1),
            ])
            .split(area);

        self.render_dest_field(
            f,
            rows[0],
            DestField::ImagesRoot,
            " images root ",
            &self.dest_images_root,
        );
        self.render_dest_field(
            f,
            rows[1],
            DestField::VideosRoot,
            " videos root ",
            &self.dest_videos_root,
        );
        self.render_dest_field(
            f,
            rows[2],
            DestField::ImagesTemplate,
            " images template ",
            &self.dest_images_template,
        );
        self.render_dest_field(
            f,
            rows[3],
            DestField::VideosTemplate,
            " videos template ",
            &self.dest_videos_template,
        );

        let hint = vec![
            Line::raw(""),
            Line::from(Span::styled(
                "Templates use tokens like {yyyy}, {mm}, {dd}, {month}, {HH}, {MM}, {SS}.",
                Style::default().fg(Color::DarkGray),
            )),
            Line::from(Span::styled(
                "Tab moves between fields. Enter accepts. Esc cancels.",
                Style::default().fg(Color::DarkGray),
            )),
        ];
        f.render_widget(Paragraph::new(hint).wrap(Wrap { trim: false }), rows[4]);
    }

    fn render_dest_field(
        &self,
        f: &mut Frame,
        area: Rect,
        field: DestField,
        title: &str,
        value: &str,
    ) {
        let focused = self.dest_field == field;
        let style = if focused {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default().fg(Color::Gray)
        };
        let display = if focused {
            let cursor = self.dest_cursor.min(value.len());
            format!("{}_{}", &value[..cursor], &value[cursor..])
        } else {
            value.to_string()
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(if focused {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default().fg(Color::DarkGray)
            });
        f.render_widget(Paragraph::new(display).style(style).block(block), area);
    }

    // ------- Screen: SaveConfigPrompt -------

    fn render_save_prompt(&self, f: &mut Frame, area: Rect) {
        let (title, intro, body_lines): (&str, &str, Vec<Line>) = match self.save_prompt_next {
            SavePromptNext::Confirm => (
                " save destinations? ",
                "Save these destination paths to your config file?",
                vec![
                    Line::from(vec![
                        Span::styled("  Images:   ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!(
                            "{}/{}",
                            self.dest_images_root.trim(),
                            self.dest_images_template.trim()
                        )),
                    ]),
                    Line::from(vec![
                        Span::styled("  Videos:   ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!(
                            "{}/{}",
                            self.dest_videos_root.trim(),
                            self.dest_videos_template.trim()
                        )),
                    ]),
                ],
            ),
            SavePromptNext::StartScan => (
                " save raw mode? ",
                "Save this raw-mode setting to your config file?",
                vec![Line::from(vec![
                    Span::styled("  Raw mode: ", Style::default().fg(Color::Cyan)),
                    Span::raw(raw_mode_label(self.cfg.filters.raw_mode)),
                ])],
            ),
        };

        let mut body = vec![
            Line::raw(""),
            Line::from(Span::styled(intro, Style::default().fg(Color::Yellow))),
            Line::raw(""),
            Line::from(vec![
                Span::styled("  Path: ", Style::default().fg(Color::Cyan)),
                Span::raw(self.cfg_path.display().to_string()),
            ]),
            Line::raw(""),
        ];
        body.extend(body_lines);
        body.push(Line::raw(""));
        body.push(action_line(self.save_action == 0, "Save to config"));
        body.push(action_line(self.save_action == 1, "Use this session only"));
        f.render_widget(
            Paragraph::new(body)
                .block(Block::default().borders(Borders::ALL).title(title))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    // ------- Screen: Confirm -------

    fn render_confirm(&self, f: &mut Frame, area: Rect) {
        let images = self
            .cfg
            .paths
            .images_root
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(unset)".into());
        let videos = self
            .cfg
            .paths
            .videos_root
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(unset)".into());
        let body = vec![
            Line::from(vec![
                Span::styled("Source: ", Style::default().fg(Color::Cyan)),
                Span::raw(
                    self.chosen_source
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default(),
                ),
            ]),
            Line::from(vec![
                Span::styled("Profile: ", Style::default().fg(Color::Cyan)),
                Span::raw(self.detected_profile_name.clone().unwrap_or_default()),
            ]),
            Line::raw(""),
            Line::from(vec![
                Span::styled("Images → ", Style::default().fg(Color::Cyan)),
                Span::raw(format!("{images}/{}", self.cfg.paths.images_template)),
            ]),
            Line::from(vec![
                Span::styled("Videos → ", Style::default().fg(Color::Cyan)),
                Span::raw(format!("{videos}/{}", self.cfg.paths.videos_template)),
            ]),
            Line::raw(""),
            Line::from(vec![
                Span::styled("Raw mode: ", Style::default().fg(Color::Cyan)),
                Span::raw(raw_mode_label(self.cfg.filters.raw_mode)),
                Span::styled(
                    "   (select the action below to change)",
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::raw(""),
            action_line(self.confirm_action == 0, "Scan and review"),
            action_line(self.confirm_action == 1, "Edit destinations"),
            action_line(
                self.confirm_action == 2,
                &format!(
                    "Change RAW mode ({})",
                    raw_mode_label(self.cfg.filters.raw_mode)
                ),
            ),
            action_line(self.confirm_action == 3, "Back"),
        ];
        let para = Paragraph::new(body)
            .block(Block::default().borders(Borders::ALL).title(" confirm "))
            .wrap(Wrap { trim: false });
        f.render_widget(para, area);
    }

    // ------- Screen: Scan -------

    fn render_scan(&self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Length(3),
                Constraint::Min(1),
            ])
            .split(area);

        if let Some(err) = &self.scan_error {
            let status = Paragraph::new("scan failed")
                .style(Style::default().fg(Color::Red).bold())
                .block(Block::default().borders(Borders::ALL).title(" status "));
            f.render_widget(status, rows[0]);

            let body = Paragraph::new(err.clone())
                .style(Style::default().fg(Color::Red))
                .wrap(Wrap { trim: false })
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" error — press any key to go back "),
                );
            f.render_widget(body, rows[1].union(rows[2]));
            return;
        }

        let status = Paragraph::new(self.scan_status.clone())
            .block(Block::default().borders(Borders::ALL).title(" status "));
        f.render_widget(status, rows[0]);

        // The scan phase only lists files and builds the plan — no EXIF here
        // (capture dates are read later, on the local copies, during sync).
        let gauge = Gauge::default()
            .block(Block::default().borders(Borders::ALL).title(" planning "))
            .gauge_style(Style::default().fg(Color::Cyan))
            .ratio(if self.scan_files_total > 0 { 1.0 } else { 0.0 })
            .label("building plan…");
        f.render_widget(gauge, rows[1]);

        let info = Paragraph::new(format!("{} files enumerated.", self.scan_files_total))
            .style(Style::default().fg(Color::DarkGray))
            .block(Block::default().borders(Borders::ALL));
        f.render_widget(info, rows[2]);
    }

    // ------- Screen: Review -------

    fn render_review(&mut self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .constraints([Constraint::Length(7), Constraint::Min(3)])
            .split(area);
        let top = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(32), Constraint::Min(20)])
            .split(rows[0]);

        let plan = match &self.plan {
            Some(p) => p,
            None => return,
        };
        let action_border = if self.review_actions_focused {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let actions = vec![
            action_line(
                self.review_actions_focused && self.review_action == 0,
                "Start import",
            ),
            action_line(
                self.review_actions_focused && self.review_action == 1,
                "Dry run",
            ),
            action_line(
                self.review_actions_focused && self.review_action == 2,
                "Back",
            ),
        ];
        f.render_widget(
            Paragraph::new(actions).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" actions ")
                    .border_style(action_border),
            ),
            top[0],
        );

        let summary = vec![
            Line::from(vec![
                Span::styled("Copies: ", Style::default().fg(Color::Green).bold()),
                Span::raw(plan.copies().to_string()),
                Span::raw("    "),
                Span::styled("Skips: ", Style::default().fg(Color::Cyan)),
                Span::raw(plan.skips().to_string()),
                Span::raw("    "),
                Span::styled("Errors: ", Style::default().fg(Color::Red)),
                Span::raw(plan.errors().to_string()),
            ]),
            Line::raw(""),
            Line::from(Span::styled(
                "Destination folders resolve during import; this is a provisional source-file list.",
                Style::default().fg(Color::DarkGray),
            )),
            Line::from(Span::styled(
                "Tab switches between actions and preview.",
                Style::default().fg(Color::DarkGray),
            )),
        ];
        f.render_widget(
            Paragraph::new(summary).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" plan ")
                    .border_style(Style::default().fg(Color::DarkGray)),
            ),
            top[1],
        );

        let items = build_review_tree(plan);
        let item_count = items.len();
        let preview_border = if self.review_actions_focused {
            Style::default().fg(Color::DarkGray)
        } else {
            Style::default().fg(Color::Yellow)
        };
        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!(
                        " provisional preview ({} new files) ",
                        plan.copies()
                    ))
                    .border_style(preview_border),
            )
            .highlight_style(if self.review_actions_focused {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default().bg(Color::Blue).fg(Color::White)
            });
        f.render_stateful_widget(list, rows[1], &mut self.review_state);
        if item_count > rows[1].height.saturating_sub(2) as usize {
            let mut scrollbar_state =
                ScrollbarState::new(item_count).position(self.review_state.selected().unwrap_or(0));
            f.render_stateful_widget(
                Scrollbar::new(ScrollbarOrientation::VerticalRight),
                rows[1],
                &mut scrollbar_state,
            );
        }
    }

    // ------- Screen: Sync -------

    fn render_sync(&self, f: &mut Frame, area: Rect) {
        // Decide how many lanes we can fit. Each lane needs 3 rows
        // (border + bar + border). Total gauge takes 3, log needs at
        // least 3.
        let lanes_requested = self.sync_slots.len();
        let max_fit = (area.height as usize).saturating_sub(3 + 3) / 3;
        let lanes_visible = lanes_requested.min(max_fit).max(1);
        let lanes_hidden = lanes_requested.saturating_sub(lanes_visible);

        let mut constraints: Vec<Constraint> = Vec::with_capacity(lanes_visible + 2);
        constraints.push(Constraint::Length(3));
        for _ in 0..lanes_visible {
            constraints.push(Constraint::Length(3));
        }
        constraints.push(Constraint::Min(3));
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints(constraints)
            .split(area);

        // Progress tracks files that required import work. Runtime skips are
        // reported separately and never make the import bar appear complete.
        let completed = self.sync_progress.copied + self.sync_progress.failed;
        let pct = if self.sync_copy_total > 0 {
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
        let total_gauge = Gauge::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(if self.dry_run {
                        " progress (dry run) "
                    } else {
                        " progress "
                    }),
            )
            .gauge_style(Style::default().fg(Color::Green))
            .ratio(pct)
            .label(total_label);
        f.render_widget(total_gauge, rows[0]);

        for i in 0..lanes_visible {
            let lane_area = rows[1 + i];
            let title = format!(" worker {} ", i + 1);
            let (lane_pct, lane_label) = match self.sync_slots.get(i).and_then(|slot| slot.as_ref())
            {
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
            let gauge = Gauge::default()
                .block(Block::default().borders(Borders::ALL).title(title))
                .gauge_style(Style::default().fg(Color::Cyan))
                .ratio(lane_pct)
                .label(lane_label);
            f.render_widget(gauge, lane_area);
        }

        let log_area = rows[1 + lanes_visible];
        let log_height = log_area.height.saturating_sub(2) as usize;
        let first_line = self.sync_log.len().saturating_sub(log_height);
        let body: Vec<Line> = self.sync_log[first_line..]
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
            .collect();
        f.render_widget(
            Paragraph::new(body).block(Block::default().borders(Borders::ALL).title(" log ")),
            log_area,
        );
    }

    // ------- Screen: Summary -------

    fn render_summary(&self, f: &mut Frame, area: Rect) {
        let summary = self.sync_summary.unwrap_or_default();
        let (heading, heading_color) = if let Some(error) = &self.sync_error {
            (format!("Sync did not complete: {error}"), Color::Red)
        } else if summary.cancelled {
            (
                "Sync cancelled — core-reported partial results below.".into(),
                Color::Yellow,
            )
        } else if self.dry_run {
            (
                "Dry run complete. No files were written.".into(),
                Color::Green,
            )
        } else {
            ("Sync complete.".into(), Color::Green)
        };
        let mut body = vec![
            Line::raw(""),
            Line::from(Span::styled(
                heading,
                Style::default().fg(heading_color).bold(),
            )),
            Line::raw(""),
        ];
        if self.sync_summary.is_some() {
            body.extend([
                Line::from(vec![
                    Span::styled("Completed: ", Style::default().fg(Color::Cyan)),
                    Span::raw(format!("{}/{}", summary.completed(), summary.total)),
                ]),
                Line::from(vec![
                    Span::styled("Copied:    ", Style::default().fg(Color::Green)),
                    Span::raw(summary.copied.to_string()),
                ]),
                Line::from(vec![
                    Span::styled("Skipped:   ", Style::default().fg(Color::Cyan)),
                    Span::raw(summary.skipped.to_string()),
                ]),
                Line::from(vec![
                    Span::styled("Failed:    ", Style::default().fg(Color::Red)),
                    Span::raw(summary.failed.to_string()),
                ]),
            ]);
        }
        if !self.sync_diagnostics.is_empty() {
            body.push(Line::raw(""));
            body.push(Line::from(Span::styled(
                "Warnings and errors:",
                Style::default().fg(Color::Yellow),
            )));
            body.extend(
                self.sync_diagnostics
                    .iter()
                    .map(|diagnostic| Line::raw(diagnostic.clone())),
            );
        }
        body.push(Line::raw(""));
        body.push(action_line(
            self.summary_action == 0,
            "Import another source",
        ));
        body.push(action_line(self.summary_action == 1, "Quit"));
        f.render_widget(
            Paragraph::new(body)
                .block(Block::default().borders(Borders::ALL).title(" done "))
                .wrap(Wrap { trim: false }),
            area,
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

fn action_line(selected: bool, label: &str) -> Line<'static> {
    let marker = if selected { "> " } else { "  " };
    let style = if selected {
        Style::default().bg(Color::Blue).fg(Color::White)
    } else {
        Style::default()
    };
    Line::from(Span::styled(format!("{marker}{label}"), style))
}

fn raw_mode_label(m: RawMode) -> &'static str {
    match m {
        RawMode::All => "All (RAW + JPEG)",
        RawMode::RawOnly => "RAW only",
        RawMode::NonRawOnly => "Non-RAW only",
    }
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
}
