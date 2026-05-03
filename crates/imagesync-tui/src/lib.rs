//! `imagesync-tui` — ratatui frontend for imagesync.
//!
//! Single-window app with five screens reached in order:
//!
//!   SourceSelect  →  Confirm  →  Scan  →  Review  →  Sync  →  Summary
//!
//! Architecture: the TUI runs the render loop on the main task and drives
//! the engine on separate tokio tasks. Engine events flow back through an
//! `mpsc::UnboundedReceiver<EngineEvent>` that's polled non-blockingly each
//! frame. Crossterm input events are polled with a short timeout so we can
//! interleave rendering and engine progress without spawning a separate
//! input thread.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use imagesync_core::config::{AppConfig, RawMode};
use imagesync_core::engine::ExecuteOptions;
use imagesync_core::events::{CopyOutcome, EngineEvent, MediaKindWire, PlannedAction, PlannedFile};
use imagesync_core::mount::{detect_mounts, DetectedMount, MountRank};
use imagesync_core::plan::SyncPlan;
use imagesync_core::source::{FilesystemSource, MediaSource};
use imagesync_core::{Engine, EngineConfig, ProfileRegistry};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the TUI. Sets up the terminal, runs the app loop, restores on exit.
pub async fn run() -> Result<()> {
    // Probe ExifTool early so users see a clear error instead of a cryptic
    // failure mid-scan.
    if let Err(e) = imagesync_core::exiftool::probe_version() {
        anyhow::bail!(
            "{e}\n\nInstall ExifTool:\n  Linux:   sudo apt install libimage-exiftool-perl\n  macOS:   brew install exiftool\n  Windows: winget install OliverBetz.ExifTool"
        );
    }

    let (app_cfg, cfg_path) = load_app_config()?;
    let registry = ProfileRegistry::with_builtins()
        .context("loading built-in camera profiles")?;

    let mut terminal = ratatui::try_init().context("initialising terminal")?;
    let result = App::new(app_cfg, cfg_path, registry).run(&mut terminal).await;
    ratatui::restore();
    result
}

fn load_app_config() -> Result<(AppConfig, PathBuf)> {
    let path = AppConfig::default_path()
        .context("could not determine config dir on this platform")?;
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

type ScanResult = Result<(SyncPlan, Arc<dyn MediaSource>), String>;
type ScanHandle = tokio::task::JoinHandle<ScanResult>;

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

    // Destination editor (live buffers; copied to cfg on accept)
    dest_images_root: String,
    dest_videos_root: String,
    dest_images_template: String,
    dest_videos_template: String,
    dest_field: DestField,

    // Filters (raw_mode picker on Confirm)
    raw_mode_disk: RawMode,
    save_prompt_next: SavePromptNext,

    // After Confirm
    chosen_source: Option<PathBuf>,
    chosen_label: Option<String>,
    detected_profile_name: Option<String>,

    // Scan
    scan_rx: Option<mpsc::UnboundedReceiver<EngineEvent>>,
    scan_handle: Option<ScanHandle>,
    scan_status: String,
    scan_files_total: u64,
    meta_done: u64,
    meta_total: u64,

    // Review
    plan: Option<SyncPlan>,
    plan_source: Option<Arc<dyn MediaSource>>,
    review_state: ListState,

    // Sync
    sync_rx: Option<mpsc::UnboundedReceiver<EngineEvent>>,
    sync_handle: Option<tokio::task::JoinHandle<()>>,
    sync_total_copy: u64,
    sync_done_copy: u64,
    sync_failed: u64,
    sync_skipped: u64,
    sync_log: Vec<String>,
    sync_current: Option<String>,
    sync_current_progress: Option<(u64, u64)>,
    dry_run: bool,

    // Final summary
    summary_copied: u64,
    summary_skipped: u64,
    summary_failed: u64,
    summary_cancelled: bool,
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
            dest_images_root,
            dest_videos_root,
            dest_images_template,
            dest_videos_template,
            dest_field: DestField::ImagesRoot,
            raw_mode_disk,
            save_prompt_next: SavePromptNext::Confirm,
            chosen_source: None,
            chosen_label: None,
            detected_profile_name: None,
            scan_rx: None,
            scan_handle: None,
            scan_status: String::new(),
            scan_files_total: 0,
            meta_done: 0,
            meta_total: 0,
            plan: None,
            plan_source: None,
            review_state: ListState::default(),
            sync_rx: None,
            sync_handle: None,
            sync_total_copy: 0,
            sync_done_copy: 0,
            sync_failed: 0,
            sync_skipped: 0,
            sync_log: Vec::new(),
            sync_current: None,
            sync_current_progress: None,
            dry_run: false,
            summary_copied: 0,
            summary_skipped: 0,
            summary_failed: 0,
            summary_cancelled: false,
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

        // Best-effort: cancel any running engine task on quit.
        if let Some(h) = self.scan_handle.take() {
            h.abort();
        }
        if let Some(h) = self.sync_handle.take() {
            h.abort();
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Engine plumbing
    // -----------------------------------------------------------------------

    fn pump_engine_events(&mut self) {
        // Drain whichever channel is currently active. Borrow rules force
        // taking ownership of buffered events first, then dispatching.
        let mut events: Vec<EngineEvent> = Vec::new();
        if let Some(rx) = self.scan_rx.as_mut() {
            while let Ok(ev) = rx.try_recv() {
                events.push(ev);
            }
        }
        for ev in events.drain(..) {
            self.on_scan_event(ev);
        }

        if let Some(rx) = self.sync_rx.as_mut() {
            while let Ok(ev) = rx.try_recv() {
                events.push(ev);
            }
        }
        for ev in events.drain(..) {
            self.on_sync_event(ev);
        }
    }

    async fn poll_handles(&mut self) {
        // Scan task complete?
        if self
            .scan_handle
            .as_ref()
            .map(|h| h.is_finished())
            .unwrap_or(false)
        {
            let h = self.scan_handle.take().unwrap();
            match h.await {
                Ok(Ok((plan, src))) => {
                    self.plan = Some(plan);
                    self.plan_source = Some(src);
                    self.review_state.select(Some(0));
                    self.screen = Screen::Review;
                    self.scan_rx = None;
                }
                Ok(Err(msg)) => {
                    self.status = Some(format!("scan failed: {msg}"));
                    self.screen = Screen::SourceSelect;
                    self.scan_rx = None;
                }
                Err(e) => {
                    self.status = Some(format!("scan task panicked: {e}"));
                    self.screen = Screen::SourceSelect;
                    self.scan_rx = None;
                }
            }
        }

        if self
            .sync_handle
            .as_ref()
            .map(|h| h.is_finished())
            .unwrap_or(false)
        {
            let h = self.sync_handle.take().unwrap();
            let _ = h.await;
            // Drain remaining buffered events (notably SyncSummary) before
            // flipping to the summary screen — without this, the summary
            // shows zeroes when the sync completed faster than one tick.
            let mut leftover: Vec<EngineEvent> = Vec::new();
            if let Some(rx) = self.sync_rx.as_mut() {
                while let Ok(ev) = rx.try_recv() {
                    leftover.push(ev);
                }
            }
            for ev in leftover {
                self.on_sync_event(ev);
            }
            self.sync_rx = None;
            self.screen = Screen::Summary;
        }
    }

    fn on_scan_event(&mut self, ev: EngineEvent) {
        match ev {
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
            EngineEvent::MetadataProgress { done, total } => {
                self.meta_done = done;
                self.meta_total = total;
            }
            EngineEvent::PlanReady { copies, skips, errors } => {
                self.scan_status = format!(
                    "plan ready: {copies} copies, {skips} skips, {errors} errors"
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

    fn on_sync_event(&mut self, ev: EngineEvent) {
        match ev {
            EngineEvent::CopyStarted { file } => {
                self.sync_current = Some(file.source_rel_path);
                self.sync_current_progress = Some((0, file.source_size));
            }
            EngineEvent::CopyProgress { rel_path, bytes_done, bytes_total } => {
                self.sync_current = Some(rel_path);
                self.sync_current_progress = Some((bytes_done, bytes_total));
            }
            EngineEvent::CopyComplete { file, outcome } => match outcome {
                CopyOutcome::Copied { .. } => {
                    if file.action == PlannedAction::Copy {
                        self.sync_done_copy += 1;
                    }
                    push_log(
                        &mut self.sync_log,
                        format!(" OK   {}", file.source_rel_path),
                    );
                }
                CopyOutcome::Skipped { reason } => {
                    self.sync_skipped += 1;
                    push_log(
                        &mut self.sync_log,
                        format!(" SKIP {} ({reason})", file.source_rel_path),
                    );
                }
                CopyOutcome::Failed { error } => {
                    self.sync_failed += 1;
                    push_log(
                        &mut self.sync_log,
                        format!(" FAIL {} ({error})", file.source_rel_path),
                    );
                }
            },
            EngineEvent::SyncSummary { copied, skipped, failed } => {
                self.summary_copied = copied;
                self.summary_skipped = skipped;
                self.summary_failed = failed;
            }
            _ => {}
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
                KeyCode::Esc => {
                    self.manual_focused = false;
                }
                KeyCode::Tab => {
                    self.manual_focused = false;
                }
                KeyCode::Enter => {
                    let p = PathBuf::from(self.manual_path.trim());
                    if p.as_os_str().is_empty() {
                        self.status = Some("path is empty".into());
                        return;
                    }
                    if !p.is_dir() {
                        self.status = Some(format!("not a directory: {}", p.display()));
                        return;
                    }
                    self.choose_source(p);
                }
                KeyCode::Backspace => {
                    self.manual_path.pop();
                }
                KeyCode::Char(c) => {
                    self.manual_path.push(c);
                }
                _ => {}
            }
            return;
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.should_quit = true;
            }
            KeyCode::Char('r') => {
                self.mounts = detect_mounts();
                self.sources_state
                    .select(if self.mounts.is_empty() { None } else { Some(0) });
                self.status = Some("rescanned mounts".into());
            }
            KeyCode::Char('m') | KeyCode::Tab => {
                self.manual_focused = true;
            }
            KeyCode::Down | KeyCode::Char('j') if !self.mounts.is_empty() => {
                let i = self.sources_state.selected().unwrap_or(0);
                self.sources_state
                    .select(Some((i + 1) % self.mounts.len()));
            }
            KeyCode::Up | KeyCode::Char('k') if !self.mounts.is_empty() => {
                let i = self.sources_state.selected().unwrap_or(0);
                self.sources_state.select(Some(
                    if i == 0 { self.mounts.len() - 1 } else { i - 1 },
                ));
            }
            KeyCode::Enter => {
                if let Some(i) = self.sources_state.selected() {
                    if let Some(m) = self.mounts.get(i).cloned() {
                        self.choose_source(m.path);
                    }
                }
            }            _ => {}
        }
    }

    fn choose_source(&mut self, path: PathBuf) {
        let label = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| path.to_string_lossy().to_string());

        // Resolve which profile would be used.
        let profile = self.registry.detect_for_source(&path);
        self.detected_profile_name = Some(profile.display_name.clone());
        self.chosen_label = Some(label);
        self.chosen_source = Some(path);
        self.status = None;

        // If destination roots are missing, force the user to fill them in
        // before showing the Confirm screen.
        if self.cfg.paths.images_root.is_none() || self.cfg.paths.videos_root.is_none() {
            self.dest_field = if self.cfg.paths.images_root.is_none() {
                DestField::ImagesRoot
            } else {
                DestField::VideosRoot
            };
            self.screen = Screen::Destination;
        } else {
            self.screen = Screen::Confirm;
        }
    }

    async fn on_key_confirm(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.screen = Screen::SourceSelect;
            }
            KeyCode::Enter => {
                if self.cfg.filters.raw_mode != self.raw_mode_disk {
                    self.save_prompt_next = SavePromptNext::StartScan;
                    self.screen = Screen::SaveConfigPrompt;
                } else {
                    self.start_scan();
                }
            }
            KeyCode::Char('e') => {
                self.dest_field = DestField::ImagesRoot;
                self.screen = Screen::Destination;
            }
            KeyCode::Char('r') | KeyCode::Char('R') => {
                self.cfg.filters.raw_mode = match self.cfg.filters.raw_mode {
                    RawMode::All => RawMode::RawOnly,
                    RawMode::RawOnly => RawMode::NonRawOnly,
                    RawMode::NonRawOnly => RawMode::All,
                };
            }
            KeyCode::Char('q') => {
                self.should_quit = true;
            }
            _ => {}
        }
    }

    // ------- Destination editor -------

    fn on_key_destination(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                // Back out without applying. If we got here because roots
                // are missing, return to SourceSelect; otherwise to Confirm.
                if self.cfg.paths.images_root.is_none()
                    || self.cfg.paths.videos_root.is_none()
                {
                    self.screen = Screen::SourceSelect;
                } else {
                    self.screen = Screen::Confirm;
                }
            }
            KeyCode::Tab | KeyCode::Down => {
                self.dest_field = self.dest_field.next();
            }
            KeyCode::BackTab | KeyCode::Up => {
                self.dest_field = self.dest_field.prev();
            }
            KeyCode::Enter => {
                self.try_apply_destination();
            }
            KeyCode::Backspace => {
                self.dest_buffer_mut().pop();
            }
            KeyCode::Char(c) => {
                self.dest_buffer_mut().push(c);
            }
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
        if let Err(e) = imagesync_core::template::PathTemplate::parse(
            self.dest_images_template.trim(),
        ) {
            self.status = Some(format!("images template invalid: {e}"));
            self.dest_field = DestField::ImagesTemplate;
            return;
        }
        if let Err(e) = imagesync_core::template::PathTemplate::parse(
            self.dest_videos_template.trim(),
        ) {
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
            KeyCode::Char('y') | KeyCode::Char('Y') => {
                match self.cfg.save(&self.cfg_path) {
                    Ok(()) => {
                        self.status =
                            Some(format!("saved config to {}", self.cfg_path.display()));
                        // Update on-disk snapshot so we don't re-prompt.
                        self.raw_mode_disk = self.cfg.filters.raw_mode;
                    }
                    Err(e) => {
                        self.status = Some(format!("save failed: {e}"));
                    }
                }
                self.finish_save_prompt();
            }
            KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc | KeyCode::Enter => {
                // Skip saving; values apply for this session only.
                self.status = Some("using values for this session only".into());
                // Treat session-only acceptance as "don't re-prompt this session".
                self.raw_mode_disk = self.cfg.filters.raw_mode;
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
        if matches!(key.code, KeyCode::Esc | KeyCode::Char('q')) {
            if let Some(h) = self.scan_handle.take() {
                h.abort();
            }
            self.scan_rx = None;
            self.screen = Screen::SourceSelect;
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
            KeyCode::Char('s') => {
                self.start_sync(false);
            }
            KeyCode::Char('d') => {
                self.start_sync(true);
            }
            _ => {}
        }
    }

    fn on_key_sync(&mut self, key: KeyEvent) {
        if matches!(key.code, KeyCode::Esc | KeyCode::Char('q')) {
            // Abort: cancel the engine task. The engine will stop spawning
            // new copies; in-flight copies finish.
            if let Some(h) = self.sync_handle.take() {
                h.abort();
            }
            self.sync_rx = None;
            // The engine never gets to send SyncSummary on abort, so seed
            // the summary from the live counters we've been maintaining.
            self.summary_copied = self.sync_done_copy;
            self.summary_skipped = self.sync_skipped;
            self.summary_failed = self.sync_failed;
            self.summary_cancelled = true;
            self.screen = Screen::Summary;
        }
    }

    fn on_key_summary(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc | KeyCode::Enter => {
                self.should_quit = true;
            }
            KeyCode::Char('n') => {
                // Start over.
                self.reset_for_new_run();
            }
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
        self.sync_total_copy = 0;
        self.sync_done_copy = 0;
        self.sync_failed = 0;
        self.sync_skipped = 0;
        self.sync_log.clear();
        self.sync_current = None;
        self.sync_current_progress = None;
        self.summary_copied = 0;
        self.summary_skipped = 0;
        self.summary_failed = 0;
        self.summary_cancelled = false;
        self.dry_run = false;
        self.status = None;
        self.mounts = detect_mounts();
    }

    // -----------------------------------------------------------------------
    // Engine kickoff
    // -----------------------------------------------------------------------

    fn start_scan(&mut self) {
        let Some(src_path) = self.chosen_source.clone() else { return };
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
        let source: Arc<dyn MediaSource> = FilesystemSource::new(
            src_path.to_string_lossy().to_string(),
            label,
            src_path,
        )
        .into_arc();

        let (tx, rx) = mpsc::unbounded_channel::<EngineEvent>();
        self.scan_rx = Some(rx);
        self.scan_status = "starting…".into();
        self.scan_files_total = 0;
        self.meta_done = 0;
        self.meta_total = 0;
        self.screen = Screen::Scan;

        let handle = tokio::spawn(async move {
            let engine = Engine::new(engine_cfg, registry);
            let (plan_handle, mut events) = engine.scan_and_plan(source.clone());
            // Forward live events as the engine emits them.
            while let Some(ev) = events.next().await {
                let _ = tx.send(ev);
            }
            // Stream is closed when the engine task drops the sender, i.e.
            // when the work has finished. Then collect the plan.
            match plan_handle.await {
                Ok(Ok(plan)) => Ok((plan, source)),
                Ok(Err(e)) => Err(format!("{e}")),
                Err(e) => Err(format!("scan task: {e}")),
            }
        });
        self.scan_handle = Some(handle);
    }

    fn start_sync(&mut self, dry_run: bool) {
        let (Some(plan), Some(source)) = (self.plan.take(), self.plan_source.clone()) else {
            return;
        };
        self.dry_run = dry_run;
        self.sync_total_copy = plan.copies();
        self.sync_done_copy = 0;
        self.sync_failed = 0;
        self.sync_skipped = 0;
        self.sync_log.clear();
        self.sync_current = None;
        self.sync_current_progress = None;
        self.summary_copied = 0;
        self.summary_skipped = 0;
        self.summary_failed = 0;
        self.summary_cancelled = false;
        self.screen = Screen::Sync;

        let engine_cfg = match EngineConfig::try_from_app(&self.cfg) {
            Ok(c) => c,
            Err(e) => {
                self.status = Some(format!("config error: {e}"));
                self.screen = Screen::Review;
                return;
            }
        };
        let registry = self.registry.clone();

        let (tx, rx) = mpsc::unbounded_channel::<EngineEvent>();
        self.sync_rx = Some(rx);

        let handle = tokio::spawn(async move {
            let engine = Engine::new(engine_cfg, registry);
            let mut events = engine.execute(plan, source, ExecuteOptions { dry_run });
            while let Some(ev) = events.next().await {
                let _ = tx.send(ev);
            }
        });
        self.sync_handle = Some(handle);
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
            Screen::SourceSelect => "↑/↓ pick   Enter select   m manual path   r rescan   q quit",
            Screen::Destination => "Tab/↑↓ field   Enter accept   Esc cancel",
            Screen::SaveConfigPrompt => "y save   n / Enter session-only   Esc cancel",
            Screen::Confirm => "Enter scan   e edit destinations   r raw mode   Esc back   q quit",
            Screen::Scan => "Esc cancel",
            Screen::Review => "↑/↓ scroll   s sync   d dry-run   Esc back   q quit",
            Screen::Sync => "Esc abort",
            Screen::Summary => "n new run   Enter/q quit",
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
            .constraints([Constraint::Min(3), Constraint::Length(3)])
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
                        MountRank::CameraCard => Span::styled(
                            "[CAMERA]",
                            Style::default().fg(Color::Green).bold(),
                        ),
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

        // Manual path input
        let title = if self.manual_focused {
            " manual path (Enter to accept, Esc to cancel) "
        } else {
            " manual path (press m or Tab to edit) "
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
            format!("{value}_")
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
        body.push(Line::from(Span::styled(
            "  [y] save to config        [n] use this session only        [Esc] cancel",
            Style::default().fg(Color::DarkGray),
        )));
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
                Span::styled("   (press r to cycle)", Style::default().fg(Color::DarkGray)),
            ]),
            Line::raw(""),
            Line::from(Span::styled(
                "Press Enter to scan and build a plan. Nothing is copied yet.",
                Style::default().fg(Color::Yellow),
            )),
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

        let status = Paragraph::new(self.scan_status.clone())
            .block(Block::default().borders(Borders::ALL).title(" status "));
        f.render_widget(status, rows[0]);

        let pct = if self.meta_total > 0 {
            (self.meta_done as f64 / self.meta_total as f64).min(1.0)
        } else {
            0.0
        };
        let label = if self.meta_total > 0 {
            format!("metadata {}/{}", self.meta_done, self.meta_total)
        } else {
            "waiting…".to_string()
        };
        let gauge = Gauge::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" reading metadata "),
            )
            .gauge_style(Style::default().fg(Color::Cyan))
            .ratio(pct)
            .label(label);
        f.render_widget(gauge, rows[1]);

        let info = Paragraph::new(format!(
            "{} files enumerated. ExifTool runs in stay-open mode.",
            self.scan_files_total
        ))
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::ALL));
        f.render_widget(info, rows[2]);
    }

    // ------- Screen: Review -------

    fn render_review(&mut self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(4), Constraint::Min(3)])
            .split(area);

        let plan = match &self.plan {
            Some(p) => p,
            None => return,
        };

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
                "Press 's' to copy, 'd' to dry-run, Esc to back out.",
                Style::default().fg(Color::Yellow),
            )),
        ];
        f.render_widget(
            Paragraph::new(summary)
                .block(Block::default().borders(Borders::ALL).title(" plan ")),
            rows[0],
        );

        let items = build_review_tree(plan);

        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!(" preview ({} new files) ", plan.copies())),
            )
            .highlight_style(Style::default().bg(Color::Blue).fg(Color::White));
        f.render_stateful_widget(list, rows[1], &mut self.review_state);
    }

    // ------- Screen: Sync -------

    fn render_sync(&self, f: &mut Frame, area: Rect) {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // total
                Constraint::Length(3), // current file
                Constraint::Min(3),    // log
            ])
            .split(area);

        // Total progress
        let pct = if self.sync_total_copy > 0 {
            ((self.sync_done_copy + self.sync_failed) as f64
                / self.sync_total_copy as f64)
                .min(1.0)
        } else {
            1.0
        };
        let total_label = format!(
            "{}/{} copied · {} failed · {} skipped",
            self.sync_done_copy, self.sync_total_copy, self.sync_failed, self.sync_skipped
        );
        let total_gauge = Gauge::default()
            .block(
                Block::default().borders(Borders::ALL).title(if self.dry_run {
                    " progress (dry run) "
                } else {
                    " progress "
                }),
            )
            .gauge_style(Style::default().fg(Color::Green))
            .ratio(pct)
            .label(total_label);
        f.render_widget(total_gauge, rows[0]);

        // Current file
        let (cur_pct, cur_label) = match (&self.sync_current, self.sync_current_progress) {
            (Some(name), Some((done, total))) if total > 0 => (
                (done as f64 / total as f64).min(1.0),
                format!("{name}  {}/{}", human_bytes(done), human_bytes(total)),
            ),
            (Some(name), _) => (0.0, name.clone()),
            _ => (0.0, "(idle)".to_string()),
        };
        let cur = Gauge::default()
            .block(Block::default().borders(Borders::ALL).title(" current "))
            .gauge_style(Style::default().fg(Color::Cyan))
            .ratio(cur_pct)
            .label(cur_label);
        f.render_widget(cur, rows[1]);

        // Log (last N lines)
        let log_h = rows[2].height.saturating_sub(2) as usize;
        let take = self.sync_log.len().saturating_sub(log_h);
        let body: Vec<Line> = self.sync_log[take..]
            .iter()
            .map(|l| {
                let style = if l.contains("FAIL") {
                    Style::default().fg(Color::Red)
                } else if l.contains("SKIP") {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default().fg(Color::Green)
                };
                Line::styled(l.clone(), style)
            })
            .collect();
        f.render_widget(
            Paragraph::new(body)
                .block(Block::default().borders(Borders::ALL).title(" log ")),
            rows[2],
        );
    }

    // ------- Screen: Summary -------

    fn render_summary(&self, f: &mut Frame, area: Rect) {
        let (heading, heading_color) = if self.summary_cancelled {
            ("Sync cancelled — partial results below.", Color::Yellow)
        } else if self.dry_run {
            ("Dry run complete. No files were written.", Color::Green)
        } else {
            ("Sync complete.", Color::Green)
        };
        let body = vec![
            Line::raw(""),
            Line::from(Span::styled(
                heading,
                Style::default().fg(heading_color).bold(),
            )),
            Line::raw(""),
            Line::from(vec![
                Span::styled("Copied:  ", Style::default().fg(Color::Green)),
                Span::raw(self.summary_copied.to_string()),
            ]),
            Line::from(vec![
                Span::styled("Skipped: ", Style::default().fg(Color::Cyan)),
                Span::raw(self.summary_skipped.to_string()),
            ]),
            Line::from(vec![
                Span::styled("Failed:  ", Style::default().fg(Color::Red)),
                Span::raw(self.summary_failed.to_string()),
            ]),
        ];
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

/// Build the grouped tree view of files that would be copied: one section
/// per destination directory, with raw/non-raw/video counts and at most
/// 3 sample filenames per directory. Directories that don't exist on disk
/// are marked with a leading `+`.
fn build_review_tree(plan: &SyncPlan) -> Vec<ListItem<'static>> {
    // Group COPY items by parent directory.
    let mut groups: BTreeMap<PathBuf, Vec<&PlannedFile>> = BTreeMap::new();
    for it in &plan.items {
        if it.action != PlannedAction::Copy {
            continue;
        }
        let Some(dest) = it.dest_path.as_ref() else {
            continue;
        };
        let parent = dest
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("/"));
        groups.entry(parent).or_default().push(it);
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
        for f in files {
            match f.kind {
                MediaKindWire::RawImage => raw += 1,
                MediaKindWire::Image => img += 1,
                MediaKindWire::Video => vid += 1,
                MediaKindWire::Sidecar => other += 1,
            }
        }

        let is_new = !dir.exists();
        let marker = if is_new { "+ " } else { "  " };
        let marker_style = if is_new {
            Style::default().fg(Color::Green).bold()
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let dir_style = if is_new {
            Style::default().fg(Color::Green).bold()
        } else {
            Style::default().fg(Color::Cyan).bold()
        };

        // Header: "+ /path/to/dir/   RAW: 12  IMG: 8  VID: 1"
        let mut spans = vec![
            Span::styled(marker, marker_style),
            Span::styled(format!("{}/", dir.display()), dir_style),
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
                format!("SIDE: {other}"),
                Style::default().fg(Color::DarkGray),
            ));
        }
        out.push(ListItem::new(Line::from(spans)));

        // Sample filenames (top 3 by destination filename, alphabetical).
        let mut sorted = files.clone();
        sorted.sort_by(|a, b| {
            let an = a
                .dest_path
                .as_ref()
                .and_then(|p| p.file_name())
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            let bn = b
                .dest_path
                .as_ref()
                .and_then(|p| p.file_name())
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            an.cmp(&bn)
        });
        let total = sorted.len();
        let show = total.min(3);
        for f in &sorted[..show] {
            let name = f
                .dest_path
                .as_ref()
                .and_then(|p| p.file_name())
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "?".into());
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
        if total > show {
            out.push(ListItem::new(Line::from(vec![
                Span::raw("    └─ "),
                Span::styled(
                    format!("(... {} more)", total - show),
                    Style::default().fg(Color::DarkGray),
                ),
            ])));
        }
        // Blank spacer between groups.
        out.push(ListItem::new(Line::raw("")));
    }

    out
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
