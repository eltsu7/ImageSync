//! `imagesync` CLI binary.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use imagesync_core::config::{AppConfig, RawMode};
use imagesync_core::events::{CopyOutcome, EngineEvent, PlannedAction};
use imagesync_core::source::{FilesystemSource, MediaSource};
use imagesync_core::{Engine, EngineConfig, ProfileRegistry};
use tokio_stream::wrappers::ReceiverStream;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "imagesync",
    about = "Import photos and videos from a camera or SD card into a date-based folder structure.",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Use a specific config file path (default: platform config dir).
    #[arg(long, global = true)]
    config: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Launch the interactive TUI (default when no subcommand given).
    Tui,
    /// Scan a source and print the planned actions without copying anything.
    Scan(ScanArgs),
    /// Sync (copy) files from source to configured destinations.
    Sync(SyncArgs),
    /// List loaded camera profiles.
    Profiles,
    /// List detected mountable sources (SD cards, removable drives).
    Sources,
    /// Print the resolved config (after applying any flags) and exit.
    Config,
}

#[derive(Parser, Debug)]
struct CommonOpts {
    /// Source path (mounted SD card / camera in MSC mode / any directory).
    #[arg(value_name = "SOURCE")]
    source: PathBuf,

    /// Destination root for images. Overrides config.
    #[arg(long)]
    images_root: Option<PathBuf>,

    /// Destination root for videos. Overrides config.
    #[arg(long)]
    videos_root: Option<PathBuf>,

    /// Path template for images (e.g. `{yyyy}/{yyyy}-{mm}-{dd}`).
    #[arg(long)]
    images_template: Option<String>,

    /// Path template for videos (e.g. `{yyyy}/{mm}/{dd}`).
    #[arg(long)]
    videos_template: Option<String>,

    /// Filter: `all` | `raw_only` | `non_raw_only`.
    #[arg(long)]
    raw_mode: Option<RawModeArg>,

    /// Skip videos.
    #[arg(long)]
    no_videos: bool,

    /// Skip sidecar files.
    #[arg(long)]
    no_sidecars: bool,

    /// Number of parallel copy workers.
    #[arg(long)]
    copy_workers: Option<usize>,

    /// Verify each copy with xxh3.
    #[arg(long)]
    verify: bool,
}

#[derive(Parser, Debug)]
struct ScanArgs {
    #[command(flatten)]
    common: CommonOpts,
}

#[derive(Parser, Debug)]
struct SyncArgs {
    #[command(flatten)]
    common: CommonOpts,

    /// Plan and report what would be copied without writing anything.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum RawModeArg {
    All,
    RawOnly,
    NonRawOnly,
}

impl From<RawModeArg> for RawMode {
    fn from(v: RawModeArg) -> Self {
        match v {
            RawModeArg::All => RawMode::All,
            RawModeArg::RawOnly => RawMode::RawOnly,
            RawModeArg::NonRawOnly => RawMode::NonRawOnly,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let mut cli = cli;

    let cmd = cli.command.take().unwrap_or(Command::Tui);
    match cmd {
        Command::Tui => run_tui().await,
        Command::Scan(args) => run_scan_or_sync(&cli, args.common, true, false).await,
        Command::Sync(args) => {
            run_scan_or_sync(&cli, args.common, false, args.dry_run).await
        }
        Command::Profiles => run_profiles(),
        Command::Sources => run_sources(),
        Command::Config => run_config(&cli),
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,imagesync_core=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .compact()
        .try_init()
        .ok();
}

#[cfg(feature = "tui")]
async fn run_tui() -> Result<()> {
    imagesync_tui::run().await
}

#[cfg(not(feature = "tui"))]
async fn run_tui() -> Result<()> {
    anyhow::bail!("imagesync was built without the `tui` feature; use `imagesync scan` or `imagesync sync`")
}

fn load_app_config(cli: &Cli) -> Result<(AppConfig, PathBuf)> {
    let path = match &cli.config {
        Some(p) => p.clone(),
        None => AppConfig::default_path()
            .context("could not determine default config path on this platform")?,
    };
    let cfg = AppConfig::load_or_default(&path)
        .with_context(|| format!("loading config from {}", path.display()))?;
    Ok((cfg, path))
}

fn apply_overrides(mut cfg: AppConfig, opts: &CommonOpts) -> AppConfig {
    if let Some(p) = &opts.images_root {
        cfg.paths.images_root = Some(p.clone());
    }
    if let Some(p) = &opts.videos_root {
        cfg.paths.videos_root = Some(p.clone());
    }
    if let Some(t) = &opts.images_template {
        cfg.paths.images_template = t.clone();
    }
    if let Some(t) = &opts.videos_template {
        cfg.paths.videos_template = t.clone();
    }
    if let Some(m) = &opts.raw_mode {
        cfg.filters.raw_mode = m.clone().into();
    }
    if opts.no_videos {
        cfg.filters.include_videos = false;
    }
    if opts.no_sidecars {
        cfg.filters.include_sidecars = false;
    }
    if let Some(w) = opts.copy_workers {
        cfg.performance.copy_workers = w;
    }
    if opts.verify {
        cfg.verify.enabled = true;
    }
    cfg
}

async fn run_scan_or_sync(
    cli: &Cli,
    opts: CommonOpts,
    scan_only: bool,
    dry_run: bool,
) -> Result<()> {
    // Probe exiftool early.
    match imagesync_core::exiftool::probe_version() {
        Ok(v) => tracing::info!("exiftool version {v}"),
        Err(e) => {
            anyhow::bail!(
                "{e}\n\nInstall ExifTool:\n  Linux:   sudo apt install libimage-exiftool-perl\n  macOS:   brew install exiftool\n  Windows: winget install OliverBetz.ExifTool"
            );
        }
    }

    let (app_cfg, _path) = load_app_config(cli)?;
    let app_cfg = apply_overrides(app_cfg, &opts);

    let engine_cfg = EngineConfig::try_from_app(&app_cfg)
        .context("incomplete configuration (set images_root and videos_root via flags or config file)")?;

    let registry = ProfileRegistry::with_builtins()?;

    let source: Arc<dyn MediaSource> = FilesystemSource::new(
        opts.source.to_string_lossy().to_string(),
        opts.source
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| opts.source.to_string_lossy().to_string()),
        opts.source.clone(),
    )
    .into_arc();

    let engine = Engine::new(engine_cfg, registry);
    let (plan, events) = engine.scan_and_plan(source.clone()).await?;

    drain_to_stderr(events).await;

    println!();
    println!("=== Plan ===");
    println!(
        "  copies: {}   skips: {}   errors: {}",
        plan.copies(),
        plan.skips(),
        plan.errors()
    );
    println!();

    let mut by_action: std::collections::BTreeMap<&'static str, usize> =
        std::collections::BTreeMap::new();
    for item in &plan.items {
        let key = match item.action {
            PlannedAction::Copy => "copy",
            PlannedAction::SkipExists => "skip(exists)",
            PlannedAction::SkipFiltered => "skip(filtered)",
            PlannedAction::SkipNoDate => "skip(no-date)",
            PlannedAction::Error => "error",
        };
        *by_action.entry(key).or_default() += 1;
    }
    for (k, v) in by_action {
        println!("  {k:<16} {v}");
    }

    println!();
    let max_show = 50usize;
    for item in plan.items.iter().take(max_show) {
        let dest = item
            .dest_path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "-".to_string());
        let date = item
            .datetime
            .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "-".to_string());
        let action = match item.action {
            PlannedAction::Copy => "COPY",
            PlannedAction::SkipExists => "SKIP-EXISTS",
            PlannedAction::SkipFiltered => "SKIP-FILTER",
            PlannedAction::SkipNoDate => "SKIP-NODATE",
            PlannedAction::Error => "ERROR",
        };
        println!(
            "  {action:<12} {} -> {} [{}]",
            item.source_rel_path, dest, date
        );
    }
    if plan.items.len() > max_show {
        println!("  ... and {} more", plan.items.len() - max_show);
    }

    if scan_only {
        return Ok(());
    }

    println!();
    println!(
        "=== Executing{} ===",
        if dry_run { " (dry run)" } else { "" }
    );
    let exec = engine.execute(
        plan,
        source,
        imagesync_core::engine::ExecuteOptions { dry_run },
    );
    drain_to_stderr(exec).await;

    Ok(())
}

async fn drain_to_stderr(events: ReceiverStream<EngineEvent>) {
    let mut events = events;
    while let Some(ev) = events.next().await {
        match ev {
            EngineEvent::ScanStarted { display_name, .. } => {
                eprintln!("scanning: {display_name}");
            }
            EngineEvent::ScanComplete { files, .. } => {
                eprintln!("  {files} files enumerated");
            }
            EngineEvent::MetadataProgress { done, total } if done == total || done % 200 == 0 => {
                eprintln!("  metadata: {done}/{total}");
            }
            EngineEvent::PlanReady {
                copies,
                skips,
                errors,
            } => {
                eprintln!("  planned: {copies} copies, {skips} skips, {errors} errors");
            }
            EngineEvent::CopyComplete { file, outcome } => match outcome {
                CopyOutcome::Copied { bytes } => {
                    eprintln!(
                        "  copied {} ({} bytes) -> {}",
                        file.source_rel_path,
                        bytes,
                        file.dest_path
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_default()
                    );
                }
                CopyOutcome::Skipped { reason } => {
                    eprintln!("  skip {} ({reason})", file.source_rel_path);
                }
                CopyOutcome::Failed { error } => {
                    eprintln!("  FAIL {} ({error})", file.source_rel_path);
                }
            },
            EngineEvent::SyncSummary {
                copied,
                skipped,
                failed,
            } => {
                eprintln!("\nsummary: copied={copied} skipped={skipped} failed={failed}");
            }
            EngineEvent::Warning { message, .. } => {
                eprintln!("warn: {message}");
            }
            EngineEvent::Error { message, .. } => {
                eprintln!("error: {message}");
            }
            _ => {}
        }
    }
}

fn run_profiles() -> Result<()> {
    let r = ProfileRegistry::with_builtins()?;
    println!("Built-in profiles:");
    for p in r.iter() {
        println!(
            "  {:<20} {} (raw: {}, image: {}, video: {})",
            p.id,
            p.display_name,
            p.raw_extensions.join(","),
            p.image_extensions.join(","),
            p.video_extensions.join(",")
        );
    }
    Ok(())
}

fn run_sources() -> Result<()> {
    let mounts = imagesync_core::mount::detect_mounts();
    if mounts.is_empty() {
        println!("No mounted sources detected.");
        return Ok(());
    }
    println!("Detected sources:");
    for m in mounts {
        let tag = match m.rank {
            imagesync_core::mount::MountRank::CameraCard => "[CAMERA]",
            imagesync_core::mount::MountRank::Removable => "[remov.]",
            imagesync_core::mount::MountRank::Other => "[other ]",
        };
        println!("  {tag} {:<24} {}", m.label, m.path.display());
    }
    Ok(())
}

fn run_config(cli: &Cli) -> Result<()> {
    let (cfg, path) = load_app_config(cli)?;
    println!("# config loaded from: {}", path.display());
    println!("{}", toml::to_string_pretty(&cfg).unwrap());
    Ok(())
}
