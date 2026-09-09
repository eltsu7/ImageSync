//! `imagesync` CLI binary.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use imagesync_core::config::{AppConfig, RawMode};
use imagesync_core::events::{EngineEvent, FileOutcome, PlannedAction};
use imagesync_core::source::{FilesystemSource, MediaSource};
use imagesync_core::{Engine, EngineConfig, Operation, ProfileRegistry};
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
    Tui(TuiArgs),
    /// Scan a source and print the planned actions without copying anything.
    Scan(ScanArgs),
    /// Sync (copy) files from source to configured destinations.
    Sync(SyncArgs),
    /// List configured import catalogues.
    Catalogues,
    /// List loaded camera profiles.
    Profiles,
    /// List detected mountable sources (SD cards, removable drives).
    Sources,
    /// Print the serialized runtime config and exit.
    Config,
}

#[derive(Parser, Debug, Default)]
struct TuiArgs {
    /// Source path to preselect before opening the TUI.
    #[arg(long)]
    source: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct CommonOpts {
    /// Source path (mounted SD card / camera in MSC mode / any directory).
    #[arg(value_name = "SOURCE")]
    source: PathBuf,

    /// Saved catalogue to use for destination routing.
    #[arg(long)]
    catalogue: String,

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

    let cmd = cli
        .command
        .take()
        .unwrap_or_else(|| Command::Tui(TuiArgs::default()));
    match cmd {
        Command::Tui(args) => run_tui(cli.config.clone(), args).await,
        Command::Scan(args) => run_scan_or_sync(&cli, args.common, true, false).await,
        Command::Sync(args) => run_scan_or_sync(&cli, args.common, false, args.dry_run).await,
        Command::Catalogues => run_catalogues(&cli),
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
async fn run_tui(config_path: Option<PathBuf>, args: TuiArgs) -> Result<()> {
    imagesync_tui::run(imagesync_tui::RunOptions {
        config_path,
        source: args.source,
    })
    .await
}

#[cfg(not(feature = "tui"))]
async fn run_tui(_config_path: Option<PathBuf>, _args: TuiArgs) -> Result<()> {
    anyhow::bail!(
        "imagesync was built without the `tui` feature; use `imagesync scan` or `imagesync sync`"
    )
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
    let (app_cfg, _path) = load_app_config(cli)?;
    let app_cfg = apply_overrides(app_cfg, &opts);
    let engine_cfg = EngineConfig::try_from_catalogue(&app_cfg, &opts.catalogue)?;

    match imagesync_core::exiftool::probe_version() {
        Ok(v) => tracing::info!("exiftool version {v}"),
        Err(e) => {
            anyhow::bail!(
                "{e}\n\nInstall ExifTool:\n  Linux:   sudo apt install libimage-exiftool-perl\n  macOS:   brew install exiftool\n  Windows: winget install OliverBetz.ExifTool"
            );
        }
    }

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
    let scan_result = await_operation(engine.scan_and_plan(source.clone()), "scan").await?;
    let plan = scan_result.plan;

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

    // The cheap plan doesn't know destination folders yet (EXIF is read at
    // copy time). List the planned actions per file; exact destinations are
    // printed by the run below.
    println!();
    let max_show = 50usize;
    for item in plan.items.iter().take(max_show) {
        let action = match item.action {
            PlannedAction::Copy => "COPY",
            PlannedAction::SkipExists => "SKIP-EXISTS",
            PlannedAction::SkipFiltered => "SKIP-FILTER",
            PlannedAction::SkipNoDate => "SKIP-NODATE",
            PlannedAction::Error => "ERROR",
        };
        println!("  {action:<12} {}", item.source_rel_path);
    }
    if plan.items.len() > max_show {
        println!("  ... and {} more", plan.items.len() - max_show);
    }

    // `scan` and `--dry-run` both resolve exact destinations via a dry run
    // (exiftool over USB, writes nothing). A real `sync` resolves the date at
    // copy time on the local staged copy.
    let effective_dry = dry_run || scan_only;
    println!();
    println!(
        "=== {} ===",
        if effective_dry {
            "Dry run (no files written)"
        } else {
            "Executing"
        }
    );
    let summary = await_operation(
        engine.execute(
            plan,
            source,
            imagesync_core::engine::ExecuteOptions {
                dry_run: effective_dry,
            },
        ),
        "sync",
    )
    .await?;
    if summary.cancelled {
        anyhow::bail!("sync was cancelled");
    }
    Ok(())
}

async fn await_operation<T>(operation: Operation<T>, label: &str) -> Result<T>
where
    T: Send + 'static,
{
    let Operation {
        events,
        mut completion,
        cancellation,
    } = operation;
    let drain = tokio::spawn(drain_to_stderr(events));

    tokio::select! {
        biased;

        result = &mut completion => {
            drain.await.context("event consumer task panicked")?;
            result
                .map_err(|error| anyhow::anyhow!("{label} task: {error}"))?
                .with_context(|| format!("{label} failed"))
        }
        interrupt = tokio::signal::ctrl_c() => {
            interrupt.context("installing Ctrl-C handler")?;
            cancellation.cancel();
            let completion_result = completion.await;
            drain.await.context("event consumer task panicked")?;
            if let Err(error) = completion_result {
                anyhow::bail!("{label} task panicked while cancelling: {error}");
            }
            anyhow::bail!("{label} interrupted after cleanup");
        }
    }
}

async fn drain_to_stderr(mut events: ReceiverStream<EngineEvent>) {
    while let Some(ev) = events.next().await {
        match ev {
            EngineEvent::ScanStarted { display_name, .. } => {
                eprintln!("scanning: {display_name}");
            }
            EngineEvent::ScanProgress { files_seen, .. } if files_seen % 200 == 0 => {
                eprintln!("  scanned: {files_seen} files");
            }
            EngineEvent::ScanProgress { .. } => {}
            EngineEvent::ScanComplete { files, .. } => {
                eprintln!("  {files} files enumerated");
            }
            EngineEvent::ProfileDetected { profile } => {
                eprintln!("  profile: {} ({})", profile.display_name, profile.id);
            }
            EngineEvent::MetadataProgress { done, total } if done == total || done % 200 == 0 => {
                eprintln!("  metadata: {done}/{total}");
            }
            EngineEvent::MetadataProgress { .. } => {}
            EngineEvent::PlanReady { summary } => {
                eprintln!(
                    "  planned: {} copies, {} skips, {} errors",
                    summary.copies, summary.skips, summary.errors
                );
            }
            EngineEvent::ExecutionStarted {
                total_files,
                total_bytes,
                dry_run,
            } => {
                eprintln!(
                    "  {} {} files ({} bytes)",
                    if dry_run { "dry-running" } else { "executing" },
                    total_files,
                    total_bytes
                );
            }
            EngineEvent::FileStarted { file } => {
                eprintln!("  starting {}", file.source_rel_path);
            }
            EngineEvent::FilePhaseChanged { file, phase } => {
                eprintln!("  {}: {phase:?}", file.rel_path);
            }
            EngineEvent::FileProgress {
                file,
                bytes_done,
                bytes_total,
            } => {
                eprintln!("  copying {}: {bytes_done}/{bytes_total}", file.rel_path);
            }
            EngineEvent::FileFinished { file, outcome } => match outcome {
                FileOutcome::Copied { bytes } => {
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
                FileOutcome::Skipped { reason } => {
                    eprintln!("  skip {} ({reason})", file.source_rel_path);
                }
                FileOutcome::Failed { error } => {
                    eprintln!("  FAIL {} ({error})", file.source_rel_path);
                }
            },
            EngineEvent::ExecutionProgress { summary } => {
                eprintln!(
                    "  progress: {}/{} (copied={} skipped={} failed={})",
                    summary.completed(),
                    summary.total,
                    summary.copied,
                    summary.skipped,
                    summary.failed
                );
            }
            EngineEvent::ExecutionFinished { summary } => {
                eprintln!(
                    "\nsummary: copied={} skipped={} failed={}{}",
                    summary.copied,
                    summary.skipped,
                    summary.failed,
                    if summary.cancelled { " cancelled" } else { "" }
                );
            }
            EngineEvent::Warning {
                kind,
                message,
                file,
            } => {
                let file = file
                    .as_ref()
                    .map(|file| format!(" ({})", file.rel_path))
                    .unwrap_or_default();
                eprintln!("warn [{kind:?}]{file}: {message}");
            }
            EngineEvent::Error {
                kind,
                message,
                file,
            } => {
                let file = file
                    .as_ref()
                    .map(|file| format!(" ({})", file.rel_path))
                    .unwrap_or_default();
                eprintln!("error [{kind:?}]{file}: {message}");
            }
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

fn run_catalogues(cli: &Cli) -> Result<()> {
    let (cfg, _) = load_app_config(cli)?;
    if cfg.catalogues.is_empty() {
        println!("No catalogues configured.");
        return Ok(());
    }

    for (index, (name, catalogue)) in cfg.catalogues.iter().enumerate() {
        if index > 0 {
            println!();
        }
        println!("{name}");
        println!("  images root: {}", catalogue.images_root.display());
        println!("  images folders: {}", catalogue.images_template);
        println!("  videos root: {}", catalogue.videos_root.display());
        println!("  videos folders: {}", catalogue.videos_template);
    }
    Ok(())
}

fn run_config(cli: &Cli) -> Result<()> {
    let (cfg, path) = load_app_config(cli)?;
    println!("# config loaded from: {}", path.display());
    println!("{}", toml::to_string_pretty(&cfg).unwrap());
    Ok(())
}
