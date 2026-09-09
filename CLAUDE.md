# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`imagesync` imports photos/videos from a camera or SD card (mounted as Mass Storage / a plain directory) into a date-based folder structure derived from EXIF metadata. Rust workspace, three crates. v0.1 in early development.

## Commands

```bash
cargo build                              # build all crates
cargo run -p imagesync-cli               # launch the TUI (default subcommand)
cargo run -p imagesync-cli -- scan <SRC> # plan and print actions, copy nothing
cargo run -p imagesync-cli -- sync <SRC> --dry-run
cargo test                               # all tests (most live inline in core modules)
cargo test -p imagesync-core plan        # tests in one module, by name substring
cargo test build_plan -- --nocapture     # single test
cargo clippy --all-targets
RUST_LOG=imagesync_core=debug cargo run -p imagesync-cli -- scan <SRC>   # per-phase timing logs
```

**Runtime dependency:** ExifTool must be on `PATH` (`exiftool` binary). Without it, scans fail at metadata read.

The binary is named `imagesync` (defined in `imagesync-cli`, with a default `tui` feature that pulls in `imagesync-tui`). CLI flags on `scan`/`sync` (`--images-root`, `--raw-mode`, `--copy-workers`, `--verify`, etc.) override the on-disk config. Config lives at `<platform config dir>/imagesync/config.toml` (see `AppConfig::default_path`).

## Architecture

**Three crates, strict layering** — `imagesync-core` (engine, UI-agnostic) ← `imagesync-cli` (clap binary) and `imagesync-tui` (ratatui frontend). Frontends never reach into engine internals; they drive `Engine` and consume the `EngineEvent` stream.

**Event-driven engine (`core/src/engine.rs`).** Two entry points, both spawn a tokio task and return a `ReceiverStream<EngineEvent>`:
- `scan_and_plan(source) -> (JoinHandle<SyncPlan>, stream)`
- `execute(plan, source, opts) -> stream`

Critical contract: the caller **must consume the stream concurrently** with awaiting the join handle. The mpsc channel buffer (256) fills otherwise, and progress events arrive as one end-of-run flush instead of live.

**Scan→plan pipeline (`engine.rs::run_scan_and_plan`):**
1. Pre-flight: `check_dest_root` refuses to run if destination roots don't exist (guards the "drive not mounted → everything copied to empty mount point" footgun).
2. Detect `CameraProfile` for the source root.
3. `source.list_files()` enumerates candidate media.
4. **Pre-skip index** (`plan.rs`): recursively index destination roots by `(lowercase basename, size)` and drop already-imported files *before* reading their EXIF — avoids running exiftool on files we'd skip anyway.
5. Read metadata in batches via a single long-lived exiftool process (`exiftool.rs`, `-stay_open True -@ -`, ~60× faster than per-file spawns). Batch size from `performance.metadata_batch_size`.
6. `plan::build_plan` produces a `SyncPlan` of `PlannedFile`s with computed destination paths and skip/copy verdicts; merges in the pre-skipped items. Emits `PlanReady`.

**Execute pipeline** spawns up to `copy_workers` concurrent copy tasks (`copy.rs`), emitting `CopyStarted`/`CopyProgress`/`CopyOutcome`, ending in `SyncSummary`.

**Key modules in `imagesync-core`:**
- `events.rs` — `EngineEvent` enum (the frontend contract) plus `*Wire` mirror types. Engine-internal enums (`MediaKind`, `DateSource`) are re-encoded into serializable `MediaKindWire`/`DateSourceWire` for events — keep both sides in sync when adding variants.
- `plan.rs` — destination-path computation + dedupe. Sidecars (`.xmp`) are paired to their parent image by `(rel_dir, basename stem)` and inherit the image's destination dir and datetime; orphan sidecars dated standalone. Dedupe is case-insensitive via per-directory `DirCache` (`read_dir` once per dir) and the cross-library pre-skip index.
- `template.rs` — path token expansion (`{yyyy}`, `{mm}`, `{dd}`, `{month}`, `{HH}`, …) for `images_template`/`videos_template`.
- `profiles.rs` — `CameraProfile` registry. Built-ins (`profiles/builtin/*.toml`) embedded via `include_str!`; user profiles from disk override built-ins by `id`. Profiles are pure TOML — adding a camera needs no code changes. Detection matches on EXIF make/model glob and marker paths.
- `classify.rs` — extension → `MediaKind` (image/raw/video/sidecar).
- `metadata.rs` — picks best capture datetime from exiftool fields, with a fallback chain down to file mtime (`DateSource`).
- `source.rs` / `mount.rs` — `MediaSource` trait (`FilesystemSource` impl) and removable-drive detection.
- `config.rs` — `AppConfig` (on-disk TOML) → `EngineConfig` (runtime). `RawMode` = `all`/`raw_only`/`non_raw_only`.

**TUI (`imagesync-tui/src/lib.rs`)** is a single large file: ratatui + crossterm event loop, screens including a Sync screen with per-worker progress bars. It builds an `Engine` and renders the `EngineEvent` stream.

## Conventions

- Tests are inline `#[test]` / `#[tokio::test]` modules at the bottom of each core file (no separate `tests/` dir). `insta` and `tempfile` are available for snapshot/fixture tests.
- The engine is async (tokio) and emits events rather than printing — never add stdout/stderr output to `imagesync-core`; surface information through `EngineEvent` so all frontends benefit. Logging via `tracing` is fine.
