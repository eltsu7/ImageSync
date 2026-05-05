# ImageSync Architecture

> Reference doc for contributors and for our future selves. Kept in sync with
> the code by hand. If something here doesn't match the source, the source wins
> — please update this doc in the same commit.

## 1. What it is

ImageSync is a cross-platform tool that copies photos and videos from a
camera or memory card into a date-based folder structure derived from
EXIF metadata. It deduplicates against the destination (case-insensitive,
size-matched), runs as either a TUI or a CLI, and never overwrites files.

Goals, in priority order:

1. **Don't lose data.** Atomic writes; never overwrite; case-insensitive
   dedupe so re-imports are idempotent.
2. **Be fast on a single SD card.** A few hundred RAWs should land in
   seconds, not minutes.
3. **Be readable.** The engine has no UI deps; a frontend is a thin
   driver around an event stream.
4. **Be cross-platform.** Linux first, but macOS and Windows builds
   work without per-OS branches in the engine.

## 2. Workspace layout

```
crates/
  imagesync-core/      Engine. No UI deps. Pure business logic.
  imagesync-tui/       ratatui frontend.
  imagesync-cli/       Binary `imagesync`. tui is a default feature.
docs/
  ARCHITECTURE.md      You are here.
  TODO.md              Backlog.
  CAMERA_PROFILES.md   Profile authoring guide.
```

Why a workspace: keep `imagesync-core` UI-agnostic and importable as a
library. The TUI is one consumer among potentially several (CLI, future
GUI, headless service). The engine never knows what's rendering it.

## 3. Engine boundary

### 3.1 Event-stream contract

Both `Engine::scan_and_plan` and `Engine::execute` are **non-async
constructors** that:

1. Spawn the work onto `tokio::spawn`.
2. Return *immediately* with a `(JoinHandle<Result<T>>, ReceiverStream<EngineEvent>)`.

Frontends drain the event stream live and then `await` the join handle
for the final result.

```rust
let (plan_handle, mut events) = engine.scan_and_plan(source.clone());
while let Some(ev) = events.next().await {
    render(ev);
}
let plan = plan_handle.await??;
```

> **Don't** make these methods `async` and `await` the work internally
> before returning the stream. We did that originally — events buffered
> in the bounded channel and arrived all at once at the end of the scan,
> making progress reporting useless. Fixed in `275384a`.

### 3.2 Event types (`events.rs`)

| Event | Phase | Purpose |
|---|---|---|
| `ScanStarted { display_name, … }` | scan | UI banner |
| `ScanProgress { files_scanned }` | scan | n-files-found counter |
| `MetadataProgress { done, total }` | scan | exiftool batch progress |
| `PlanReady { copies, skips, errors }` | scan | terminal scan event |
| `CopyStarted { file }` | execute | per-file gauge init |
| `CopyProgress { rel_path, bytes_done, bytes_total }` | execute | per-file gauge |
| `CopyComplete { file, outcome }` | execute | per-file result + log line |
| `SyncSummary { copied, skipped, failed }` | execute | terminal execute event |
| `Warning { message, … }` | any | non-fatal |
| `Error { message, … }` | any | per-item error |

The bounded channel is 256 events. Progress events are **throttled at
the producer side** (per-file copy progress at every ~4 MiB; per-batch
metadata) so the channel never fills under normal load.

> **Sync abort caveat.** `JoinHandle::abort()` kills the engine task
> before it can send `SyncSummary`. Frontends must seed their final
> totals from the live counters they've been maintaining, otherwise
> the summary will read 0/0/0. The TUI does this in `on_key_sync`
> (commit `a99aec9`).

## 4. Pipeline

### 4.1 Scan & plan

```
detect mounts ──► profile match ──► walk filesystem ──► classify by ext
                                              │
                                              ▼
                                  build DestIndex (recursive)
                                              │
                                              ▼
                          pre-skip files where (basename, size)
                          already exists anywhere in the library
                                              │
                                              ▼
                                  exiftool batch (50 files)
                                              │
                                              ▼
                                   resolve datetime per file
                                              │
                                              ▼
                                  apply path template ──► PlannedFile
                                              │
                                              ▼
                                   DirCache lookup ─► COPY / SKIP / ERROR
```

Steps:

1. **Mount detection** (`mount.rs`). Linux: `/run/media/$USER/*`,
   `/media/$USER/*`, `/media/*`, `/mnt/*`. macOS: `/Volumes/*` minus
   boot. Windows: `sysinfo::Disks` minus `C:`. Presence of `DCIM/`
   bumps to `MountRank::CameraCard`.
2. **Profile match** (`profiles.rs`). Each TOML profile has a `[match]`
   block (vendor IDs, marker files, etc.). Falls back to `dcim-generic`.
3. **Walk** (`source.rs::FilesystemSource`). Recursive walk under
   `DCIM/`. Yields `SourceFile { rel_path, size }`.
4. **Classify** (`classify.rs`). Extension lookup in the matched profile
   classifies each file as `Image { raw }`, `Video`, `Sidecar`, or
   ignored. RAW filter (`RawMode::All | RawOnly | NonRawOnly`) and
   include flags (`include_videos`, `include_sidecars`) gate inclusion.
5. **Pre-skip index** (`plan.rs::DestIndex`). Recursive walk of
   `images_root` and `videos_root` builds
   `HashMap<(lowercase basename, size), PathBuf>`. Any non-sidecar
   source file whose `(basename, size)` is in the index is emitted
   directly as `SkipExists` with `datetime: None` and the matched
   on-disk path as `dest_path`. This avoids running exiftool on files
   we'd skip anyway — the dominant cost on partially-imported cards
   (~4× scan speedup measured on a 1232-file Sony α7 IV card with
   1010 already imported). Sidecars are intentionally excluded so they
   can still inherit their parent's datetime in the normal path.
6. **Metadata batch** (`metadata.rs` + `exiftool.rs`). ExifTool is
   spawned once with `-stay_open True -@ -`. Only files that survived
   the pre-skip filter are batched (default 50) and submitted via
   `-execute<n>` / `{ready<n>}` markers. Significantly faster than
   spawning exiftool per file (~60×).
7. **Datetime resolution** (`metadata.rs`). For each file the resolver
   tries `DateTimeOriginal`, `CreateDate`, file mtime in that order.
   Sidecars inherit their parent's datetime.
8. **Path template** (`template.rs`). Tokens: `yyyy yy mm dd month
   Month HH MM SS`. Parsed to an AST at config-load and re-applied per
   file.
9. **Plan build** (`plan.rs`). Compute the destination path. Look up
   the parent dir in a `DirCache` (one `read_dir` per dir, lowercased
   basenames hashmap). Decide:
   - exact basename hit, same size → `SkipExists`
   - exact basename hit, different size → `Error` ("size mismatch")
   - case-different basename hit, same size → `SkipExists`
   - no hit → `Copy`

   Pre-skipped items (step 5) are appended to the final
   `SyncPlan.items` after `build_plan` returns.

### 4.2 Execute

```
┌── plan items (sequence) ────────────────────────────────────────────┐
│                                                                     │
│   for each item:                                                    │
│     match action:                                                   │
│       Copy        → semaphore.acquire()                             │
│                     spawn(do_copy)  ── progress events ──► UI       │
│       SkipExists  → emit CopyComplete{Skipped}                      │
│       Error       → emit CopyComplete{Failed}                       │
│                                                                     │
│   join all spawned tasks                                            │
│   emit SyncSummary                                                  │
└─────────────────────────────────────────────────────────────────────┘
```

`copy_workers` (default **1**) gates concurrency via a `Semaphore`.
Default 1 because:

- A single SD card's USB read speed is the bottleneck; concurrent
  reads usually slow things down on consumer cards.
- The TUI's "current file" gauge flickered between in-flight files
  with multiple workers (commit `c194af0`).
- Matches RapidPhotoDownloader's one-worker-per-device model.

Power users with NVMe destinations can override via
`[performance].copy_workers`.

### 4.3 Per-file copy (`copy.rs`)

Hot path:

```rust
tokio::task::spawn_blocking(|| {
    let mut reader = std::fs::File::open(src)?;
    let mut writer = std::fs::OpenOptions::new()
        .create_new(true).write(true).open(&tmp)?;
    let mut buf = vec![0u8; 1 MiB];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 { break; }
        writer.write_all(&buf[..n])?;
        if total - last_progress >= 4 MiB {
            progress(total, bytes_total);  // throttled
        }
    }
    writer.flush()?;  // no fsync
    std::fs::rename(&tmp, &dest)?;
}).await
```

Key decisions:

- **One `spawn_blocking` for the whole file.** Tokio's `fs::File` does
  spawn_blocking on every `read`/`write`, which crossed the threadpool
  twice per chunk and dominated CPU/futex overhead.
  See <https://docs.rs/tokio/latest/tokio/fs/>:
  > To get good performance with file IO on Tokio, it is recommended
  > to batch your operations into as few `spawn_blocking` calls as
  > possible.
- **No per-file `fsync`.** Atomic visibility comes from temp+rename.
  A power loss between `rename` and writeback flush could lose a few
  recently-renamed files; the user re-runs and the case-insensitive
  dedupe picks up where it left off.
- **Progress throttling at 4 MiB.** RPD's `copyfiles.py` uses 5 MiB.
  Reduces UI event volume by ~10× on RAW files.

Atomic temp name: `<dest>.imagesync-tmp-<pid>-<nanos>`. Same-filesystem
rename is the atomicity primitive. Cross-filesystem copies fall back
to a regular write+rename which still leaves no half-named files
visible at `dest`.

## 5. Configuration

### 5.1 Files

- App config: `$XDG_CONFIG_HOME/imagesync/config.toml`
  (Linux), `~/Library/Application Support/imagesync/config.toml` (macOS),
  `%APPDATA%/imagesync/config.toml` (Windows).
- Camera profiles: bundled in the binary via `include_str!` from
  `crates/imagesync-core/src/profiles/*.toml`.

### 5.2 Schema (`config.rs`)

```toml
[paths]
images_root = "/media/Pictures/"
videos_root = "/media/Pictures/"
images_template = "{yyyy}/{yyyy}-{mm}-{dd}"
videos_template = "{yyyy}/{yyyy}-{mm}-{dd}"

[filters]
raw_mode = "raw_only"        # all | raw_only | non_raw_only
include_videos = true
include_sidecars = true

[performance]
scan_workers = 0             # 0 = auto (num_cpus)
metadata_batch_size = 50
copy_workers = 1

[verify]
enabled = false
algorithm = "xxh3"

[profiles]
default = "auto"             # auto | <profile-id>

[ui]
remember_last_sources = true
```

### 5.3 Persistence gotcha

`AppConfig::save` serializes the entire struct. When the TUI hits
`SaveConfigPrompt`, every default-valued field gets written explicitly
to disk. Later default changes in code don't propagate to existing
users — their config keeps pinning the old value.

Already burned us twice:
- `metadata_batch_size`: 200 → 50 in code, but saved configs still had 200.
- `copy_workers`: 2 → 1 in code, saved configs still had 2.

Fix on the backlog: `#[serde(skip_serializing_if = "is_default")]` on
`PerformanceConfig` fields.

## 6. TUI

Single `App` struct in `crates/imagesync-tui/src/lib.rs`. Screens:

```
SourceSelect ──► [Destination] ──► [SaveConfigPrompt] ──► Confirm ──► Scan
       ▲              │                                       │         │
       │              └─ Esc ─────────────────────────────────┘         │
       │                                                                ▼
       └────────────── n new run ────── Summary ◄── Sync ◄─── Review ◄──┘
```

The Destination and SaveConfigPrompt screens are skipped when the
existing config already has both roots set and no edits are needed.

### 6.1 Render loop

```rust
loop {
    terminal.draw(|f| self.render(f))?;
    self.pump_engine_events();          // drain mpsc → state
    if event::poll(33ms)? {
        self.on_key(event::read()?.into());
    }
    self.poll_handles().await;          // check JoinHandles
    if self.should_quit { break; }
}
```

`event::poll` blocks the calling OS thread, but with multi-thread
tokio runtime other tasks (engine workers) keep progressing on other
workers.

### 6.2 Engine integration

Engine output is forwarded into a TUI-owned
`mpsc::unbounded_channel<EngineEvent>` by a small forwarding task:

```rust
self.scan_handle = Some(tokio::spawn(async move {
    let engine = Engine::new(engine_cfg, registry);
    let (plan_handle, mut events) = engine.scan_and_plan(source.clone());
    while let Some(ev) = events.next().await {
        let _ = tx.send(ev);            // unbounded → never blocks
    }
    match plan_handle.await {
        Ok(Ok(plan)) => Ok((plan, source)),
        Ok(Err(e)) => Err(format!("{e}")),
        Err(e) => Err(format!("scan task: {e}")),
    }
}));
```

`poll_handles().await` polls `JoinHandle::is_finished()` each tick;
when set, it `await`s the handle to get the final value and transitions
screens.

### 6.3 Review-screen tree (commit `d2df6a2`)

`build_review_tree(plan)` builds a `Vec<ListItem<'static>>`:

```
+ /media/Pictures/2026/2026-05-03/   RAW: 28
    ├─ RAW  DSC04563.ARW
    ├─ RAW  DSC04564.ARW
    ├─ RAW  DSC04565.ARW
    └─ (... 25 more)

  /media/Pictures/2026/2026-05-02/   IMG: 12
    ...
```

Rules:

- Group only `PlannedAction::Copy` items by `dest_path.parent()`.
- New folders (the `dir.exists()` check fails) get a green `+`.
- Per-folder counts: RAW / IMG / VID / SIDE, only shown if > 0.
- Up to 3 sample filenames per folder, sorted alphabetically by
  destination basename. Excess summarized as `(... N more)`.

The same helper is called from `on_key_review` for cursor clamping, so
the up/down keys can never select past the last visible row.

### 6.4 Keybinds

| Key | Screen | Action |
|---|---|---|
| `j` `k` `↓` `↑` | any list | move cursor |
| `Enter` | most | confirm |
| `Esc` | most | back / cancel |
| `q` | most | quit |
| `m` | SourceSelect | enter manual path |
| `r` | SourceSelect | rescan mounts |
| `r` | Confirm | cycle raw_mode |
| `e` | Confirm | edit destinations |
| `s` | Review | start sync |
| `d` | Review | start dry-run |
| `n` | Summary | start over |

## 7. Camera profiles

See `docs/CAMERA_PROFILES.md` for authoring. Schema lives in
`crates/imagesync-core/src/profiles.rs`.

Two profiles ship:

- `sony-a7m4` — explicit match for Sony α7 IV.
- `dcim-generic` — fallback for any DCIM-shaped device.

> **TOML gotcha.** Top-level fields (`image_extensions`,
> `raw_extensions`, etc.) MUST come BEFORE any `[match]` section.
> Otherwise serde parses them as fields *of* `[match]` and silently
> drops them. Bit us during M1.

## 8. Testing

- `cargo test` — 34 unit tests across 5 suites.
- ExifTool 13.50+ must be on `PATH` for the binary; tests that need it
  are gated with `#[ignore]` so `cargo test` works in CI without it.
- TUI smoke tests use Python's `pty.fork()` to drive a real terminal.
  Caveat: ANSI-stripped regex matching can give false negatives on
  live-progress checks. Cross-check with `RUST_LOG=imagesync_tui=debug`
  tracing or with the CLI (timestamped progress to stderr).

## 9. Performance landmarks

Numbers measured on `/run/media/eeli/CC49-31C9/` (Sony a7 IV exFAT
card, 1232 files, ~30 GB), copying to `/media/Pictures/` (ext4):

| Stage | Old | New | Notes |
|---|---|---|---|
| Scan + metadata, fresh card (1232 files, 0 already imported) | ~21 s | ~21 s | Dominated by exiftool I/O. |
| Scan + metadata, partial card (1232 files, 1010 already imported) | ~21 s | ~5 s (expected) | Pre-skip index removes already-imported files from the exiftool batch. See 4.1 step 5. |
| Copy 222 files | TODO measure | TODO measure | Was tokio::fs spawn-blocking-per-chunk; now one spawn_blocking per file. |

Bottleneck inventory:

- Scan, fresh card: file walk and exiftool throughput. Already batched;
  further speedup needs concurrent exiftool processes or an embedded
  EXIF parser. See TODO.
- Scan, partial card: handled by the `DestIndex` pre-skip pass.
- Plan: `read_dir` per destination directory. Cached in `DirCache`.
- Copy: SD-card USB read speed (~30–100 MB/s for class-10).

## 10. Cross-platform notes

- Path display: always use `Path::display()`, never assume UTF-8.
- Line endings: we never read text from the card; ExifTool JSON output
  is read as bytes and parsed by `serde_json`.
- File locking on Windows: untested. The temp+rename pattern is the
  same across platforms but a destination still being scanned by
  Windows Defender may briefly fail the rename. Not currently handled.
- `tokio::fs::rename` is atomic on POSIX same-filesystem and on NTFS;
  cross-volume copies aren't atomic, but since we always write the temp
  next to the destination this isn't an issue in practice.

## 11. Out of scope (for v0.1)

- PTP/MTP cameras via libgphoto2. The `MediaSource` trait is shaped so
  a `PtpSource` impl drops in without engine changes; deferred until
  someone needs it.
- Thumbnail generation. RPD does this; we don't need to.
- Backup-to-second-destination. Pipe `imagesync` twice or use rclone
  for this.
- Renaming files. We preserve the camera's filename. Renaming opens a
  collision-handling can of worms we don't want to deal with yet.

## 12. Where to look first

When debugging:

| Symptom | Look at |
|---|---|
| Wrong destination dir | `template.rs`, `metadata.rs::resolve_datetime` |
| Wrong duplicate decision | `plan.rs::DirCache`, the lowercase-basename matcher |
| Slow scan | `metadata.rs` batch size, `exiftool.rs` stay-open lifecycle |
| Slow copy | `copy.rs` — should be one `spawn_blocking` per file |
| TUI shows 0 events live | `engine.rs` — make sure scan_and_plan returns immediately |
| TUI shows 0 after cancel | `lib.rs::on_key_sync` — seed summary from live counters |
| Config field ignored | TOML field order; `#[serde(rename_all = "snake_case")]` |
| New camera not detected | `profiles/<id>.toml` — check `[match]` block order |
