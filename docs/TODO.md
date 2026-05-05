# ImageSync TODO

> Backlog. Roughly ordered by impact within each section. When picking
> something up, move it under "In progress" and link the commit when done.
> Closed items archive at the bottom with a one-line summary.

## Performance

- [ ] **Speed up metadata / image discovery for fresh cards.** A
      partially-imported card is now fast (pre-skip index — see Recently
      closed). The fresh-card case (everything must be exiftool'd) is
      still ~21 s on 1232 Sony a7 IV files. Options, in roughly
      increasing complexity:
  - Run multiple exiftool stay-open processes in parallel (one per
    `scan_workers`). Each handles a slice of the file list.
  - Use an embedded EXIF parser (`kamadak-exif`, `little_exif`, or
    `quickexif`) for the common case (JPEG/ARW/HEIC) and only fall
    back to exiftool for unknown formats. EXIF reading without an
    external process should be ~5–10× faster.
  - Pipeline scan and metadata: don't wait for the full file walk to
    complete before starting the first metadata batch. Stream files
    into batches as the walker yields them.
  - Cache file→datetime per `(rel_path, size, mtime)` keyed entry in a
    sled or sqlite cache so repeated scans of the same card are near-free.
    See RPD's `ThumbnailCacheSql` / `DownloadedSQL` for prior art.
  - Re-measure with `--release` first; debug builds are misleading.

- [ ] **Measure copy throughput vs `cp(1)`.** After the
      `spawn_blocking + std::fs` rewrite (`58d14d8`) we never benchmarked.
      Run something like
      `hyperfine --warmup 1 'cp -r DCIM/100MSDCF /tmp/a' 'imagesync sync …'`
      and record the gap in `docs/ARCHITECTURE.md` §9.

- [ ] **`#[serde(skip_serializing_if = "is_default")]` on
      `PerformanceConfig`.** Saved configs currently pin every field at
      its serialization-time default, so changing a Rust default doesn't
      reach existing users. Burned us with `metadata_batch_size`
      (200→50) and `copy_workers` (2→1). See ARCHITECTURE.md §5.3.

## TUI

- [ ] **Change raw-mode after scan and update the preview.** Right now
      raw-mode is locked at scan time — to change it you Esc back to
      Confirm, cycle `r`, and rescan. The plan already contains every
      file with its `MediaKindWire`, so we can re-filter and re-render
      the review tree client-side without touching the engine.
      Keybind on Review screen: `r` to cycle raw_mode; recompute
      `build_review_tree` from the existing `SyncPlan` filtered through
      the new mode. The plan items themselves don't change — the filter
      just hides RAW or non-RAW items from the visible tree and updates
      the counts. Question to resolve: should `s` (sync) then copy only
      the visible files, or the full plan? Probably visible.

- [ ] **Filter the review list.** Toggle to show only COPY (default),
      only SKIPS, only ERRORS. Useful when triaging "why didn't this
      one come over?" Skip/filter info already lives on `PlannedFile`.

- [ ] **ETA on the sync gauge.** We have `bytes_done / bytes_total`
      across all in-flight files; an exponential moving average over
      the last few seconds gives a usable ETA.

- [ ] **Free space at destination.** Show free bytes on the volume
      under each `images_root` / `videos_root` on Confirm and Sync
      screens. `sysinfo` already a transitive dep.

- [ ] **Last-used sources persistence.** `UiConfig.remember_last_sources`
      exists in the schema but is never read or written. Should auto-
      select the most-recently-used mount when the same volume is
      detected next time.

- [ ] **Show per-file warnings inline.** When `metadata` falls back to
      mtime, the user can't see it. Surface the `date_source` next to
      the file in the review tree, or at least add a tally to the
      header.

## Engine

- [ ] **PTP / MTP source.** `MediaSource` trait is already shaped for
      this; want a `PtpSource` impl using libgphoto2 behind a
      `--features ptp` flag. Out of v0.1 scope but design it now so
      the engine API doesn't need to change.

- [ ] **Verify-on-copy default.** Currently `[verify].enabled = false`.
      Reconsider for v0.2 once we measure the xxh3 cost (probably
      negligible compared to the read).

- [ ] **Cross-filesystem copy fallback.** Right now the temp file is
      written next to the destination. If the temp filename is too
      long (Windows 260-char limit on some FS) or the dest dir is
      read-only at the moment, we error out instead of falling back.

- [ ] **Sidecar grouping.** XMP and JPEG-paired-with-RAW should share
      a destination decision (if you skip the RAW, also skip the JPEG;
      if you copy the RAW, copy the XMP into the same target dir).
      Currently they're decided independently.

## CLI

- [ ] **Paginated / filterable plan output.** Hard-cap at 50 items in
      `crates/imagesync-cli/src/main.rs:269`. Add `--limit`, `--filter
      copy|skip|error`, and a paged mode for big plans.

- [ ] **`imagesync doctor` subcommand.** Print exiftool version, detect
      mounts, show config path and resolved values, dump effective
      profile. One-stop shop for "why isn't this working".

- [ ] **`--watch` mode.** Re-scan a mount as new files appear. Useful
      for tethered shooting where the camera writes during a session.

## Profiles

- [ ] **Bring more profiles in.** Currently only `sony-a7m4` and
      `dcim-generic`. Tracked in `docs/CAMERA_PROFILES.md`. Each new
      profile needs a real-card test; don't ship untested matchers.

- [ ] **Profile auto-detection from the device itself.** Read
      `MAKERNOTES`/`Make`/`Model` from the first ARW we find and
      pick a profile by EXIF identity, not just folder layout.

## Release / packaging

- [ ] **`cargo-dist` setup.** Cross-compile release binaries for
      Linux x86_64, macOS arm64+x86_64, Windows x86_64. Sign macOS.

- [ ] **GitHub Actions CI.** `cargo test`, `cargo clippy --all-targets
      -- -D warnings`, `cargo fmt --check`. Test matrix: Linux + macOS
      + Windows. Pin a known exiftool version in CI.

- [ ] **A man page and shell completions.** `clap_mangen` and
      `clap_complete`.

## Documentation

- [ ] **Screencast / GIF in the README.** A 30-second capture of the
      TUI flow on a real card.

- [ ] **`docs/SECURITY.md`.** Threat model is light (we're a local
      file copier) but worth stating explicitly: we don't read network,
      we don't run code from the card, exiftool is the only subprocess
      and it runs with `-stay_open` against files we already saw.

## Testing

- [ ] **Integration tests with a synthetic card.** Build a tempdir
      DCIM tree with known EXIF-stamped fixtures, run scan + sync, and
      assert the destination layout. Today's tests are unit-level.

- [ ] **Property tests for path templates.** `proptest` over arbitrary
      datetimes and template strings; assert round-trip and that the
      output never contains forbidden characters for the target FS.

## Recently closed

- `60848cb` — Pre-skip index: build `DestIndex` over `images_root` +
  `videos_root` and skip already-imported files before exiftool runs.
  ~4× scan speedup on partially-imported cards. Bonus: dedupe survives
  path-template changes.
- `a99aec9` — TUI: show real numbers + 'Cancelled' heading after sync abort.
- `c194af0` — Default copy_workers from 2 to 1.
- `58d14d8` — Copy: run blocking std::fs inside a single spawn_blocking.
- `d2df6a2` — TUI: replace flat review list with grouped destination tree.
- `293f999` — TUI: add raw_mode picker on Confirm screen.
- `8118406` — Lower default metadata_batch_size from 200 to 50.
- `275384a` — Engine: live event streaming during scan_and_plan.
- `19a825c` — TUI: editable destination paths.
- `638d679` — TUI frontend + mount auto-detection.
- `3446381` — Case-insensitive destination dedupe.
