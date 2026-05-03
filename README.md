# imagesync

A cross-platform TUI tool that copies photos and videos from a camera or SD card into a date-based folder structure derived from EXIF metadata.

**Status:** early development (v0.1 in progress).

## Features (planned for v0.1)

- Copy images and videos to separate destination roots.
- Configurable date-based path template (default `{yyyy}/{yyyy}-{mm}-{dd}`).
- Filter RAW vs non-RAW (`all` / `raw_only` / `non_raw_only`).
- Skip files that already exist at destination (size-based dedupe).
- Pluggable per-camera profiles (TOML, no code changes needed).
- Sony α (a7 IV and similar) supported in v0.1 via Mass Storage mode.

## Requirements

- [ExifTool](https://exiftool.org/) on `PATH`. Install via:
  - Linux: `apt install libimage-exiftool-perl` (or your package manager)
  - macOS: `brew install exiftool`
  - Windows: `winget install OliverBetz.ExifTool` or download from exiftool.org

## Sony camera setup (v0.1)

Set your camera's USB connection mode to **Mass Storage** (a7 IV: `Menu → Setup → USB → USB Connection: Mass Storage`). The camera will then appear as a regular USB drive on Linux, macOS, and Windows. SD card readers also work.

Cameras without Mass Storage mode (newer Sony bodies, etc.) will be supported via PTP/MTP in a later release.

## License

MIT — see [LICENSE](LICENSE).
