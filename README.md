# imagesync

A cross-platform TUI tool that copies photos and videos from a camera or SD card into a date-based folder structure derived from EXIF metadata.

**Status:** early development (v0.1 in progress).

## Current capabilities

- Interactive TUI and `scan` / `sync` CLI commands for mounted cameras, SD cards, or directories.
- RAW, image, video, and sidecar filtering; configurable image/video roots and date templates.
- Provisional fast scan, then staged copy → local metadata read → final placement.
- Destination pre-skip and per-folder conflict detection; optional xxh3 copy verification.
- Per-file progress, explicit cancellation with staging cleanup, and filesystem locks per destination root.
- Configurable TOML camera profiles; Sony Mass Storage sources are supported.

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
