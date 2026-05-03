# Camera Profiles

A camera profile describes how files are laid out on a particular camera's storage and which file extensions are used. Profiles are TOML files. Built-in profiles ship inside the binary; user profiles are loaded from `<config_dir>/imagesync/profiles/*.toml` and override built-ins by `id`.

## Schema

```toml
id = "sony-a7m4"               # unique identifier
display_name = "Sony α7 IV (and similar)"

# File classification (top-level fields, must come BEFORE any [section]).
image_extensions  = ["JPG", "JPEG", "HEIF", "HIF"]
raw_extensions    = ["ARW"]
video_extensions  = ["MP4", "MTS", "M2TS"]
sidecar_extensions = ["XMP"]

# Subdirectories of the source root that contain media files.
# Empty means scan the whole source.
dcim_subdirs = ["DCIM", "PRIVATE/M4ROOT/CLIP", "AVCHD/BDMV/STREAM"]

# Match rules — used to auto-select this profile for a source.
[match]
exif_make = ["SONY"]            # any of these strings (case-insensitive) match
exif_model_glob = []            # optional glob list, e.g. ["ILCE-7M*"]
marker_paths = [                # any path under the source that must exist
    "PRIVATE/SONY/SONYCARD.IND",
]
```

## Built-in profiles

| Profile | Status |
|---|---|
| `sony-a7m4` | shipped in v0.1 |
| `dcim-generic` | shipped in v0.1 (silent fallback) |

## Planned profiles (contributions welcome)

These are not in v0.1. Adding one is a no-code change: drop a TOML file into the user config dir, or open a PR adding it under `crates/imagesync-core/src/profiles/builtin/`.

- [ ] `canon-generic` — CR2, CR3, JPG/HEIF, MP4/MOV. Markers: `DCIM/`, `MISC/`.
- [ ] `nikon-generic` — NEF, NRW, JPG, MP4/MOV. Markers: `NIKON001.DSC`.
- [ ] `fuji-generic` — RAF, JPG, MOV. Markers: `DCIM/`.
- [ ] `olympus-omds-generic` — ORF, JPG, MOV.
- [ ] `panasonic-generic` — RW2, JPG, MP4/MOV. Markers: `PRIVATE/PANA_GRP/`.
- [ ] `dji-generic` — DNG, JPG, MP4/MOV.
- [ ] `gopro-generic` — GPR, JPG, MP4/360.
- [ ] Newer Sony bodies without Mass Storage mode (a7R V, a9 III, ZV-E1) — needs PTP/MTP support first.

## Contributing a profile

1. Identify the camera's RAW extension and any unique marker file/folder on its SD card.
2. Copy `sony-a7m4.toml` from `crates/imagesync-core/src/profiles/builtin/` and edit.
3. Place a sample card image under `crates/imagesync-core/tests/fixtures/<profile-id>/` (just folder structure with empty placeholder files is fine).
4. Add a snapshot test case so detection stays correct.
5. Open a PR.
