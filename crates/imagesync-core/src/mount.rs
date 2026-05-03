//! Removable-volume / mount auto-detection.
//!
//! Returns a best-effort list of currently-mounted volumes that could be
//! camera SD cards. Detection is heuristic per platform:
//!
//! * **Linux**: enumerate `/run/media/$USER/*`, `/media/$USER/*`, `/media/*`,
//!   and `/mnt/*`. Skip the system root.
//! * **macOS**: enumerate `/Volumes/*`, skipping the boot volume.
//! * **Windows**: enumerate drive letters `A:` … `Z:` whose drive type is
//!   `Removable`.
//!
//! Each candidate is scored: presence of a `DCIM` directory bumps it to
//! "likely camera card", which is what callers usually want to surface
//! first. Non-DCIM volumes are still returned (rank `Other`) so the user can
//! pick e.g. an external SSD.

use std::path::{Path, PathBuf};

use crate::source::looks_like_camera_card;

/// A candidate source mount the user might want to import from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedMount {
    /// Absolute path of the mount root.
    pub path: PathBuf,
    /// Friendly name (last path component, or drive letter on Windows).
    pub label: String,
    /// Heuristic ranking — `CameraCard` first in UI lists.
    pub rank: MountRank,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MountRank {
    /// Has a `DCIM` directory at the root.
    CameraCard,
    /// Mounted removable / user volume but no DCIM.
    Removable,
    /// Anything else we surfaced (e.g. `/mnt`).
    Other,
}

/// Detect candidate mounts on the current platform. Never errors — on any
/// I/O hiccup we just return what we have.
pub fn detect_mounts() -> Vec<DetectedMount> {
    let mut out = platform_detect();

    // Filter clearly bogus entries (non-existent paths, the system root).
    out.retain(|m| m.path.exists() && !is_system_root(&m.path));

    // Score: presence of DCIM bumps to CameraCard.
    for m in &mut out {
        if m.rank != MountRank::CameraCard && looks_like_camera_card(&m.path) {
            m.rank = MountRank::CameraCard;
        }
    }

    // Sort: CameraCard first, then Removable, then Other; alpha within rank.
    out.sort_by(|a, b| a.rank.cmp(&b.rank).then_with(|| a.label.cmp(&b.label)));

    // Dedupe by canonical path.
    out.dedup_by(|a, b| canonical(&a.path) == canonical(&b.path));

    out
}

fn canonical(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

fn is_system_root(p: &Path) -> bool {
    let s = p.to_string_lossy();
    matches!(s.as_ref(), "/" | "C:\\" | "C:/")
}

#[cfg(target_os = "linux")]
fn platform_detect() -> Vec<DetectedMount> {
    let mut out = Vec::new();
    let user = std::env::var("USER").or_else(|_| std::env::var("LOGNAME")).ok();

    let mut roots: Vec<PathBuf> = Vec::new();
    if let Some(u) = &user {
        roots.push(PathBuf::from(format!("/run/media/{u}")));
        roots.push(PathBuf::from(format!("/media/{u}")));
    }
    roots.push(PathBuf::from("/media"));
    roots.push(PathBuf::from("/mnt"));

    for r in roots {
        let Ok(rd) = std::fs::read_dir(&r) else { continue };
        for entry in rd.flatten() {
            let path = entry.path();
            // Skip files and the per-user dirs we already enumerated.
            if !path.is_dir() {
                continue;
            }
            let label = entry.file_name().to_string_lossy().to_string();
            // Skip the per-user wrapper dirs themselves (e.g. /media/eeli).
            if Some(&label) == user.as_ref() && (r == Path::new("/media") || r == Path::new("/run/media")) {
                continue;
            }
            out.push(DetectedMount {
                path,
                label,
                rank: MountRank::Removable,
            });
        }
    }

    out
}

#[cfg(target_os = "macos")]
fn platform_detect() -> Vec<DetectedMount> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir("/Volumes") else { return out };
    // Resolve boot volume so we can skip it.
    let boot = std::fs::canonicalize("/").ok();
    for entry in rd.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if let (Some(b), Ok(c)) = (&boot, std::fs::canonicalize(&path)) {
            if &c == b {
                continue;
            }
        }
        let label = entry.file_name().to_string_lossy().to_string();
        out.push(DetectedMount {
            path,
            label,
            rank: MountRank::Removable,
        });
    }
    out
}

#[cfg(target_os = "windows")]
fn platform_detect() -> Vec<DetectedMount> {
    // Use sysinfo's Disks API to enumerate volumes; filter to removable when
    // possible. sysinfo doesn't expose drive type, so we surface every fixed
    // drive too — TUI ranks by DCIM presence.
    let mut out = Vec::new();
    let disks = sysinfo::Disks::new_with_refreshed_list();
    for d in &disks {
        let mount = d.mount_point().to_path_buf();
        if !mount.exists() {
            continue;
        }
        let label = mount.to_string_lossy().trim_end_matches('\\').to_string();
        // Skip C: (best-effort: it's almost always the OS drive).
        if label.eq_ignore_ascii_case("C:") {
            continue;
        }
        out.push(DetectedMount {
            path: mount,
            label,
            rank: if d.is_removable() {
                MountRank::Removable
            } else {
                MountRank::Other
            },
        });
    }
    out
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn platform_detect() -> Vec<DetectedMount> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_ordering_camera_first() {
        assert!(MountRank::CameraCard < MountRank::Removable);
        assert!(MountRank::Removable < MountRank::Other);
    }

    #[test]
    fn detect_does_not_panic() {
        let _ = detect_mounts();
    }
}
