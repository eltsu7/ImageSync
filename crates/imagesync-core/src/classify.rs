//! Classification of media files into kinds.

use crate::config::RawMode;
use crate::profiles::{CameraProfile, ExtensionKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Image { raw: bool },
    Video,
    Sidecar,
}

impl MediaKind {
    pub fn is_image(self) -> bool {
        matches!(self, MediaKind::Image { .. })
    }
    pub fn is_video(self) -> bool {
        matches!(self, MediaKind::Video)
    }
    pub fn is_raw(self) -> bool {
        matches!(self, MediaKind::Image { raw: true })
    }
    pub fn is_sidecar(self) -> bool {
        matches!(self, MediaKind::Sidecar)
    }
}

/// Classify a file by extension using a profile.
pub fn classify(profile: &CameraProfile, ext: &str) -> Option<MediaKind> {
    profile.classify_extension(ext).map(|k| match k {
        ExtensionKind::RawImage => MediaKind::Image { raw: true },
        ExtensionKind::Image => MediaKind::Image { raw: false },
        ExtensionKind::Video => MediaKind::Video,
        ExtensionKind::Sidecar => MediaKind::Sidecar,
    })
}

/// Apply user filters: returns true if the file should be kept.
pub fn should_include(
    kind: MediaKind,
    raw_mode: RawMode,
    include_videos: bool,
    include_sidecars: bool,
) -> bool {
    match kind {
        MediaKind::Image { raw } => match raw_mode {
            RawMode::All => true,
            RawMode::RawOnly => raw,
            RawMode::NonRawOnly => !raw,
        },
        MediaKind::Video => include_videos,
        MediaKind::Sidecar => include_sidecars,
    }
}
