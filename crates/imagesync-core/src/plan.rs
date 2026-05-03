//! Sync planning: scanned files + metadata → list of planned actions with
//! computed destination paths and dedupe verdicts.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::classify::{self, MediaKind};
use crate::config::EngineConfig;
use crate::error::Result;
use crate::events::{MediaKindWire, PlannedAction, PlannedFile};
use crate::metadata::ResolvedMetadata;
use crate::profiles::CameraProfile;
use crate::source::{SourceFile, SourceId};
use crate::template::PathTemplate;

/// One scanned file with its (optional) resolved metadata.
#[derive(Debug, Clone)]
pub struct ScannedFile {
    pub source: SourceId,
    pub file: SourceFile,
    pub kind: MediaKind,
    pub metadata: Option<ResolvedMetadata>,
}

#[derive(Debug, Clone, Default)]
pub struct SyncPlan {
    pub items: Vec<PlannedFile>,
}

impl SyncPlan {
    pub fn copies(&self) -> u64 {
        self.items
            .iter()
            .filter(|p| p.action == PlannedAction::Copy)
            .count() as u64
    }
    pub fn skips(&self) -> u64 {
        self.items
            .iter()
            .filter(|p| {
                matches!(
                    p.action,
                    PlannedAction::SkipExists
                        | PlannedAction::SkipFiltered
                        | PlannedAction::SkipNoDate
                )
            })
            .count() as u64
    }
    pub fn errors(&self) -> u64 {
        self.items
            .iter()
            .filter(|p| p.action == PlannedAction::Error)
            .count() as u64
    }
}

/// Compute the destination path for a media file, given its parent media's
/// datetime. For sidecars, callers should pass the parent's datetime.
pub fn dest_path_for(
    cfg: &EngineConfig,
    kind: MediaKind,
    datetime: &chrono::NaiveDateTime,
    source_file_name: &str,
) -> Result<PathBuf> {
    let (root, template_str) = match kind {
        MediaKind::Image { .. } | MediaKind::Sidecar => {
            // Sidecars follow their image. Plan code passes the image kind
            // when looking up sidecar destinations, but if a sidecar is
            // planned standalone (orphan) we treat it like an image.
            (&cfg.images_root, &cfg.images_template)
        }
        MediaKind::Video => (&cfg.videos_root, &cfg.videos_template),
    };
    let template = PathTemplate::parse(template_str)?;
    let rendered = template.render(datetime);
    let mut out = root.clone();
    for part in rendered.split('/').filter(|p| !p.is_empty()) {
        out.push(part);
    }
    out.push(source_file_name);
    Ok(out)
}

/// Build a [`SyncPlan`] from scanned files. Sidecars (`.xmp` etc.) are paired
/// with their parent image (same source-relative directory + same basename
/// stem) and inherit the image's destination directory and datetime. Orphan
/// sidecars are still planned, dated by their own metadata or mtime.
pub fn build_plan(
    cfg: &EngineConfig,
    _profile: &CameraProfile,
    scanned: Vec<ScannedFile>,
) -> Result<SyncPlan> {
    // Separate sidecars from non-sidecars.
    let mut media: Vec<ScannedFile> = Vec::new();
    let mut sidecars: Vec<ScannedFile> = Vec::new();
    for s in scanned {
        if s.kind.is_sidecar() {
            sidecars.push(s);
        } else {
            media.push(s);
        }
    }

    // Index media by (source-rel-dir, stem) so sidecars can find them.
    let mut media_index: HashMap<(String, String), &ScannedFile> = HashMap::new();
    for m in &media {
        let key = stem_key(&m.file.rel_path);
        media_index.insert(key, m);
    }

    let mut items: Vec<PlannedFile> = Vec::with_capacity(media.len() + sidecars.len());

    for m in &media {
        items.push(plan_one_media(cfg, m)?);
    }

    for s in sidecars {
        let key = stem_key(&s.file.rel_path);
        if let Some(parent) = media_index.get(&key).copied() {
            items.push(plan_sidecar_with_parent(cfg, &s, parent)?);
        } else {
            // Orphan sidecar — plan it standalone if we have any date.
            items.push(plan_one_media(cfg, &s)?);
        }
    }

    Ok(SyncPlan { items })
}

/// Compute (rel_dir, stem) key. `rel_path` uses `/` separators.
fn stem_key(rel_path: &str) -> (String, String) {
    let (dir, name) = match rel_path.rsplit_once('/') {
        Some((d, n)) => (d.to_string(), n.to_string()),
        None => (String::new(), rel_path.to_string()),
    };
    let stem = name
        .rsplit_once('.')
        .map(|(s, _)| s.to_string())
        .unwrap_or(name.clone());
    (dir, stem.to_ascii_uppercase())
}

fn file_name_of(rel_path: &str) -> &str {
    rel_path.rsplit('/').next().unwrap_or(rel_path)
}

fn plan_one_media(cfg: &EngineConfig, s: &ScannedFile) -> Result<PlannedFile> {
    // Apply user filters.
    if !classify::should_include(
        s.kind,
        cfg.filters.raw_mode,
        cfg.filters.include_videos,
        cfg.filters.include_sidecars,
    ) {
        return Ok(PlannedFile {
            source: s.source.clone(),
            source_rel_path: s.file.rel_path.clone(),
            source_size: s.file.size,
            kind: MediaKindWire::from(s.kind),
            action: PlannedAction::SkipFiltered,
            dest_path: None,
            datetime: None,
            date_source: None,
            reason: Some("filtered by user settings".to_string()),
        });
    }

    let Some(meta) = &s.metadata else {
        return Ok(PlannedFile {
            source: s.source.clone(),
            source_rel_path: s.file.rel_path.clone(),
            source_size: s.file.size,
            kind: MediaKindWire::from(s.kind),
            action: PlannedAction::SkipNoDate,
            dest_path: None,
            datetime: None,
            date_source: None,
            reason: Some("no usable date (no EXIF and no mtime)".to_string()),
        });
    };

    let name = file_name_of(&s.file.rel_path);
    let dest = dest_path_for(cfg, s.kind, &meta.datetime, name)?;
    let action = decide_action(&dest, s.file.size);
    let reason = match action {
        PlannedAction::SkipExists => Some("destination exists with same size".into()),
        PlannedAction::Error => Some("destination exists with different size".into()),
        _ => None,
    };

    Ok(PlannedFile {
        source: s.source.clone(),
        source_rel_path: s.file.rel_path.clone(),
        source_size: s.file.size,
        kind: MediaKindWire::from(s.kind),
        action,
        dest_path: Some(dest),
        datetime: Some(meta.datetime),
        date_source: Some(meta.source.into()),
        reason,
    })
}

fn plan_sidecar_with_parent(
    cfg: &EngineConfig,
    s: &ScannedFile,
    parent: &ScannedFile,
) -> Result<PlannedFile> {
    if !cfg.filters.include_sidecars {
        return Ok(PlannedFile {
            source: s.source.clone(),
            source_rel_path: s.file.rel_path.clone(),
            source_size: s.file.size,
            kind: MediaKindWire::Sidecar,
            action: PlannedAction::SkipFiltered,
            dest_path: None,
            datetime: None,
            date_source: None,
            reason: Some("sidecars disabled".into()),
        });
    }

    // Inherit parent's datetime; place in parent's destination dir.
    let Some(parent_meta) = &parent.metadata else {
        return Ok(PlannedFile {
            source: s.source.clone(),
            source_rel_path: s.file.rel_path.clone(),
            source_size: s.file.size,
            kind: MediaKindWire::Sidecar,
            action: PlannedAction::SkipNoDate,
            dest_path: None,
            datetime: None,
            date_source: None,
            reason: Some("parent media has no date".into()),
        });
    };

    let name = file_name_of(&s.file.rel_path);
    let dest = dest_path_for(cfg, parent.kind, &parent_meta.datetime, name)?;
    let action = decide_action(&dest, s.file.size);
    let reason = match action {
        PlannedAction::SkipExists => Some("destination exists with same size".into()),
        PlannedAction::Error => Some("destination exists with different size".into()),
        _ => None,
    };

    Ok(PlannedFile {
        source: s.source.clone(),
        source_rel_path: s.file.rel_path.clone(),
        source_size: s.file.size,
        kind: MediaKindWire::Sidecar,
        action,
        dest_path: Some(dest),
        datetime: Some(parent_meta.datetime),
        date_source: Some(parent_meta.source.into()),
        reason,
    })
}

fn decide_action(dest: &Path, source_size: u64) -> PlannedAction {
    match std::fs::metadata(dest) {
        Ok(m) if m.is_file() => {
            if m.len() == source_size {
                PlannedAction::SkipExists
            } else {
                PlannedAction::Error
            }
        }
        Ok(_) => PlannedAction::Error, // dest exists but isn't a file
        Err(_) => PlannedAction::Copy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FiltersConfig, PerformanceConfig, RawMode, VerifyConfig};
    use chrono::NaiveDate;

    fn cfg(images: &str, videos: &str, tmpl: &str) -> EngineConfig {
        EngineConfig {
            images_root: PathBuf::from(images),
            videos_root: PathBuf::from(videos),
            images_template: tmpl.to_string(),
            videos_template: tmpl.to_string(),
            filters: FiltersConfig::default(),
            performance: PerformanceConfig::default(),
            verify: VerifyConfig::default(),
            template_warnings: Vec::new(),
        }
    }

    fn dt() -> chrono::NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 5, 3)
            .unwrap()
            .and_hms_opt(13, 45, 7)
            .unwrap()
    }

    #[test]
    fn default_template_image_path() {
        let c = cfg("/photos", "/videos", "{yyyy}/{yyyy}-{mm}-{dd}");
        let p = dest_path_for(&c, MediaKind::Image { raw: true }, &dt(), "DSC00001.ARW").unwrap();
        assert_eq!(
            p,
            PathBuf::from("/photos/2026/2026-05-03/DSC00001.ARW")
        );
    }

    #[test]
    fn yyyy_mm_dd_template_video_path() {
        let c = cfg("/photos", "/videos", "{yyyy}/{mm}/{dd}");
        let p = dest_path_for(&c, MediaKind::Video, &dt(), "C0001.MP4").unwrap();
        assert_eq!(p, PathBuf::from("/videos/2026/05/03/C0001.MP4"));
    }

    #[test]
    fn raw_only_skips_jpegs() {
        assert!(!classify::should_include(
            MediaKind::Image { raw: false },
            RawMode::RawOnly,
            true,
            true
        ));
        assert!(classify::should_include(
            MediaKind::Image { raw: true },
            RawMode::RawOnly,
            true,
            true
        ));
    }
}
