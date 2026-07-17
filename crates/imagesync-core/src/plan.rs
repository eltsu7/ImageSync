//! Sync planning: scanned files + metadata → list of planned actions with
//! computed destination paths and dedupe verdicts.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::classify::{self, MediaKind};
use crate::config::EngineConfig;
use crate::error::Result;
use crate::events::{DateSourceWire, MediaKindWire, PlannedAction, PlannedFile};
use crate::metadata::{DateSource, ResolvedMetadata};
use crate::profiles::CameraProfile;
use crate::source::{SourceFile, SourceId};
use crate::template::PathTemplate;

/// Name of the per-root staging directory used by the copy→exif→move executor.
/// Lives under each destination root so the final placement is a same-filesystem
/// rename. Excluded from the pre-skip [`DestIndex`] so in-flight staged files
/// don't masquerade as already-imported library members.
pub const STAGING_DIR_NAME: &str = ".imagesync-staging";

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
    /// Per-file display rows: provisional copy candidates (no destination yet,
    /// dated by mtime if available) plus already-decided skips/filters. Drives
    /// the preview UI and the `copies/skips/errors` counts.
    pub items: Vec<PlannedFile>,
    /// Copy units to execute (parent media + inherited sidecars), carrying the
    /// full [`SourceFile`] needed to copy and date each file. Empty for plans
    /// built by the over-USB [`build_plan`] path (dry-run / scan).
    pub units: Vec<CopyUnit>,
}

/// A media file selected for copying, plus any sidecars that inherit its
/// capture date. Produced by [`cheap_plan`] without reading EXIF — the date and
/// destination are resolved later, at copy time, on the local staged file.
#[derive(Debug, Clone)]
pub struct CopyUnit {
    pub parent: SourceFile,
    pub parent_kind: MediaKind,
    /// Sidecars (`.xmp` …) paired to `parent` by `(rel_dir, stem)`. Empty when
    /// `parent` is itself an orphan sidecar.
    pub sidecars: Vec<SourceFile>,
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
pub fn build_plan(cfg: &EngineConfig, scanned: Vec<ScannedFile>) -> Result<SyncPlan> {
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

    // Cache of destination directory listings for case-insensitive dedupe.
    let mut dir_cache = DirCache::default();

    let mut items: Vec<PlannedFile> = Vec::with_capacity(media.len() + sidecars.len());

    for m in &media {
        items.push(plan_one_media(cfg, m, &mut dir_cache)?);
    }

    for s in sidecars {
        let key = stem_key(&s.file.rel_path);
        if let Some(parent) = media_index.get(&key).copied() {
            items.push(plan_sidecar_with_parent(cfg, &s, parent, &mut dir_cache)?);
        } else {
            // Orphan sidecar — plan it standalone if we have any date.
            items.push(plan_one_media(cfg, &s, &mut dir_cache)?);
        }
    }

    Ok(SyncPlan {
        items,
        units: Vec::new(),
    })
}

/// Classify, filter, pre-skip and pair sidecars **without reading EXIF**.
///
/// This is the fast path for the real `sync`: deciding *which* files to copy
/// needs only `(basename, size)` (the pre-skip [`DestIndex`]) and the extension
/// filters — never the capture date. The date and destination folder are
/// resolved later, at copy time, on the local staged copy (see the engine's
/// staging executor). Produces [`CopyUnit`]s to execute plus already-decided
/// skip/filter rows for display.
pub fn cheap_plan(
    cfg: &EngineConfig,
    profile: &CameraProfile,
    source_id: &SourceId,
    files: Vec<SourceFile>,
    dest_index: &DestIndex,
) -> CheapPlan {
    let mut media: Vec<(SourceFile, MediaKind)> = Vec::new();
    let mut sidecars: Vec<SourceFile> = Vec::new();
    let mut decided: Vec<PlannedFile> = Vec::new();

    for f in files {
        let Some(kind) = classify::classify(profile, &f.extension) else {
            continue; // unknown extension; ignore silently
        };
        if !classify::should_include(
            kind,
            cfg.filters.raw_mode,
            cfg.filters.include_videos,
            cfg.filters.include_sidecars,
        ) {
            decided.push(decided_skip(
                source_id,
                &f,
                kind,
                PlannedAction::SkipFiltered,
                None,
                "filtered by user settings",
            ));
            continue;
        }
        if kind.is_sidecar() {
            // Sidecars are tiny and rare; they ride with their parent and are
            // never pre-skipped here (they inherit the parent's date/dir).
            sidecars.push(f);
        } else {
            let basename = file_name_of(&f.rel_path);
            if let Some(existing) = dest_index.lookup(basename, f.size) {
                decided.push(decided_skip(
                    source_id,
                    &f,
                    kind,
                    PlannedAction::SkipExists,
                    Some(existing.to_path_buf()),
                    "already in library (pre-skip index)",
                ));
                continue;
            }
            media.push((f, kind));
        }
    }

    // Index surviving media by (rel_dir, stem) so sidecars can find a parent.
    let mut media_index: HashMap<(String, String), usize> = HashMap::new();
    for (i, (f, _)) in media.iter().enumerate() {
        media_index.insert(stem_key(&f.rel_path), i);
    }

    let mut units: Vec<CopyUnit> = media
        .into_iter()
        .map(|(parent, parent_kind)| CopyUnit {
            parent,
            parent_kind,
            sidecars: Vec::new(),
        })
        .collect();

    for sf in sidecars {
        match media_index.get(&stem_key(&sf.rel_path)) {
            Some(&idx) => units[idx].sidecars.push(sf),
            None => units.push(CopyUnit {
                parent: sf,
                parent_kind: MediaKind::Sidecar,
                sidecars: Vec::new(),
            }),
        }
    }

    CheapPlan { units, decided }
}

/// Output of [`cheap_plan`]: copy units to execute plus already-decided rows.
#[derive(Debug, Default)]
pub struct CheapPlan {
    pub units: Vec<CopyUnit>,
    pub decided: Vec<PlannedFile>,
}

impl CheapPlan {
    /// Flatten into a [`SyncPlan`] for preview/counting. Copy candidates carry a
    /// provisional mtime date and no destination (resolved at copy time).
    pub fn into_sync_plan(self, source_id: &SourceId) -> SyncPlan {
        let mut items = Vec::new();
        for u in &self.units {
            items.push(provisional_copy(source_id, &u.parent, u.parent_kind));
            for s in &u.sidecars {
                items.push(provisional_copy(source_id, s, MediaKind::Sidecar));
            }
        }
        items.extend(self.decided.iter().cloned());
        SyncPlan {
            items,
            units: self.units,
        }
    }
}

fn provisional_copy(source_id: &SourceId, f: &SourceFile, kind: MediaKind) -> PlannedFile {
    PlannedFile {
        source: source_id.clone(),
        source_rel_path: f.rel_path.clone(),
        source_size: f.size,
        kind: MediaKindWire::from(kind),
        action: PlannedAction::Copy,
        dest_path: None,
        datetime: f.mtime,
        date_source: f.mtime.map(|_| DateSourceWire::from(DateSource::FileMtime)),
        reason: None,
    }
}

fn decided_skip(
    source_id: &SourceId,
    f: &SourceFile,
    kind: MediaKind,
    action: PlannedAction,
    dest_path: Option<PathBuf>,
    reason: &str,
) -> PlannedFile {
    PlannedFile {
        source: source_id.clone(),
        source_rel_path: f.rel_path.clone(),
        source_size: f.size,
        kind: MediaKindWire::from(kind),
        action,
        dest_path,
        datetime: None,
        date_source: None,
        reason: Some(reason.to_string()),
    }
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

fn plan_one_media(
    cfg: &EngineConfig,
    s: &ScannedFile,
    dir_cache: &mut DirCache,
) -> Result<PlannedFile> {
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
    let (action, reason) = decide_action_with_reason(&dest, name, s.file.size, dir_cache);

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
    dir_cache: &mut DirCache,
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
    let (action, reason) = decide_action_with_reason(&dest, name, s.file.size, dir_cache);

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

/// Cache of destination directory listings keyed by directory path. Each
/// entry maps a lowercase basename to the actual on-disk filename and its
/// size. Lets us check existence case-insensitively with one `read_dir` per
/// directory instead of per file.
#[derive(Debug, Default)]
struct DirCache {
    cache: HashMap<PathBuf, Option<HashMap<String, DirEntry>>>,
}

#[derive(Debug, Clone)]
struct DirEntry {
    /// Actual on-disk filename (preserves case).
    actual_name: String,
    size: u64,
    is_file: bool,
}

impl DirCache {
    fn lookup(&mut self, dir: &Path, basename: &str) -> Option<DirEntry> {
        let entries = self
            .cache
            .entry(dir.to_path_buf())
            .or_insert_with(|| read_dir_lowercase(dir));
        let key = basename.to_lowercase();
        entries.as_ref().and_then(|m| m.get(&key).cloned())
    }

    fn reserve(&mut self, dest: &Path, basename: &str, size: u64) {
        let Some(parent) = dest.parent() else {
            return;
        };
        let entries = self
            .cache
            .entry(parent.to_path_buf())
            .or_insert_with(|| read_dir_lowercase(parent));
        let entries = entries.get_or_insert_with(HashMap::new);
        entries.insert(
            basename.to_lowercase(),
            DirEntry {
                actual_name: basename.to_string(),
                size,
                is_file: true,
            },
        );
    }
}

fn read_dir_lowercase(dir: &Path) -> Option<HashMap<String, DirEntry>> {
    let rd = std::fs::read_dir(dir).ok()?;
    let mut out = HashMap::new();
    for entry in rd.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        let key = name.to_lowercase();
        let (size, is_file) = match entry.metadata() {
            Ok(m) => (m.len(), m.is_file()),
            Err(_) => (0, false),
        };
        out.insert(
            key,
            DirEntry {
                actual_name: name,
                size,
                is_file,
            },
        );
    }
    Some(out)
}

/// Recursive index of one or more destination roots, keyed by
/// `(lowercase basename, size)`. Lets the engine pre-skip already-imported
/// files without needing to read their EXIF datetime first.
///
/// Two files with the same basename+size are extremely unlikely to be
/// different content in practice (basenames from cameras are dense
/// monotonic counters and size collisions on raw/jpeg files are rare).
/// This is the same key the per-directory `DirCache` dedupe uses, just
/// applied across the whole library so the check doesn't depend on the
/// computed destination subfolder.
#[derive(Debug, Default)]
pub struct DestIndex {
    by_name_size: HashMap<(String, u64), PathBuf>,
}

impl DestIndex {
    /// Build an index by recursively walking each root that exists. Roots
    /// that don't exist or can't be read are silently skipped (treated as
    /// empty).
    pub fn build(roots: &[&Path]) -> Self {
        let mut by_name_size: HashMap<(String, u64), PathBuf> = HashMap::new();
        for root in roots {
            if !root.exists() {
                continue;
            }
            walk_into(root, &mut by_name_size);
        }
        DestIndex { by_name_size }
    }

    /// Number of distinct files indexed. Mostly for logging/tests.
    pub fn len(&self) -> usize {
        self.by_name_size.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_name_size.is_empty()
    }

    /// Look up by case-insensitive basename + exact size. Returns the
    /// actual on-disk path of the matching file if any.
    pub fn lookup(&self, basename: &str, size: u64) -> Option<&Path> {
        let key = (basename.to_lowercase(), size);
        self.by_name_size.get(&key).map(|p| p.as_path())
    }
}

fn walk_into(dir: &Path, out: &mut HashMap<(String, u64), PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in rd.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        let path = entry.path();
        if ft.is_dir() {
            // Skip the staging area: files mid-copy there are not yet library
            // members and must not pre-skip their own source.
            if entry.file_name() == std::ffi::OsStr::new(STAGING_DIR_NAME) {
                continue;
            }
            walk_into(&path, out);
        } else if ft.is_file() {
            let Ok(meta) = entry.metadata() else { continue };
            let name = entry.file_name().to_string_lossy().to_lowercase();
            let key = (name, meta.len());
            // First-write-wins: if the same (name, size) appears in
            // multiple folders, keep the first one we saw. The match is
            // only used to report "exists somewhere"; either path is a
            // valid answer.
            out.entry(key).or_insert(path);
        }
    }
}

/// Per-directory dedupe cache for the staging executor. Wraps the private
/// [`DirCache`] so the engine can decide copy/skip/error verdicts against the
/// final date folder (resolved at copy time) without re-reading each directory.
#[derive(Debug, Default)]
pub struct DedupeCache {
    inner: DirCache,
}

impl DedupeCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decide the action for a resolved destination and reserve its basename
    /// when it is free. Reservations make same-run duplicate names behave like
    /// already-existing files even before their staged copies are moved.
    pub fn reserve(
        &mut self,
        dest: &Path,
        source_name: &str,
        source_size: u64,
    ) -> (PlannedAction, Option<String>) {
        let verdict = decide_action_with_reason(dest, source_name, source_size, &mut self.inner);
        if verdict.0 == PlannedAction::Copy {
            self.inner.reserve(dest, source_name, source_size);
        }
        verdict
    }
}

/// Decide what to do with a planned destination, using case-insensitive
/// lookup. If a file with the same basename (in any case) exists at the
/// destination directory, we treat it as the existing target. Returns the
/// action plus an optional human-readable reason (especially useful when the
/// existing file has different case than the source).
fn decide_action_with_reason(
    dest: &Path,
    source_name: &str,
    source_size: u64,
    dir_cache: &mut DirCache,
) -> (PlannedAction, Option<String>) {
    let Some(parent) = dest.parent() else {
        return (PlannedAction::Copy, None);
    };
    match dir_cache.lookup(parent, source_name) {
        None => (PlannedAction::Copy, None),
        Some(entry) if !entry.is_file => (
            PlannedAction::Error,
            Some(format!(
                "destination path exists but is not a regular file: {}",
                entry.actual_name
            )),
        ),
        Some(entry) if entry.size == source_size => {
            if entry.actual_name == source_name {
                (
                    PlannedAction::SkipExists,
                    Some("destination exists with same size".into()),
                )
            } else {
                (
                    PlannedAction::SkipExists,
                    Some(format!(
                        "destination exists with same size (different case: {})",
                        entry.actual_name
                    )),
                )
            }
        }
        Some(entry) => (
            PlannedAction::Error,
            Some(format!(
                "destination exists ({}) with different size: {} vs source {}",
                entry.actual_name, entry.size, source_size
            )),
        ),
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
        assert_eq!(p, PathBuf::from("/photos/2026/2026-05-03/DSC00001.ARW"));
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

    fn write_file(path: &Path, size: usize) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, vec![0u8; size]).unwrap();
    }

    #[test]
    fn dedupe_no_existing_returns_copy() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("DSC04290.ARW");
        let mut cache = DirCache::default();
        let (action, _) = decide_action_with_reason(&dest, "DSC04290.ARW", 100, &mut cache);
        assert_eq!(action, PlannedAction::Copy);
    }

    #[test]
    fn dedupe_same_case_same_size_skips() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("DSC04290.ARW");
        write_file(&dest, 100);
        let mut cache = DirCache::default();
        let (action, reason) = decide_action_with_reason(&dest, "DSC04290.ARW", 100, &mut cache);
        assert_eq!(action, PlannedAction::SkipExists);
        assert!(reason.unwrap().contains("same size"));
    }

    #[test]
    fn dedupe_different_case_same_size_skips() {
        let tmp = tempfile::tempdir().unwrap();
        // On disk: lowercase. Source: uppercase.
        let on_disk = tmp.path().join("dsc04290.arw");
        write_file(&on_disk, 100);
        let dest = tmp.path().join("DSC04290.ARW");
        let mut cache = DirCache::default();
        let (action, reason) = decide_action_with_reason(&dest, "DSC04290.ARW", 100, &mut cache);
        assert_eq!(action, PlannedAction::SkipExists);
        let r = reason.unwrap();
        assert!(r.contains("different case"), "reason was: {}", r);
        assert!(r.contains("dsc04290.arw"), "reason was: {}", r);
    }

    #[test]
    fn dedupe_different_case_different_size_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let on_disk = tmp.path().join("dsc04290.arw");
        write_file(&on_disk, 50);
        let dest = tmp.path().join("DSC04290.ARW");
        let mut cache = DirCache::default();
        let (action, reason) = decide_action_with_reason(&dest, "DSC04290.ARW", 100, &mut cache);
        assert_eq!(action, PlannedAction::Error);
        let r = reason.unwrap();
        assert!(r.contains("different size"), "reason was: {}", r);
    }

    #[test]
    fn dest_index_finds_file_in_nested_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let nested = tmp.path().join("2026").join("2026-05-03");
        write_file(&nested.join("DSC04290.ARW"), 100);

        let idx = DestIndex::build(&[tmp.path()]);
        assert_eq!(idx.len(), 1);
        let hit = idx.lookup("DSC04290.ARW", 100).unwrap();
        assert_eq!(hit, nested.join("DSC04290.ARW"));
    }

    #[test]
    fn dest_index_lookup_is_case_insensitive() {
        let tmp = tempfile::tempdir().unwrap();
        // On-disk lowercase, query uppercase.
        write_file(&tmp.path().join("2026").join("dsc04290.arw"), 100);

        let idx = DestIndex::build(&[tmp.path()]);
        assert!(idx.lookup("DSC04290.ARW", 100).is_some());
        assert!(idx.lookup("dsc04290.arw", 100).is_some());
    }

    #[test]
    fn dest_index_size_mismatch_is_miss() {
        let tmp = tempfile::tempdir().unwrap();
        write_file(&tmp.path().join("DSC04290.ARW"), 100);

        let idx = DestIndex::build(&[tmp.path()]);
        assert!(idx.lookup("DSC04290.ARW", 100).is_some());
        assert!(idx.lookup("DSC04290.ARW", 99).is_none());
    }

    #[test]
    fn dest_index_handles_missing_root() {
        let tmp = tempfile::tempdir().unwrap();
        let does_not_exist = tmp.path().join("not-here");
        let idx = DestIndex::build(&[does_not_exist.as_path()]);
        assert!(idx.is_empty());
    }

    #[test]
    fn dest_index_excludes_staging_dir() {
        let tmp = tempfile::tempdir().unwrap();
        // A real library file, and a mid-copy file in the staging area.
        write_file(&tmp.path().join("2026").join("IMG001.JPG"), 100);
        write_file(
            &tmp.path()
                .join(STAGING_DIR_NAME)
                .join("7")
                .join("IMG002.JPG"),
            200,
        );

        let idx = DestIndex::build(&[tmp.path()]);
        assert!(idx.lookup("IMG001.JPG", 100).is_some());
        // The staged file must NOT pre-skip its own source.
        assert!(idx.lookup("IMG002.JPG", 200).is_none());
        assert_eq!(idx.len(), 1);
    }

    fn src_file(rel: &str, size: u64, ext: &str) -> SourceFile {
        use crate::source::BackendHandle;
        SourceFile {
            rel_path: rel.to_string(),
            size,
            mtime: NaiveDate::from_ymd_opt(2026, 5, 3)
                .unwrap()
                .and_hms_opt(9, 0, 0),
            extension: ext.to_string(),
            backend_handle: BackendHandle::Path(PathBuf::from(rel)),
        }
    }

    #[test]
    fn cheap_plan_pairs_sidecars_filters_and_preskips() {
        use crate::profiles::ProfileRegistry;

        let tmp = tempfile::tempdir().unwrap();
        let images = tmp.path().join("images");
        let videos = tmp.path().join("videos");
        // An already-imported JPG so the pre-skip index has a hit.
        write_file(&images.join("2026").join("OLD.JPG"), 10);
        let dest_index = DestIndex::build(&[images.as_path(), videos.as_path()]);

        let mut c = cfg(
            images.to_str().unwrap(),
            videos.to_str().unwrap(),
            "{yyyy}/{yyyy}-{mm}-{dd}",
        );
        c.filters.raw_mode = RawMode::NonRawOnly; // RAW should be filtered out

        let reg = ProfileRegistry::with_builtins().unwrap();
        let profile = reg.get("dcim-generic").unwrap();
        let sid = SourceId("s".into());

        let files = vec![
            src_file("DCIM/IMG001.JPG", 100, "jpg"),
            src_file("DCIM/IMG001.XMP", 5, "xmp"), // sidecar of IMG001
            src_file("DCIM/IMG002.ARW", 2000, "arw"), // filtered (non-raw-only)
            src_file("DCIM/VID001.MP4", 9000, "mp4"),
            src_file("DCIM/OLD.JPG", 10, "jpg"), // pre-skipped (in library)
        ];

        let cheap = cheap_plan(&c, profile, &sid, files, &dest_index);

        // Units: IMG001 (with its sidecar) + VID001 = 2. ARW filtered, OLD pre-skipped.
        assert_eq!(cheap.units.len(), 2);
        let img = cheap
            .units
            .iter()
            .find(|u| u.parent.rel_path == "DCIM/IMG001.JPG")
            .unwrap();
        assert_eq!(img.sidecars.len(), 1);
        assert_eq!(img.sidecars[0].rel_path, "DCIM/IMG001.XMP");

        // Decided: 1 filtered (ARW) + 1 pre-skipped (OLD).
        assert_eq!(
            cheap
                .decided
                .iter()
                .filter(|p| p.action == PlannedAction::SkipFiltered)
                .count(),
            1
        );
        assert_eq!(
            cheap
                .decided
                .iter()
                .filter(|p| p.action == PlannedAction::SkipExists)
                .count(),
            1
        );

        // Preview plan: copy rows are provisional (no destination yet).
        let plan = cheap.into_sync_plan(&sid);
        assert_eq!(plan.copies(), 3); // IMG001.JPG, IMG001.XMP, VID001.MP4
        for it in plan
            .items
            .iter()
            .filter(|i| i.action == PlannedAction::Copy)
        {
            assert!(it.dest_path.is_none());
        }
    }

    #[test]
    fn dedupe_reservation_prevents_same_run_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp
            .path()
            .join("2026")
            .join("2026-05-03")
            .join("IMG0001.JPG");
        let mut cache = DedupeCache::new();

        assert_eq!(
            cache.reserve(&dest, "IMG0001.JPG", 100).0,
            PlannedAction::Copy
        );
        assert_eq!(
            cache.reserve(&dest, "IMG0001.JPG", 100).0,
            PlannedAction::SkipExists
        );
    }

    #[test]
    fn dest_index_merges_two_roots() {
        let tmp = tempfile::tempdir().unwrap();
        let images = tmp.path().join("images");
        let videos = tmp.path().join("videos");
        write_file(&images.join("2026").join("DSC04290.ARW"), 100);
        write_file(&videos.join("2026").join("C0001.MP4"), 200);

        let idx = DestIndex::build(&[images.as_path(), videos.as_path()]);
        assert_eq!(idx.len(), 2);
        assert!(idx.lookup("DSC04290.ARW", 100).is_some());
        assert!(idx.lookup("C0001.MP4", 200).is_some());
    }
}
