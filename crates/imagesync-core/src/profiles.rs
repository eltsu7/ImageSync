//! Camera profile registry and schema.
//!
//! Built-in profiles are embedded as TOML strings via `include_str!`. User
//! profiles loaded from disk override built-ins by `id`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use crate::error::{Error, Result};

/// A camera profile describes file extensions and folder layout for a camera.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CameraProfile {
    pub id: String,
    pub display_name: String,

    #[serde(default, rename = "match")]
    pub match_rules: MatchRules,

    #[serde(default)]
    pub image_extensions: Vec<String>,
    #[serde(default)]
    pub raw_extensions: Vec<String>,
    #[serde(default)]
    pub video_extensions: Vec<String>,
    #[serde(default)]
    pub sidecar_extensions: Vec<String>,

    /// Subdirectories of the source root that contain media. Empty = scan root.
    #[serde(default)]
    pub dcim_subdirs: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MatchRules {
    #[serde(default)]
    pub exif_make: Vec<String>,
    #[serde(default)]
    pub exif_model_glob: Vec<String>,
    #[serde(default)]
    pub marker_paths: Vec<String>,
}

impl CameraProfile {
    pub fn from_toml_str(s: &str) -> Result<Self> {
        Ok(toml::from_str(s)?)
    }

    /// Return true if any marker path exists under `source_root`.
    pub fn matches_markers(&self, source_root: &Path) -> bool {
        self.match_rules
            .marker_paths
            .iter()
            .any(|m| source_root.join(m).exists())
    }

    /// Return true if `make` matches any configured EXIF make (case-insensitive).
    pub fn matches_make(&self, make: &str) -> bool {
        self.match_rules
            .exif_make
            .iter()
            .any(|m| m.eq_ignore_ascii_case(make))
    }

    /// Lower-cased extension lookup helpers.
    pub fn classify_extension(&self, ext: &str) -> Option<ExtensionKind> {
        let lower = ext.to_ascii_lowercase();
        if self
            .raw_extensions
            .iter()
            .any(|e| e.eq_ignore_ascii_case(&lower))
        {
            Some(ExtensionKind::RawImage)
        } else if self
            .image_extensions
            .iter()
            .any(|e| e.eq_ignore_ascii_case(&lower))
        {
            Some(ExtensionKind::Image)
        } else if self
            .video_extensions
            .iter()
            .any(|e| e.eq_ignore_ascii_case(&lower))
        {
            Some(ExtensionKind::Video)
        } else if self
            .sidecar_extensions
            .iter()
            .any(|e| e.eq_ignore_ascii_case(&lower))
        {
            Some(ExtensionKind::Sidecar)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionKind {
    Image,
    RawImage,
    Video,
    Sidecar,
}

const BUILTIN_SONY_A7M4: &str = include_str!("profiles/builtin/sony-a7m4.toml");
const BUILTIN_DCIM_GENERIC: &str = include_str!("profiles/builtin/dcim-generic.toml");

#[derive(Debug, Clone, Default)]
pub struct ProfileRegistry {
    profiles: HashMap<String, CameraProfile>,
    /// Insertion order of built-ins for deterministic detection.
    order: Vec<String>,
}

impl ProfileRegistry {
    /// Registry preloaded with built-in profiles.
    pub fn with_builtins() -> Result<Self> {
        let mut r = Self::default();
        r.add_builtin(BUILTIN_SONY_A7M4)?;
        r.add_builtin(BUILTIN_DCIM_GENERIC)?;
        Ok(r)
    }

    fn add_builtin(&mut self, src: &str) -> Result<()> {
        let p = CameraProfile::from_toml_str(src).map_err(|e| {
            Error::Profile(format!("failed to parse builtin profile: {e}"))
        })?;
        self.insert(p);
        Ok(())
    }

    pub fn insert(&mut self, profile: CameraProfile) {
        if !self.profiles.contains_key(&profile.id) {
            self.order.push(profile.id.clone());
        }
        self.profiles.insert(profile.id.clone(), profile);
    }

    pub fn get(&self, id: &str) -> Option<&CameraProfile> {
        self.profiles.get(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &CameraProfile> {
        self.order.iter().filter_map(|id| self.profiles.get(id))
    }

    /// Load any `*.toml` files from `dir`, overriding built-ins by id.
    /// Missing dir is not an error.
    pub fn load_user_dir(&mut self, dir: &Path) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(dir).map_err(|e| Error::io(dir, e))? {
            let entry = entry.map_err(|e| Error::io(dir, e))?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("toml") {
                continue;
            }
            let s = std::fs::read_to_string(&path).map_err(|e| Error::io(&path, e))?;
            let p = CameraProfile::from_toml_str(&s).map_err(|e| {
                Error::Profile(format!(
                    "failed to parse user profile {}: {e}",
                    path.display()
                ))
            })?;
            self.insert(p);
        }
        Ok(())
    }

    /// Pick a profile for a given source root.
    /// Order: marker match → first profile with markers wins → fallback to
    /// `dcim-generic`.
    pub fn detect_for_source(&self, source_root: &Path) -> &CameraProfile {
        for p in self.iter() {
            if !p.match_rules.marker_paths.is_empty() && p.matches_markers(source_root) {
                return p;
            }
        }
        self.get("dcim-generic")
            .expect("dcim-generic builtin must exist")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_load() {
        let r = ProfileRegistry::with_builtins().unwrap();
        assert!(r.get("sony-a7m4").is_some());
        assert!(r.get("dcim-generic").is_some());
    }

    #[test]
    fn sony_classifies_arw_as_raw() {
        let r = ProfileRegistry::with_builtins().unwrap();
        let p = r.get("sony-a7m4").unwrap();
        assert_eq!(p.classify_extension("ARW"), Some(ExtensionKind::RawImage));
        assert_eq!(p.classify_extension("arw"), Some(ExtensionKind::RawImage));
        assert_eq!(p.classify_extension("JPG"), Some(ExtensionKind::Image));
        assert_eq!(p.classify_extension("MP4"), Some(ExtensionKind::Video));
        assert_eq!(p.classify_extension("XMP"), Some(ExtensionKind::Sidecar));
        assert_eq!(p.classify_extension("zzz"), None);
    }
}
