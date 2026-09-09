//! Configuration: persistent + runtime engine config.

use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

use crate::error::{Error, Result};

pub const CONFIG_FILENAME: &str = "config.toml";
pub const APP_NAME: &str = "imagesync";

/// On-disk configuration. Stored at `<config_dir>/imagesync/config.toml`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub catalogues: BTreeMap<String, CatalogueConfig>,
    pub filters: FiltersConfig,
    pub performance: PerformanceConfig,
    pub verify: VerifyConfig,
    pub profiles: ProfilesConfig,
    pub ui: UiConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogueConfig {
    pub images_root: PathBuf,
    pub videos_root: PathBuf,
    pub images_template: String,
    pub videos_template: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FiltersConfig {
    pub raw_mode: RawMode,
    pub include_videos: bool,
    pub include_sidecars: bool,
}

impl Default for FiltersConfig {
    fn default() -> Self {
        Self {
            raw_mode: RawMode::All,
            include_videos: true,
            include_sidecars: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawMode {
    All,
    RawOnly,
    NonRawOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PerformanceConfig {
    /// 0 = auto (num_cpus).
    pub scan_workers: usize,
    pub metadata_batch_size: usize,
    pub copy_workers: usize,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            scan_workers: 0,
            metadata_batch_size: 50,
            copy_workers: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct VerifyConfig {
    pub enabled: bool,
    pub algorithm: VerifyAlgorithm,
}

impl Default for VerifyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: VerifyAlgorithm::Xxh3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyAlgorithm {
    None,
    Xxh3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProfilesConfig {
    /// `"auto"` or a profile id.
    pub default: String,
}

impl Default for ProfilesConfig {
    fn default() -> Self {
        Self {
            default: "auto".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UiConfig {
    pub remember_last_sources: bool,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            remember_last_sources: true,
        }
    }
}

impl AppConfig {
    /// Load from a path, returning defaults if it doesn't exist.
    pub fn load_or_default(path: &std::path::Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let bytes = std::fs::read_to_string(path).map_err(|e| Error::io(path, e))?;
        let cfg: AppConfig = toml::from_str(&bytes)?;
        cfg.validate_catalogues()?;
        Ok(cfg)
    }

    pub fn catalogue(&self, name: &str) -> Result<&CatalogueConfig> {
        let requested = name.trim();
        self.catalogues
            .iter()
            .find(|(stored, _)| stored.eq_ignore_ascii_case(requested))
            .map(|(_, catalogue)| catalogue)
            .ok_or_else(|| {
                let available = if self.catalogues.is_empty() {
                    "none".to_string()
                } else {
                    self.catalogues
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                Error::Config(format!(
                    "catalogue \"{requested}\" not found; available: {available}"
                ))
            })
    }

    pub fn insert_catalogue(&mut self, name: &str, catalogue: CatalogueConfig) -> Result<String> {
        let name = name.trim();
        if name.is_empty() {
            return Err(Error::Config("catalogue name must not be empty".into()));
        }
        if let Some(existing) = self
            .catalogues
            .keys()
            .find(|stored| stored.eq_ignore_ascii_case(name))
        {
            return Err(Error::Config(format!(
                "catalogue name \"{name}\" conflicts with existing catalogue \"{existing}\""
            )));
        }
        validate_catalogue_templates(name, &catalogue)?;
        crate::engine::check_dest_root(&catalogue.images_root, "images_root")?;
        crate::engine::check_dest_root(&catalogue.videos_root, "videos_root")?;

        let key = name.to_string();
        self.catalogues.insert(key.clone(), catalogue);
        Ok(key)
    }

    fn validate_catalogues(&self) -> Result<()> {
        let mut names: Vec<&str> = Vec::with_capacity(self.catalogues.len());
        for (name, catalogue) in &self.catalogues {
            if name.is_empty() || name.trim() != name {
                return Err(Error::Config(format!(
                    "catalogue name \"{name}\" must not be empty or contain outer whitespace"
                )));
            }
            if let Some(existing) = names
                .iter()
                .find(|existing| existing.eq_ignore_ascii_case(name))
            {
                return Err(Error::Config(format!(
                    "catalogue name \"{name}\" conflicts with existing catalogue \"{existing}\""
                )));
            }
            validate_catalogue_templates(name, catalogue)?;
            names.push(name);
        }
        Ok(())
    }

    pub fn save(&self, path: &std::path::Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| Error::io(parent, e))?;
        }
        let s = toml::to_string_pretty(self)?;
        std::fs::write(path, s).map_err(|e| Error::io(path, e))?;
        Ok(())
    }

    /// Default location: `<config_dir>/imagesync/config.toml`.
    pub fn default_path() -> Option<PathBuf> {
        directories::ProjectDirs::from("", "", APP_NAME)
            .map(|p| p.config_dir().join(CONFIG_FILENAME))
    }
}

fn validate_catalogue_templates(name: &str, catalogue: &CatalogueConfig) -> Result<()> {
    crate::template::PathTemplate::parse(&catalogue.images_template).map_err(|error| {
        Error::Config(format!(
            "catalogue \"{name}\" images folder format is invalid: {error}"
        ))
    })?;
    crate::template::PathTemplate::parse(&catalogue.videos_template).map_err(|error| {
        Error::Config(format!(
            "catalogue \"{name}\" videos folder format is invalid: {error}"
        ))
    })?;
    Ok(())
}

/// Runtime engine configuration. Built from [`AppConfig`] + CLI overrides.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub images_root: PathBuf,
    pub videos_root: PathBuf,
    pub images_template: String,
    pub videos_template: String,
    pub filters: FiltersConfig,
    pub performance: PerformanceConfig,
    pub verify: VerifyConfig,
    /// Path template warnings (e.g. unknown tokens) collected during config
    /// build. The engine surfaces these as [`crate::EngineEvent::Warning`].
    pub template_warnings: Vec<String>,
}

impl EngineConfig {
    pub fn try_from_catalogue(cfg: &AppConfig, catalogue_name: &str) -> Result<Self> {
        let catalogue = cfg.catalogue(catalogue_name)?;

        crate::template::PathTemplate::parse(&catalogue.images_template)?;
        crate::template::PathTemplate::parse(&catalogue.videos_template)?;

        Ok(Self {
            images_root: catalogue.images_root.clone(),
            videos_root: catalogue.videos_root.clone(),
            images_template: catalogue.images_template.clone(),
            videos_template: catalogue.videos_template.clone(),
            filters: cfg.filters.clone(),
            performance: cfg.performance.clone(),
            verify: cfg.verify.clone(),
            template_warnings: Vec::new(),
        })
    }

    pub fn scan_workers(&self) -> usize {
        if self.performance.scan_workers == 0 {
            num_cpus::get().max(1)
        } else {
            self.performance.scan_workers
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEMPLATE: &str = "{yyyy}/{yyyy}-{mm}-{dd}";

    fn catalogue(images_root: PathBuf, videos_root: PathBuf) -> CatalogueConfig {
        CatalogueConfig {
            images_root,
            videos_root,
            images_template: TEMPLATE.into(),
            videos_template: TEMPLATE.into(),
        }
    }

    fn load(contents: &str) -> Result<AppConfig> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, contents).unwrap();
        AppConfig::load_or_default(&path)
    }

    #[test]
    fn loads_catalogues_in_btree_order() {
        let cfg = load(
            r#"
[catalogues.Zebra]
images_root = "/offline/images-z"
videos_root = "/offline/videos-z"
images_template = "{yyyy}"
videos_template = "{yyyy}"

[catalogues.Alpha]
images_root = "/offline/images-a"
videos_root = "/offline/videos-a"
images_template = "{yyyy}/{mm}"
videos_template = "{yyyy}/{mm}"
"#,
        )
        .unwrap();

        assert_eq!(
            cfg.catalogues
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["Alpha", "Zebra"]
        );
    }

    #[test]
    fn rejects_invalid_deserialized_catalogue_names() {
        for contents in [
            r#"
[catalogues.""]
images_root = "/offline/images"
videos_root = "/offline/videos"
images_template = "{yyyy}"
videos_template = "{yyyy}"
"#,
            r#"
[catalogues." Photos "]
images_root = "/offline/images"
videos_root = "/offline/videos"
images_template = "{yyyy}"
videos_template = "{yyyy}"
"#,
            r#"
[catalogues.Photos]
images_root = "/offline/images"
videos_root = "/offline/videos"
images_template = "{yyyy}"
videos_template = "{yyyy}"
[catalogues.photos]
images_root = "/offline/images-2"
videos_root = "/offline/videos-2"
images_template = "{yyyy}"
videos_template = "{yyyy}"
"#,
        ] {
            assert!(load(contents).is_err(), "{contents}");
        }
    }

    #[test]
    fn rejects_missing_fields_and_invalid_templates() {
        assert!(load(
            r#"
[catalogues.Photos]
images_root = "/offline/images"
videos_root = "/offline/videos"
images_template = "{yyyy}"
"#
        )
        .is_err());
        assert!(load(
            r#"
[catalogues.Photos]
images_root = "/offline/images"
videos_root = "/offline/videos"
images_template = "{unknown}"
videos_template = "{yyyy}"
"#
        )
        .is_err());
    }

    #[test]
    fn legacy_paths_do_not_create_catalogues() {
        let cfg = load(
            r#"
[paths]
images_root = "/legacy/images"
videos_root = "/legacy/videos"
images_template = "{yyyy}"
videos_template = "{yyyy}"
"#,
        )
        .unwrap();
        assert!(cfg.catalogues.is_empty());
    }

    #[test]
    fn lookup_is_trimmed_case_insensitive_and_lists_available_names() {
        let mut cfg = AppConfig::default();
        cfg.catalogues.insert(
            "Film".into(),
            catalogue("/offline/film-images".into(), "/offline/film-videos".into()),
        );
        cfg.catalogues.insert(
            "Photos".into(),
            catalogue(
                "/offline/photo-images".into(),
                "/offline/photo-videos".into(),
            ),
        );

        assert_eq!(
            cfg.catalogue(" photos ").unwrap().images_root,
            PathBuf::from("/offline/photo-images")
        );
        assert_eq!(
            cfg.catalogue("missing").unwrap_err().to_string(),
            "config error: catalogue \"missing\" not found; available: Film, Photos"
        );
        assert_eq!(
            AppConfig::default()
                .catalogue("missing")
                .unwrap_err()
                .to_string(),
            "config error: catalogue \"missing\" not found; available: none"
        );
    }

    #[test]
    fn insertion_validates_name_templates_and_roots() {
        let dir = tempfile::tempdir().unwrap();
        let images = dir.path().join("images");
        let videos = dir.path().join("videos");
        std::fs::create_dir_all(&images).unwrap();
        std::fs::create_dir_all(&videos).unwrap();
        let not_dir = dir.path().join("file");
        std::fs::write(&not_dir, "x").unwrap();

        let mut cfg = AppConfig::default();
        let key = cfg
            .insert_catalogue("  My Photos  ", catalogue(images.clone(), videos.clone()))
            .unwrap();
        assert_eq!(key, "My Photos");
        assert!(cfg.catalogues.contains_key("My Photos"));

        assert!(cfg
            .insert_catalogue("my photos", catalogue(images.clone(), videos.clone()))
            .unwrap_err()
            .to_string()
            .contains("conflicts with existing catalogue \"My Photos\""));
        assert!(cfg
            .insert_catalogue("", catalogue(images.clone(), videos.clone()))
            .is_err());

        let mut invalid_template = catalogue(images.clone(), videos.clone());
        invalid_template.images_template = "{unknown}".into();
        assert!(cfg
            .insert_catalogue("Bad template", invalid_template)
            .is_err());
        assert!(matches!(
            cfg.insert_catalogue(
                "Missing root",
                catalogue(dir.path().join("missing"), videos.clone())
            ),
            Err(Error::DestRootMissing { .. })
        ));
        assert!(matches!(
            cfg.insert_catalogue("File root", catalogue(images, not_dir)),
            Err(Error::DestRootNotDir { .. })
        ));
    }

    #[test]
    fn equal_roots_are_valid_and_runtime_selection_preserves_globals() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("media");
        std::fs::create_dir(&root).unwrap();
        let mut cfg = AppConfig::default();
        cfg.filters.raw_mode = RawMode::RawOnly;
        cfg.filters.include_videos = false;
        cfg.performance.scan_workers = 7;
        cfg.verify.enabled = true;
        cfg.insert_catalogue(
            "Combined",
            CatalogueConfig {
                images_root: root.clone(),
                videos_root: root.clone(),
                images_template: "{yyyy}/images".into(),
                videos_template: "{yyyy}/videos".into(),
            },
        )
        .unwrap();

        let runtime = EngineConfig::try_from_catalogue(&cfg, "combined").unwrap();
        assert_eq!(runtime.images_root, root);
        assert_eq!(runtime.videos_root, runtime.images_root);
        assert_eq!(runtime.images_template, "{yyyy}/images");
        assert_eq!(runtime.videos_template, "{yyyy}/videos");
        assert_eq!(runtime.filters.raw_mode, RawMode::RawOnly);
        assert!(!runtime.filters.include_videos);
        assert_eq!(runtime.performance.scan_workers, 7);
        assert!(runtime.verify.enabled);
        assert!(runtime.template_warnings.is_empty());
    }
}
