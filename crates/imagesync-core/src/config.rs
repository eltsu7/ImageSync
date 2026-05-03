//! Configuration: persistent + runtime engine config.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::error::{Error, Result};

pub const CONFIG_FILENAME: &str = "config.toml";
pub const APP_NAME: &str = "imagesync";

/// On-disk configuration. Stored at `<config_dir>/imagesync/config.toml`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub paths: PathsConfig,
    pub filters: FiltersConfig,
    pub performance: PerformanceConfig,
    pub verify: VerifyConfig,
    pub profiles: ProfilesConfig,
    pub ui: UiConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PathsConfig {
    pub images_root: Option<PathBuf>,
    pub videos_root: Option<PathBuf>,
    /// Path template for image destinations, relative to `images_root`.
    /// Tokens: `{yyyy}`, `{yy}`, `{mm}`, `{dd}`, `{month}`, `{Month}`,
    /// `{HH}`, `{MM}`, `{SS}`. May contain `/` to nest directories.
    pub images_template: String,
    /// Path template for video destinations.
    pub videos_template: String,
}

impl Default for PathsConfig {
    fn default() -> Self {
        Self {
            images_root: None,
            videos_root: None,
            images_template: "{yyyy}/{yyyy}-{mm}-{dd}".to_string(),
            videos_template: "{yyyy}/{yyyy}-{mm}-{dd}".to_string(),
        }
    }
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
        Ok(cfg)
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
    /// Build from [`AppConfig`]. Both image and video roots must be set.
    pub fn try_from_app(cfg: &AppConfig) -> Result<Self> {
        let images_root = cfg
            .paths
            .images_root
            .clone()
            .ok_or_else(|| Error::Config("paths.images_root not set".into()))?;
        let videos_root = cfg
            .paths
            .videos_root
            .clone()
            .ok_or_else(|| Error::Config("paths.videos_root not set".into()))?;

        // Validate templates eagerly so the user sees errors at startup.
        crate::template::PathTemplate::parse(&cfg.paths.images_template)?;
        crate::template::PathTemplate::parse(&cfg.paths.videos_template)?;

        Ok(Self {
            images_root,
            videos_root,
            images_template: cfg.paths.images_template.clone(),
            videos_template: cfg.paths.videos_template.clone(),
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
