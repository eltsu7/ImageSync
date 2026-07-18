//! Error types for `imagesync-core`.

use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("io error: {0}")]
    IoBare(#[from] std::io::Error),

    #[error("config error: {0}")]
    Config(String),

    #[error("invalid path template `{template}`: {message}")]
    Template { template: String, message: String },

    #[error("exiftool not found on PATH; install it from https://exiftool.org/")]
    ExiftoolMissing,

    #[error("exiftool error: {0}")]
    Exiftool(String),

    #[error("metadata parse error for {path}: {message}")]
    Metadata { path: PathBuf, message: String },

    #[error("profile error: {0}")]
    Profile(String),

    #[error("source error: {0}")]
    Source(String),

    #[error("destination root not found: {path} ({kind}). Create it with `mkdir -p {path}`, or check that the drive is mounted.")]
    DestRootMissing { path: PathBuf, kind: &'static str },

    #[error("destination root is not a directory: {path} ({kind})")]
    DestRootNotDir { path: PathBuf, kind: &'static str },

    #[error("another ImageSync import is already using destination root: {path}")]
    DestinationLocked { path: PathBuf },

    #[error("operation cancelled")]
    Cancelled,

    #[error("internal error: {0}")]
    Internal(String),

    #[error("toml parse error: {0}")]
    TomlDe(#[from] toml::de::Error),

    #[error("toml serialize error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

impl Error {
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Error::Io {
            path: path.into(),
            source,
        }
    }
}
