//! `imagesync-core` — engine for importing photos/videos from a camera or SD
//! card into a date-based folder structure.
//!
//! The engine is UI-agnostic: frontends (TUI, GUI, CLI) drive [`Engine`] and
//! consume the [`EngineEvent`] stream it emits.

pub mod classify;
pub mod config;
pub mod copy;
pub mod engine;
pub mod error;
pub mod events;
pub mod exiftool;
pub mod metadata;
pub mod plan;
pub mod profiles;
pub mod source;
pub mod template;

pub use config::EngineConfig;
pub use engine::Engine;
pub use error::{Error, Result};
pub use events::{CopyOutcome, EngineEvent, PlannedAction, PlannedFile};
pub use plan::SyncPlan;
pub use profiles::{CameraProfile, ProfileRegistry};
pub use source::{FilesystemSource, MediaSource, SourceFile, SourceId};
