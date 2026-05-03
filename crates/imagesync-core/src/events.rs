//! Engine event types emitted to frontends.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::classify::MediaKind;
use crate::metadata::DateSource;
use crate::source::SourceId;

/// What the engine plans to do with a single file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedFile {
    pub source: SourceId,
    pub source_rel_path: String,
    pub source_size: u64,
    pub kind: MediaKindWire,
    pub action: PlannedAction,
    /// Absolute destination path (only present when `action == Copy`).
    pub dest_path: Option<PathBuf>,
    /// Resolved capture datetime. May be a fallback.
    pub datetime: Option<chrono::NaiveDateTime>,
    pub date_source: Option<DateSourceWire>,
    /// Human-readable reason (skip / error explanation).
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannedAction {
    Copy,
    SkipExists,
    SkipFiltered,
    SkipNoDate,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKindWire {
    Image,
    RawImage,
    Video,
    Sidecar,
}

impl From<MediaKind> for MediaKindWire {
    fn from(k: MediaKind) -> Self {
        match k {
            MediaKind::Image { raw: true } => MediaKindWire::RawImage,
            MediaKind::Image { raw: false } => MediaKindWire::Image,
            MediaKind::Video => MediaKindWire::Video,
            MediaKind::Sidecar => MediaKindWire::Sidecar,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DateSourceWire {
    DateTimeOriginal,
    CreateDate,
    MediaCreateDate,
    ModifyDate,
    FileMtime,
}

impl From<DateSource> for DateSourceWire {
    fn from(d: DateSource) -> Self {
        match d {
            DateSource::DateTimeOriginal => DateSourceWire::DateTimeOriginal,
            DateSource::CreateDate => DateSourceWire::CreateDate,
            DateSource::MediaCreateDate => DateSourceWire::MediaCreateDate,
            DateSource::ModifyDate => DateSourceWire::ModifyDate,
            DateSource::FileMtime => DateSourceWire::FileMtime,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EngineEvent {
    ScanStarted {
        source: SourceId,
        display_name: String,
    },
    ScanProgress {
        source: SourceId,
        files_seen: u64,
    },
    ScanComplete {
        source: SourceId,
        files: u64,
    },
    MetadataProgress {
        done: u64,
        total: u64,
    },
    PlanReady {
        copies: u64,
        skips: u64,
        errors: u64,
    },
    CopyStarted {
        file: PlannedFile,
    },
    CopyProgress {
        rel_path: String,
        bytes_done: u64,
        bytes_total: u64,
    },
    CopyComplete {
        file: PlannedFile,
        outcome: CopyOutcome,
    },
    SyncSummary {
        copied: u64,
        skipped: u64,
        failed: u64,
    },
    Warning {
        message: String,
        rel_path: Option<String>,
    },
    Error {
        message: String,
        rel_path: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CopyOutcome {
    Copied { bytes: u64 },
    Skipped { reason: String },
    Failed { error: String },
}
