//! Frontend-neutral events and operation result types emitted by the engine.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::classify::MediaKind;
use crate::metadata::DateSource;
use crate::source::SourceId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileId {
    pub source: SourceId,
    pub rel_path: String,
}

/// What the engine plans to do with a single file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedFile {
    pub source: SourceId,
    pub source_rel_path: String,
    pub source_size: u64,
    pub kind: MediaKindWire,
    pub action: PlannedAction,
    /// Absolute destination path once metadata and routing have been resolved.
    pub dest_path: Option<PathBuf>,
    /// Resolved capture datetime. May be a fallback.
    pub datetime: Option<chrono::NaiveDateTime>,
    pub date_source: Option<DateSourceWire>,
    /// Human-readable reason (skip / error explanation).
    pub reason: Option<String>,
}

impl PlannedFile {
    pub fn id(&self) -> FileId {
        FileId {
            source: self.source.clone(),
            rel_path: self.source_rel_path.clone(),
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectedProfile {
    pub id: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanSummary {
    pub copies: u64,
    pub skips: u64,
    pub errors: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncSummary {
    pub total: u64,
    pub copied: u64,
    pub skipped: u64,
    pub failed: u64,
    pub cancelled: bool,
}

impl SyncSummary {
    pub fn completed(&self) -> u64 {
        self.copied + self.skipped + self.failed
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilePhase {
    Copying,
    Verifying,
    ReadingMetadata,
    ResolvingDestination,
    Finalizing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarningKind {
    ExifToolUnavailable,
    MetadataFallback,
    Template,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationErrorKind {
    Cancelled,
    Configuration,
    Destination,
    Source,
    ExifTool,
    Io,
    Internal,
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
    ProfileDetected {
        profile: DetectedProfile,
    },
    MetadataProgress {
        done: u64,
        total: u64,
    },
    PlanReady {
        summary: PlanSummary,
    },
    ExecutionStarted {
        total_files: u64,
        total_bytes: u64,
        dry_run: bool,
    },
    FileStarted {
        file: PlannedFile,
    },
    FilePhaseChanged {
        file: FileId,
        phase: FilePhase,
    },
    FileProgress {
        file: FileId,
        bytes_done: u64,
        bytes_total: u64,
    },
    FileFinished {
        file: PlannedFile,
        outcome: FileOutcome,
    },
    ExecutionProgress {
        summary: SyncSummary,
    },
    ExecutionFinished {
        summary: SyncSummary,
    },
    Warning {
        kind: WarningKind,
        message: String,
        file: Option<FileId>,
    },
    Error {
        kind: OperationErrorKind,
        message: String,
        file: Option<FileId>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FileOutcome {
    Copied { bytes: u64 },
    Skipped { reason: String },
    Failed { error: String },
}
