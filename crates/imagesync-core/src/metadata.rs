//! Higher-level metadata façade. Wraps [`crate::exiftool::ExifTool`] and adds
//! mtime fallback for files exiftool can't date.

use std::path::PathBuf;

use crate::error::Result;
use crate::exiftool::{ExifTool, FileMetadata};
use crate::source::SourceFile;

/// Source of the chosen capture datetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateSource {
    DateTimeOriginal,
    CreateDate,
    MediaCreateDate,
    ModifyDate,
    FileMtime,
}

impl DateSource {
    pub fn is_fallback(self) -> bool {
        !matches!(self, DateSource::DateTimeOriginal)
    }
    pub fn label(self) -> &'static str {
        match self {
            DateSource::DateTimeOriginal => "DateTimeOriginal",
            DateSource::CreateDate => "CreateDate",
            DateSource::MediaCreateDate => "MediaCreateDate",
            DateSource::ModifyDate => "ModifyDate",
            DateSource::FileMtime => "FileMtime",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedMetadata {
    pub datetime: chrono::NaiveDateTime,
    pub source: DateSource,
    pub make: Option<String>,
    pub model: Option<String>,
}

pub fn resolve_one(
    file: &SourceFile,
    exif: &FileMetadata,
) -> Option<ResolvedMetadata> {
    if let Some(dt) = exif.date_time_original {
        return Some(ResolvedMetadata {
            datetime: dt,
            source: DateSource::DateTimeOriginal,
            make: exif.make.clone(),
            model: exif.model.clone(),
        });
    }
    if let Some(dt) = exif.create_date {
        return Some(ResolvedMetadata {
            datetime: dt,
            source: DateSource::CreateDate,
            make: exif.make.clone(),
            model: exif.model.clone(),
        });
    }
    if let Some(dt) = exif.media_create_date {
        return Some(ResolvedMetadata {
            datetime: dt,
            source: DateSource::MediaCreateDate,
            make: exif.make.clone(),
            model: exif.model.clone(),
        });
    }
    if let Some(dt) = exif.modify_date {
        return Some(ResolvedMetadata {
            datetime: dt,
            source: DateSource::ModifyDate,
            make: exif.make.clone(),
            model: exif.model.clone(),
        });
    }
    if let Some(dt) = file.mtime {
        return Some(ResolvedMetadata {
            datetime: dt,
            source: DateSource::FileMtime,
            make: exif.make.clone(),
            model: exif.model.clone(),
        });
    }
    None
}

/// Read metadata for many files in one ExifTool round-trip and resolve dates.
/// Output preserves the input order; entries with no usable date become `None`.
pub async fn resolve_batch(
    exif: &ExifTool,
    files: &[(SourceFile, PathBuf)],
) -> Result<Vec<Option<ResolvedMetadata>>> {
    let paths: Vec<PathBuf> = files.iter().map(|(_, p)| p.clone()).collect();
    let metas = exif.read_batch(&paths).await?;
    let out = files
        .iter()
        .zip(metas.iter())
        .map(|((sf, _), m)| resolve_one(sf, m))
        .collect();
    Ok(out)
}
