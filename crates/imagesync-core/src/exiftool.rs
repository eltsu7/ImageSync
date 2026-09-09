//! ExifTool subprocess driver, using `-stay_open True -@ -` for batch reads.
//!
//! ExifTool is launched once and fed argument lists separated by `-execute`
//! markers. Output is JSON parsed lazily. This is ~60× faster than spawning
//! exiftool per file.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

use crate::error::{Error, Result};

/// Tags requested for every file. Order matters for determinism only; ExifTool
/// returns a JSON object so order is irrelevant for parsing.
const REQUESTED_TAGS: &[&str] = &[
    "-DateTimeOriginal",
    "-CreateDate",
    "-MediaCreateDate",
    "-ModifyDate",
    "-Make",
    "-Model",
    "-FileType",
    "-MIMEType",
];

/// Parsed metadata fields imagesync cares about.
#[derive(Debug, Clone, Default)]
pub struct FileMetadata {
    pub date_time_original: Option<chrono::NaiveDateTime>,
    pub create_date: Option<chrono::NaiveDateTime>,
    pub media_create_date: Option<chrono::NaiveDateTime>,
    pub modify_date: Option<chrono::NaiveDateTime>,
    pub make: Option<String>,
    pub model: Option<String>,
}

impl FileMetadata {
    /// Best-effort capture datetime in priority order.
    pub fn best_datetime(&self) -> Option<chrono::NaiveDateTime> {
        self.date_time_original
            .or(self.create_date)
            .or(self.media_create_date)
            .or(self.modify_date)
    }
}

/// Probe `exiftool` on PATH and return its version string.
pub fn probe_version() -> Result<String> {
    let out = std::process::Command::new("exiftool")
        .arg("-ver")
        .output()
        .map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => Error::ExiftoolMissing,
            _ => Error::Exiftool(format!("failed to run exiftool: {e}")),
        })?;
    if !out.status.success() {
        return Err(Error::Exiftool(format!(
            "exiftool exited with status {}",
            out.status
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Long-lived ExifTool process used for batch reads.
pub struct ExifTool {
    inner: Mutex<Inner>,
}

struct Inner {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    counter: u64,
}

impl ExifTool {
    /// Spawn a stay_open exiftool process.
    pub async fn spawn() -> Result<Self> {
        let mut child = Command::new("exiftool")
            .args(["-stay_open", "True", "-@", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => Error::ExiftoolMissing,
                _ => Error::Exiftool(format!("failed to spawn exiftool: {e}")),
            })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::Exiftool("no stdin".into()))?;
        let stdout = BufReader::new(
            child
                .stdout
                .take()
                .ok_or_else(|| Error::Exiftool("no stdout".into()))?,
        );

        Ok(Self {
            inner: Mutex::new(Inner {
                child,
                stdin,
                stdout,
                counter: 0,
            }),
        })
    }

    /// Read metadata for a batch of files. Order of returned items mirrors
    /// `paths`; missing files are returned with default metadata and a
    /// warning logged.
    pub async fn read_batch(&self, paths: &[PathBuf]) -> Result<Vec<FileMetadata>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let mut g = self.inner.lock().await;
        g.counter += 1;
        let exec_tag = g.counter;

        // Build the argfile body.
        let mut buf = String::new();
        buf.push_str("-json\n");
        buf.push_str("-charset\nfilename=UTF8\n");
        buf.push_str("-fast2\n");
        buf.push_str("-q\n");
        buf.push_str("-d\n");
        buf.push_str("%Y-%m-%d %H:%M:%S\n");
        for tag in REQUESTED_TAGS {
            buf.push_str(tag);
            buf.push('\n');
        }
        for p in paths {
            // exiftool reads each line as a single argument verbatim
            buf.push_str(p.to_string_lossy().as_ref());
            buf.push('\n');
        }
        buf.push_str(&format!("-execute{exec_tag}\n"));

        g.stdin
            .write_all(buf.as_bytes())
            .await
            .map_err(|e| Error::Exiftool(format!("write to stdin: {e}")))?;
        g.stdin
            .flush()
            .await
            .map_err(|e| Error::Exiftool(format!("flush: {e}")))?;

        // Read until we see {ready<exec_tag>}.
        let ready_marker = format!("{{ready{exec_tag}}}");
        let mut json_out = String::new();
        loop {
            let mut line = String::new();
            let n = g
                .stdout
                .read_line(&mut line)
                .await
                .map_err(|e| Error::Exiftool(format!("read line: {e}")))?;
            if n == 0 {
                return Err(Error::Exiftool(
                    "exiftool stdout closed unexpectedly".into(),
                ));
            }
            let trimmed = line.trim_end();
            if trimmed == ready_marker {
                break;
            }
            json_out.push_str(&line);
        }

        if json_out.trim().is_empty() {
            // No JSON output. Could happen if exiftool failed on every path.
            return Ok(paths.iter().map(|_| FileMetadata::default()).collect());
        }

        let entries: Vec<RawEntry> = serde_json::from_str(&json_out).map_err(|e| {
            Error::Exiftool(format!(
                "failed to parse exiftool JSON ({} bytes): {e}",
                json_out.len()
            ))
        })?;

        // Map by SourceFile name. ExifTool returns SourceFile as the path it
        // received. We zip by index when the count matches; otherwise look up
        // by path.
        let mut out: Vec<FileMetadata> = Vec::with_capacity(paths.len());
        if entries.len() == paths.len() {
            for e in &entries {
                out.push(e.to_metadata());
            }
        } else {
            // Build a lookup by SourceFile.
            let mut by_path: std::collections::HashMap<&str, &RawEntry> =
                std::collections::HashMap::new();
            for e in &entries {
                if let Some(p) = e.source_file.as_deref() {
                    by_path.insert(p, e);
                }
            }
            for p in paths {
                let key = p.to_string_lossy();
                let m = by_path
                    .get(key.as_ref())
                    .map(|e| e.to_metadata())
                    .unwrap_or_default();
                out.push(m);
            }
        }
        Ok(out)
    }

    /// Read a single file (convenience).
    pub async fn read_one(&self, path: &Path) -> Result<FileMetadata> {
        let v = self.read_batch(&[path.to_path_buf()]).await?;
        Ok(v.into_iter().next().unwrap_or_default())
    }

    /// Cleanly shut the subprocess down.
    pub async fn shutdown(self) -> Result<()> {
        let mut g = self.inner.lock().await;
        let _ = g.stdin.write_all(b"-stay_open\nFalse\n").await;
        let _ = g.stdin.flush().await;
        // give it a moment, then ensure it's gone
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), g.child.wait()).await;
        let _ = g.child.kill().await;
        Ok(())
    }
}

/// Raw JSON entry as returned by exiftool. All fields optional.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RawEntry {
    #[serde(rename = "SourceFile")]
    source_file: Option<String>,
    #[serde(rename = "DateTimeOriginal")]
    date_time_original: Option<String>,
    #[serde(rename = "CreateDate")]
    create_date: Option<String>,
    #[serde(rename = "MediaCreateDate")]
    media_create_date: Option<String>,
    #[serde(rename = "ModifyDate")]
    modify_date: Option<String>,
    #[serde(rename = "Make")]
    make: Option<String>,
    #[serde(rename = "Model")]
    model: Option<String>,
}

impl RawEntry {
    fn to_metadata(&self) -> FileMetadata {
        FileMetadata {
            date_time_original: self.date_time_original.as_deref().and_then(parse_dt),
            create_date: self.create_date.as_deref().and_then(parse_dt),
            media_create_date: self.media_create_date.as_deref().and_then(parse_dt),
            modify_date: self.modify_date.as_deref().and_then(parse_dt),
            make: self.make.clone(),
            model: self.model.clone(),
        }
    }
}

fn parse_dt(s: &str) -> Option<chrono::NaiveDateTime> {
    // Our `-d` format is `%Y-%m-%d %H:%M:%S`. Be defensive about the original
    // EXIF colons format too in case `-d` was ignored for some reason.
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt);
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y:%m:%d %H:%M:%S") {
        return Some(dt);
    }
    // Sometimes there's a sub-second or timezone suffix; try truncating.
    if s.len() >= 19 {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&s[..19], "%Y-%m-%d %H:%M:%S") {
            return Some(dt);
        }
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&s[..19], "%Y:%m:%d %H:%M:%S") {
            return Some(dt);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_iso_dt() {
        let dt = parse_dt("2026-05-03 13:45:07").unwrap();
        assert_eq!(dt.to_string(), "2026-05-03 13:45:07");
    }

    #[test]
    fn parse_exif_colons_dt() {
        let dt = parse_dt("2026:05:03 13:45:07").unwrap();
        assert_eq!(dt.to_string(), "2026-05-03 13:45:07");
    }

    #[test]
    fn parse_with_subseconds() {
        let dt = parse_dt("2026:05:03 13:45:07.123").unwrap();
        assert_eq!(dt.to_string(), "2026-05-03 13:45:07");
    }
}
