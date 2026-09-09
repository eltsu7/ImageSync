//! Path template rendering.
//!
//! Templates contain literal text and tokens. Tokens are substituted with
//! fields of a [`chrono::NaiveDateTime`]. Forward slashes in the template are
//! preserved as path separators (regardless of host OS — they are translated
//! to native separators when the result is joined under a root).
//!
//! Tokens:
//! - `{yyyy}` 4-digit year
//! - `{yy}`   2-digit year
//! - `{mm}`   2-digit month (01..12)
//! - `{dd}`   2-digit day of month (01..31)
//! - `{month}` lowercase month name (`january`)
//! - `{Month}` capitalized month name (`January`)
//! - `{HH}`, `{MM}`, `{SS}` 2-digit hour / minute / second
//!
//! Unknown tokens cause a parse error.

use chrono::{Datelike, NaiveDateTime, Timelike};

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Literal(String),
    Token(Token),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    Yyyy,
    Yy,
    Mm,
    Dd,
    MonthLower,
    MonthCap,
    HH,
    MM,
    SS,
}

impl Token {
    fn from_name(name: &str) -> Option<Token> {
        match name {
            "yyyy" => Some(Token::Yyyy),
            "yy" => Some(Token::Yy),
            "mm" => Some(Token::Mm),
            "dd" => Some(Token::Dd),
            "month" => Some(Token::MonthLower),
            "Month" => Some(Token::MonthCap),
            "HH" => Some(Token::HH),
            "MM" => Some(Token::MM),
            "SS" => Some(Token::SS),
            _ => None,
        }
    }

    fn render(self, dt: &NaiveDateTime) -> String {
        match self {
            Token::Yyyy => format!("{:04}", dt.year()),
            Token::Yy => format!("{:02}", dt.year().rem_euclid(100)),
            Token::Mm => format!("{:02}", dt.month()),
            Token::Dd => format!("{:02}", dt.day()),
            Token::MonthLower => month_name(dt.month()).to_lowercase(),
            Token::MonthCap => month_name(dt.month()).to_string(),
            Token::HH => format!("{:02}", dt.hour()),
            Token::MM => format!("{:02}", dt.minute()),
            Token::SS => format!("{:02}", dt.second()),
        }
    }
}

fn month_name(m: u32) -> &'static str {
    match m {
        1 => "January",
        2 => "February",
        3 => "March",
        4 => "April",
        5 => "May",
        6 => "June",
        7 => "July",
        8 => "August",
        9 => "September",
        10 => "October",
        11 => "November",
        12 => "December",
        _ => "Unknown",
    }
}

#[derive(Debug, Clone)]
pub struct PathTemplate {
    raw: String,
    segments: Vec<Segment>,
}

impl PathTemplate {
    pub fn parse(template: &str) -> Result<Self> {
        let mut segments = Vec::new();
        let mut buf = String::new();
        let mut chars = template.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '{' => {
                    if !buf.is_empty() {
                        segments.push(Segment::Literal(std::mem::take(&mut buf)));
                    }
                    let mut name = String::new();
                    let mut closed = false;
                    while let Some(&nc) = chars.peek() {
                        chars.next();
                        if nc == '}' {
                            closed = true;
                            break;
                        }
                        name.push(nc);
                    }
                    if !closed {
                        return Err(Error::Template {
                            template: template.to_string(),
                            message: format!("unterminated `{{{name}` token"),
                        });
                    }
                    let tok = Token::from_name(&name).ok_or_else(|| Error::Template {
                        template: template.to_string(),
                        message: format!(
                            "unknown token `{{{name}}}` (valid: yyyy, yy, mm, dd, month, Month, HH, MM, SS)"
                        ),
                    })?;
                    segments.push(Segment::Token(tok));
                }
                '}' => {
                    return Err(Error::Template {
                        template: template.to_string(),
                        message:
                            "stray `}` (escape literal `}` is not supported; just don't use it)"
                                .into(),
                    });
                }
                _ => buf.push(c),
            }
        }
        if !buf.is_empty() {
            segments.push(Segment::Literal(buf));
        }

        // Reject absolute templates and parent-traversal.
        if template.starts_with('/') || template.starts_with('\\') {
            return Err(Error::Template {
                template: template.to_string(),
                message: "template must be relative (no leading `/`)".into(),
            });
        }
        if template.split('/').any(|p| p == "..") {
            return Err(Error::Template {
                template: template.to_string(),
                message: "template must not contain `..` segments".into(),
            });
        }

        Ok(Self {
            raw: template.to_string(),
            segments,
        })
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Render the template with the given datetime. Returns a relative path
    /// using `/` separators; callers should join under a root using
    /// [`std::path::Path::join`] which handles native separators.
    pub fn render(&self, dt: &NaiveDateTime) -> String {
        let mut out = String::new();
        for seg in &self.segments {
            match seg {
                Segment::Literal(s) => out.push_str(s),
                Segment::Token(t) => out.push_str(&t.render(dt)),
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn dt(y: i32, m: u32, d: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(13, 45, 7)
            .unwrap()
    }

    #[test]
    fn default_template() {
        let t = PathTemplate::parse("{yyyy}/{yyyy}-{mm}-{dd}").unwrap();
        assert_eq!(t.render(&dt(2026, 5, 3)), "2026/2026-05-03");
    }

    #[test]
    fn yyyy_mm_dd() {
        let t = PathTemplate::parse("{yyyy}/{mm}/{dd}").unwrap();
        assert_eq!(t.render(&dt(2026, 1, 9)), "2026/01/09");
    }

    #[test]
    fn month_names() {
        let t = PathTemplate::parse("{yyyy}/{mm}-{Month}").unwrap();
        assert_eq!(t.render(&dt(2026, 5, 3)), "2026/05-May");
        let t = PathTemplate::parse("{yyyy}/{mm}-{month}").unwrap();
        assert_eq!(t.render(&dt(2026, 5, 3)), "2026/05-may");
    }

    #[test]
    fn time_tokens() {
        let t = PathTemplate::parse("{yyyy}-{mm}-{dd}_{HH}{MM}{SS}").unwrap();
        assert_eq!(t.render(&dt(2026, 5, 3)), "2026-05-03_134507");
    }

    #[test]
    fn rejects_unknown_token() {
        let err = PathTemplate::parse("{yyyy}/{nope}").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown token"), "{msg}");
    }

    #[test]
    fn rejects_unterminated_token() {
        assert!(PathTemplate::parse("{yyyy/{mm}").is_err());
    }

    #[test]
    fn rejects_absolute() {
        assert!(PathTemplate::parse("/{yyyy}").is_err());
    }

    #[test]
    fn rejects_parent_traversal() {
        assert!(PathTemplate::parse("{yyyy}/../foo").is_err());
    }

    #[test]
    fn literal_text_preserved() {
        let t = PathTemplate::parse("photos-{yyyy}/day-{dd}").unwrap();
        assert_eq!(t.render(&dt(2026, 5, 3)), "photos-2026/day-03");
    }
}
