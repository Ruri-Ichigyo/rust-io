use std::fmt;
use std::path::PathBuf;

use url::Url;

use crate::error::{IoError, IoResult};

/// Transport selected by a URI's scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scheme {
    Local,
    S3,
    Http,
}

impl fmt::Display for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Scheme::Local => "file",
            Scheme::S3 => "s3",
            Scheme::Http => "http",
        })
    }
}

/// A parsed I/O location.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Uri {
    Local(PathBuf),
    S3 { bucket: String, key: String },
    Http(Url),
}

impl Uri {
    pub fn parse(input: &str) -> IoResult<Self> {
        if input.trim().is_empty() {
            return Err(IoError::InvalidUri(input.to_string()));
        }

        if input
            .get(..5)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("s3://"))
        {
            let rest = &input[5..];
            let (bucket, key) = rest
                .split_once('/')
                .ok_or_else(|| IoError::InvalidUri(input.to_string()))?;
            if bucket.is_empty() || key.is_empty() {
                return Err(IoError::InvalidUri(input.to_string()));
            }
            return Ok(Uri::S3 {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        if input
            .get(..7)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://"))
            || input
                .get(..8)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
        {
            let url = Url::parse(input).map_err(|_| IoError::InvalidUri(input.to_string()))?;
            return Ok(Uri::Http(url));
        }

        if input
            .get(..7)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("file://"))
        {
            let url = Url::parse(input).map_err(|_| IoError::InvalidUri(input.to_string()))?;
            let path = url
                .to_file_path()
                .map_err(|_| IoError::InvalidUri(input.to_string()))?;
            return Ok(Uri::Local(path));
        }

        if input.contains("://") {
            return Err(IoError::InvalidUri(input.to_string()));
        }

        Ok(Uri::Local(PathBuf::from(input)))
    }

    pub fn scheme(&self) -> Scheme {
        match self {
            Uri::Local(_) => Scheme::Local,
            Uri::S3 { .. } => Scheme::S3,
            Uri::Http(_) => Scheme::Http,
        }
    }
}

impl fmt::Display for Uri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Uri::Local(path) => write!(f, "{}", path.display()),
            Uri::S3 { bucket, key } => write!(f, "s3://{bucket}/{key}"),
            Uri::Http(url) => f.write_str(url.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_and_file_paths() {
        assert_eq!(
            Uri::parse("/tmp/a b.txt").unwrap(),
            Uri::Local(PathBuf::from("/tmp/a b.txt"))
        );
        assert_eq!(
            Uri::parse("relative/a.txt").unwrap(),
            Uri::Local(PathBuf::from("relative/a.txt"))
        );
        assert_eq!(
            Uri::parse("file:///tmp/a%20b.txt").unwrap(),
            Uri::Local(PathBuf::from("/tmp/a b.txt"))
        );
    }

    #[test]
    fn parses_s3_and_http() {
        assert_eq!(
            Uri::parse("s3://bucket/key/path").unwrap(),
            Uri::S3 {
                bucket: "bucket".into(),
                key: "key/path".into()
            }
        );
        assert_eq!(
            Uri::parse("https://example.com/a.pdf").unwrap(),
            Uri::Http(Url::parse("https://example.com/a.pdf").unwrap())
        );
    }

    #[test]
    fn rejects_invalid_locations() {
        for input in ["", "s3://", "s3://bucket", "ftp://example.com/x"] {
            let error = Uri::parse(input).unwrap_err();
            assert!(
                matches!(error, IoError::InvalidUri(_)),
                "{input}: {error:?}"
            );
            assert!(error.to_string().contains(input) || input.is_empty());
        }
    }

    #[test]
    fn display_round_trips_every_accepted_form() {
        for input in [
            "/tmp/a b.txt",
            "relative/a.txt",
            "s3://bucket/key/path",
            "https://example.com/a.pdf?x=1",
        ] {
            let uri = Uri::parse(input).unwrap();
            assert_eq!(Uri::parse(&uri.to_string()).unwrap(), uri);
        }
    }

    #[test]
    fn parses_schemes_case_insensitively() {
        assert!(matches!(
            Uri::parse("S3://bucket/key").unwrap(),
            Uri::S3 { .. }
        ));
        assert!(matches!(
            Uri::parse("HTTP://example.com/x").unwrap(),
            Uri::Http(_)
        ));
        assert!(matches!(
            Uri::parse("FILE:///tmp/x").unwrap(),
            Uri::Local(_)
        ));
    }

    #[test]
    fn rejects_hostile_and_malformed_locations() {
        for input in [
            "file://remotehost/tmp/x",
            "s3://bucket",
            "s3://bucket/",
            "s3:///key",
            "http://",
            "https://",
            "a://b",
            "://",
        ] {
            assert!(
                matches!(Uri::parse(input), Err(IoError::InvalidUri(_))),
                "{input} should be rejected"
            );
        }
    }

    #[test]
    fn scheme_display_is_stable() {
        assert_eq!(Scheme::Local.to_string(), "file");
        assert_eq!(Scheme::S3.to_string(), "s3");
        assert_eq!(Scheme::Http.to_string(), "http");
    }
}
