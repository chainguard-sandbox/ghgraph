use std::fmt;

/// Error class for the JSON error envelope. Mirrors the exit-code contract:
/// every failure prints `{"error":{"code":...,"message":...}}` on stdout and
/// exits 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    UserInput,
    Configuration,
    Transient,
    Internal,
}

impl Code {
    fn as_str(self) -> &'static str {
        match self {
            Code::UserInput => "USER_INPUT",
            Code::Configuration => "CONFIGURATION",
            Code::Transient => "TRANSIENT",
            Code::Internal => "INTERNAL",
        }
    }
}

#[derive(Debug)]
pub struct Error {
    pub code: Code,
    pub message: String,
    /// RFC 3339 time after which a retry can succeed, when the failing
    /// call learned one (gh's rateLimit.resetAt — API data, passed
    /// through, never computed locally). Serialized into the envelope as
    /// `retry_after` when present; the freeze batch's TRANSIENT
    /// disclosure (ROADMAP milestone 3).
    pub retry_after: Option<String>,
}

impl Error {
    pub fn user(message: impl Into<String>) -> Self {
        Error {
            code: Code::UserInput,
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn config(message: impl Into<String>) -> Self {
        Error {
            code: Code::Configuration,
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Error {
            code: Code::Transient,
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Error {
            code: Code::Internal,
            message: message.into(),
            retry_after: None,
        }
    }

    /// Attach the retry bound a failing call learned, when it learned one.
    /// Builder-style so the classification constructors stay the one place
    /// a code is chosen.
    pub fn with_retry_after(mut self, retry_after: Option<String>) -> Self {
        self.retry_after = retry_after;
        self
    }

    pub fn envelope(&self) -> String {
        let mut inner = serde_json::json!({
            "code": self.code.as_str(), "message": self.message
        });
        if let Some(at) = &self.retry_after {
            inner["retry_after"] = serde_json::json!(at);
        }
        serde_json::json!({ "error": inner }).to_string()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)
    }
}

// There are deliberately NO blanket From<rusqlite::Error> / From<serde_json
// ::Error> / From<io::Error> impls: the code names the actor who can fix the
// failure, and a blanket From launders everything into INTERNAL ("file a
// ghgraph bug"). The counterexample that killed them: one PR with a deleted
// author (author: null) × strict deserialization × From<serde_json::Error>
// = a permanent repo-wide INTERNAL abort from ordinary data. The compiler
// now forces classification at each call site: a user's SQL typo is
// USER_INPUT, ENOSPC is CONFIGURATION with the disposable-cache remedy,
// malformed gh output is TRANSIENT.

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    /// The envelope carries `retry_after` exactly when a failing call
    /// learned one — absent otherwise, never null (an absent bound and an
    /// unknown bound are the same thing to a consumer: retry blind).
    #[test]
    fn envelope_carries_retry_after_only_when_learned() {
        let e = Error::transient("rate limited");
        assert_eq!(
            e.envelope(),
            r#"{"error":{"code":"TRANSIENT","message":"rate limited"}}"#
        );
        let e = e.with_retry_after(Some("2026-08-01T00:00:00Z".into()));
        assert_eq!(
            e.envelope(),
            r#"{"error":{"code":"TRANSIENT","message":"rate limited","retry_after":"2026-08-01T00:00:00Z"}}"#
        );
        assert_eq!(
            Error::user("x").with_retry_after(None).envelope(),
            r#"{"error":{"code":"USER_INPUT","message":"x"}}"#
        );
    }
}
