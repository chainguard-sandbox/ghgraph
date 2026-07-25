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
}

impl Error {
    pub fn user(message: impl Into<String>) -> Self {
        Error {
            code: Code::UserInput,
            message: message.into(),
        }
    }

    pub fn config(message: impl Into<String>) -> Self {
        Error {
            code: Code::Configuration,
            message: message.into(),
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Error {
            code: Code::Transient,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Error {
            code: Code::Internal,
            message: message.into(),
        }
    }

    pub fn envelope(&self) -> String {
        serde_json::json!({
            "error": { "code": self.code.as_str(), "message": self.message }
        })
        .to_string()
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
