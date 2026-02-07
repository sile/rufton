#[derive(Debug)]
pub struct Error {
    pub reason: String,
    pub backtrace: std::backtrace::Backtrace,
}

impl Error {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            backtrace: std::backtrace::Backtrace::capture(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)?;

        if self.backtrace.status() == std::backtrace::BacktraceStatus::Captured {
            write!(f, "\n\nBacktrace:\n{}", self.backtrace)?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Self::new(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::new(err.to_string())
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(err: std::net::AddrParseError) -> Self {
        Self::new(err.to_string())
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Self::new(err.to_string())
    }
}

impl From<nojson::JsonParseError> for Error {
    fn from(err: nojson::JsonParseError) -> Self {
        Self::new(err.to_string())
    }
}
