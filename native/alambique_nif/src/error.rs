//! Error type shared by the byte-level modules.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Malformed or inconsistent input (LTX, WAL, arguments).
    Invalid(String),
    /// The database checksum does not match what an LTX file expects.
    PreApplyMismatch { expected: u64, got: u64 },
    /// Applying an LTX file produced a checksum other than the trailer's.
    PostApplyMismatch { expected: u64, got: u64 },
    /// A filesystem failure.
    Io(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

pub fn invalid<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Invalid(msg.into()))
}
