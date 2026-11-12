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
    /// The disk or the quota is full (`ENOSPC`, `EDQUOT`).
    DiskFull,
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        match e.kind() {
            std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded => Error::DiskFull,
            _ => Error::Io(e.to_string()),
        }
    }
}

pub fn invalid<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Invalid(msg.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn a_full_disk_is_told_by_its_code_not_its_text() {
        // ENOSPC and EDQUOT, whatever the language of the message.
        for code in [28, 122] {
            assert_eq!(
                Error::from(IoError::from_raw_os_error(code)),
                Error::DiskFull
            );
        }
        assert_eq!(
            Error::from(IoError::from(ErrorKind::StorageFull)),
            Error::DiskFull
        );
        assert!(matches!(
            Error::from(IoError::from_raw_os_error(13)),
            Error::Io(_)
        ));
    }
}
