//! Copy-on-write file copies (`ioctl(FICLONE)`), for refreshing a query
//! replica without copying the database.

use std::fs::OpenOptions;
use std::path::Path;

use crate::error::Result;

/// Makes `dst` (an existing, empty file) share the data blocks of `src`.
/// `Ok(false)` when the filesystem or platform cannot (the caller copies).
pub fn clone_file(src: &Path, dst: &Path) -> Result<bool> {
    let src = OpenOptions::new().read(true).open(src)?;
    let dst = OpenOptions::new().write(true).open(dst)?;
    Ok(ficlone(&src, &dst))
}

#[cfg(target_os = "linux")]
fn ficlone(src: &std::fs::File, dst: &std::fs::File) -> bool {
    use std::os::fd::AsRawFd;

    extern "C" {
        fn ioctl(fd: i32, request: u64, ...) -> i32;
    }
    // _IOW(0x94, 9, int)
    const FICLONE: u64 = 0x4004_9409;
    // SAFETY: both descriptors are open for the whole call, and FICLONE takes
    // the source descriptor by value.
    unsafe { ioctl(dst.as_raw_fd(), FICLONE, src.as_raw_fd()) == 0 }
}

#[cfg(not(target_os = "linux"))]
fn ficlone(_src: &std::fs::File, _dst: &std::fs::File) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn a_clone_has_the_content_or_is_declined() {
        let dir = tempfile::tempdir().unwrap();
        let (src, dst) = (dir.path().join("src"), dir.path().join("dst"));
        fs::write(&src, vec![7u8; 8192]).unwrap();
        fs::write(&dst, b"").unwrap();

        if clone_file(&src, &dst).unwrap() {
            assert_eq!(fs::read(&dst).unwrap(), fs::read(&src).unwrap());
            // Writing the clone leaves the source alone.
            fs::write(&dst, b"changed").unwrap();
            assert_eq!(fs::read(&src).unwrap(), vec![7u8; 8192]);
        } else {
            assert_eq!(fs::read(&dst).unwrap(), b"");
        }
    }

    #[test]
    fn a_missing_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("dst");
        fs::write(&dst, b"").unwrap();
        assert!(clone_file(&dir.path().join("none"), &dst).is_err());
    }
}
