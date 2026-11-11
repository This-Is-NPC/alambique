//! The exclusive lock on a data directory (design §1).
//!
//! `File::try_lock` is `flock(2)` on Linux: the lock belongs to the open file
//! description, so a second open of the same file conflicts even inside this
//! process, and the kernel releases it when the process dies.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;
use std::sync::Mutex;

use rustler::{Atom, Encoder, Env, ResourceArc, Term};

mod atoms {
    rustler::atoms! { ok, error, in_use, io }
}

/// The open lock file; `None` once released.
pub struct DirLock(Mutex<Option<File>>);

#[rustler::resource_impl]
impl rustler::Resource for DirLock {}

/// Takes the lock on `path`, creating the file. `Ok(None)` when another
/// holder has it.
pub(crate) fn acquire(path: &Path) -> std::io::Result<Option<File>> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn dir_lock<'a>(env: Env<'a>, path: String) -> Term<'a> {
    match acquire(Path::new(&path)) {
        Ok(Some(file)) => {
            let res = ResourceArc::new(DirLock(Mutex::new(Some(file))));
            (atoms::ok(), res).encode(env)
        }
        Ok(None) => (atoms::error(), atoms::in_use()).encode(env),
        Err(e) => (atoms::error(), (atoms::io(), e.to_string())).encode(env),
    }
}

/// Releases the lock now; the resource's drop would do it later.
#[rustler::nif]
fn dir_unlock(lock: ResourceArc<DirLock>) -> Atom {
    drop(lock.0.lock().unwrap_or_else(|p| p.into_inner()).take());
    atoms::ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_open_conflicts_until_the_first_drops() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock");
        let first = acquire(&path).unwrap().expect("free");
        assert!(acquire(&path).unwrap().is_none());
        drop(first);
        assert!(acquire(&path).unwrap().is_some());
    }
}
