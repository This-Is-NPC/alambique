//! Database files that a cell's connection holds, and the wal-index header
//! that tells whether anything else wrote to them (design §15).
//!
//! Prevention: a process-wide table of the (device, inode) of every main
//! database file that a gated read-write connection has open. Another
//! read-write open of such a file, through any name or hard link, is refused
//! (`sqlite_open` and the `x_open` of the VFS).
//!
//! Detection: the first 96 bytes of the shared-memory wal-index hold two
//! copies of the header SQLite keeps current at every commit and every
//! restart of the WAL. `Watch` compares it with what the cell's own
//! operations account for.

use std::collections::BTreeSet;
use std::ffi::c_void;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{fence, Ordering};
use std::sync::Mutex;

use libsqlite3_sys as ffi;

/// Device and inode of a main database file.
pub type FileId = (u64, u64);

static HELD: Mutex<BTreeSet<FileId>> = Mutex::new(BTreeSet::new());

thread_local! {
    /// Set by the VFS when it refused an open because the file is held, so the
    /// thread that called `sqlite3_open_v2` can tell why it failed.
    static REFUSED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(feature = "test-panic")]
thread_local! {
    /// Test builds: the opens of this thread are not refused, as the opens of
    /// another process are not.
    static BYPASS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Test builds: runs `f` as a writer the table cannot see.
#[cfg(feature = "test-panic")]
pub fn bypassing<T>(f: impl FnOnce() -> T) -> T {
    BYPASS.with(|b| b.set(true));
    let out = f();
    BYPASS.with(|b| b.set(false));
    out
}

#[cfg(feature = "test-panic")]
pub fn bypassed() -> bool {
    BYPASS.with(|b| b.get())
}

#[cfg(not(feature = "test-panic"))]
pub fn bypassed() -> bool {
    false
}

/// The identity of the file at `path`; `None` when it does not exist.
pub fn file_id(path: &Path) -> Option<FileId> {
    std::fs::metadata(path).ok().map(|m| (m.dev(), m.ino()))
}

/// Whether a cell's connection holds the file.
pub fn is_held(id: FileId) -> bool {
    HELD.lock().unwrap_or_else(|e| e.into_inner()).contains(&id)
}

/// Takes the file; `false` when it is held already.
pub fn hold(id: FileId) -> bool {
    HELD.lock().unwrap_or_else(|e| e.into_inner()).insert(id)
}

pub fn release(id: FileId) {
    HELD.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
}

/// The check of the VFS: whether a read-write open of the main database file
/// `path` must be refused. Notes the refusal for `take_refused`.
pub fn refuse_open(path: &Path) -> bool {
    let refused = !bypassed() && file_id(path).is_some_and(is_held);
    if refused {
        REFUSED.with(|r| r.set(true));
    }
    refused
}

/// Whether `ATTACH` of `spec` (a file name or a `file:` URI) opens a held
/// file read-write. Escapes in a URI are not decoded: those are left to the
/// VFS.
pub fn attach_refused(spec: &str) -> bool {
    let (path, query) = match spec.strip_prefix("file:") {
        Some(rest) => rest.split_once('?').unwrap_or((rest, "")),
        None => (spec, ""),
    };
    if query
        .split('&')
        .any(|q| q == "mode=ro" || q == "immutable=1")
    {
        return false;
    }
    !bypassed() && file_id(Path::new(path)).is_some_and(is_held)
}

pub fn clear_refused() {
    REFUSED.with(|r| r.set(false));
}

/// Whether the VFS refused an open on this thread since `clear_refused`.
pub fn take_refused() -> bool {
    REFUSED.with(|r| r.replace(false))
}

/// What the cell's connection compares: `iChange`, `mxFrame`, `nPage` and the
/// two salts of the wal-index header.
pub type Ident = [u32; 5];

const HDR_WORDS: usize = 12;

fn ident(h: &[u32; HDR_WORDS]) -> Ident {
    [h[2], h[4], h[5], h[8], h[9]]
}

/// Reads the header of the wal-index of `db`. `None` when it cannot be read
/// consistently: no shared memory mapped, or copies that never agree.
///
/// The header is read as SQLite reads it (`walIndexTryHdr`): two copies, the
/// second written after the first, equal. While the connection holds the WAL
/// write lock nothing writes the header, so a read inside `BEGIN IMMEDIATE`
/// is exact; outside it the copies may differ for an instant and the read
/// retries.
///
/// # Safety
/// `db` is a live connection.
pub unsafe fn read_header(db: *mut ffi::sqlite3) -> Option<[u32; HDR_WORDS]> {
    let mut file: *mut ffi::sqlite3_file = ptr::null_mut();
    let mut region: *mut c_void = ptr::null_mut();
    // SAFETY: by the contract; the database mutex keeps another thread from
    // using the connection's file while the mapping is asked for.
    let (rc, mapped) = unsafe {
        let mutex = ffi::sqlite3_db_mutex(db);
        ffi::sqlite3_mutex_enter(mutex);
        let rc = ffi::sqlite3_file_control(
            db,
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_FILE_POINTER,
            &mut file as *mut _ as *mut c_void,
        );
        let mut mapped = ffi::SQLITE_ERROR;
        if rc == ffi::SQLITE_OK && !file.is_null() && !(*file).pMethods.is_null() {
            let methods = &*(*file).pMethods;
            if methods.iVersion >= 2 {
                if let Some(shm_map) = methods.xShmMap {
                    // Region 0, without creating it: it exists while the WAL
                    // of this connection is open.
                    mapped = shm_map(file, 0, 32768, 0, &mut region);
                }
            }
        }
        ffi::sqlite3_mutex_leave(mutex);
        (rc, mapped)
    };
    if rc != ffi::SQLITE_OK || mapped != ffi::SQLITE_OK || region.is_null() {
        return None;
    }
    let base = region as *const u32;
    for attempt in 0..64 {
        // SAFETY: the region holds at least the two 48-byte headers and stays
        // mapped while the WAL of the connection is open.
        let (a, b) = unsafe {
            let a = copy(base);
            fence(Ordering::Acquire);
            let b = copy(base.add(HDR_WORDS));
            (a, b)
        };
        // `isInit` is the first byte after `iChange`.
        if a == b && a[3].to_ne_bytes()[0] != 0 {
            return Some(a);
        }
        if attempt < 16 {
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
    }
    None
}

/// # Safety
/// `p` points to `HDR_WORDS` readable words.
unsafe fn copy(p: *const u32) -> [u32; HDR_WORDS] {
    let mut out = [0u32; HDR_WORDS];
    for (i, w) in out.iter_mut().enumerate() {
        // SAFETY: by the contract; another process may be writing, which is
        // why the read is volatile and the caller compares two copies.
        *w = unsafe { p.add(i).read_volatile() };
    }
    out
}

/// What a check found.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verdict {
    /// The header is what the connection's own operations left.
    Same,
    /// Something else committed: its frames follow in the same WAL.
    Advanced,
    /// The WAL restarted, or the header cannot be read: frames may be gone.
    Restarted,
}

#[derive(Default)]
struct State {
    /// The header after the latest own operation; `None` before the baseline.
    recorded: Option<Ident>,
    /// Own commits when `recorded` was taken.
    commits_at: u64,
    /// The header the latest check saw, for `accept`.
    seen: Option<Ident>,
    /// An own operation left a header it does not account for.
    tainted: bool,
}

/// The wal-index header of the connection of a cell, against what its own
/// commits and checkpoints account for. `commits` and `frames` are the WAL
/// hook's counters: commits made on the connection, and frames in the WAL
/// after the latest of them.
#[derive(Default)]
pub struct Watch {
    state: Mutex<State>,
}

impl Watch {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Takes the header as it is as the baseline.
    pub fn baseline(&self, header: Option<[u32; HDR_WORDS]>, commits: u64) {
        let mut s = self.lock();
        s.recorded = header.as_ref().map(ident);
        s.commits_at = commits;
        s.seen = None;
        s.tainted = false;
    }

    /// Compares the header with the record. Without a baseline yet, the
    /// header becomes it.
    pub fn check(&self, header: Option<[u32; HDR_WORDS]>, commits: u64) -> Verdict {
        let Some(header) = header else {
            return Verdict::Restarted;
        };
        let id = ident(&header);
        let mut s = self.lock();
        let Some(rec) = s.recorded else {
            s.recorded = Some(id);
            s.commits_at = commits;
            return Verdict::Same;
        };
        if !s.tainted && id == rec {
            return Verdict::Same;
        }
        s.seen = Some(id);
        if id[3..] == rec[3..] && id[1] >= rec[1] {
            Verdict::Advanced
        } else {
            Verdict::Restarted
        }
    }

    /// Takes what the latest check saw as accounted for (its frames were
    /// replicated).
    pub fn accept(&self, commits: u64) {
        let mut s = self.lock();
        if let Some(id) = s.seen.take() {
            s.recorded = Some(id);
            s.commits_at = commits;
            s.tainted = false;
        }
    }

    /// After an own commit or checkpoint: the header must be what they
    /// leave, `iChange` up by the commits made and `mxFrame` the frames the
    /// hook reported. Otherwise something slipped in after the lock was
    /// released, and the record keeps its old value so the next check sees
    /// it. Returns whether the header is accounted for.
    pub fn settle(&self, header: Option<[u32; HDR_WORDS]>, commits: u64, frames: u64) -> bool {
        let mut s = self.lock();
        let Some(rec) = s.recorded else {
            return true;
        };
        if s.tainted {
            return false;
        }
        let ok = header.as_ref().map(ident).filter(|id| {
            id[0] == rec[0].wrapping_add(commits.wrapping_sub(s.commits_at) as u32)
                && u64::from(id[1]) == frames
        });
        match ok {
            Some(id) => {
                s.recorded = Some(id);
                s.commits_at = commits;
                true
            }
            None => {
                s.tainted = true;
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(change: u32, frames: u32, salt0: u32) -> Option<[u32; HDR_WORDS]> {
        let mut h = [0u32; HDR_WORDS];
        h[2] = change;
        h[3] = u32::from_ne_bytes([1, 0, 0, 0]);
        h[4] = frames;
        h[5] = 7;
        h[8] = salt0;
        h[9] = 99;
        Some(h)
    }

    #[test]
    fn the_table_holds_a_file_once_and_gives_it_back() {
        let id = (u64::MAX, 424_242);
        assert!(hold(id));
        assert!(is_held(id));
        assert!(!hold(id));
        release(id);
        assert!(!is_held(id));
    }

    #[test]
    fn a_header_that_is_not_the_record_is_foreign() {
        let w = Watch::default();
        w.baseline(header(5, 3, 1), 0);
        assert_eq!(w.check(header(5, 3, 1), 0), Verdict::Same);
        assert_eq!(w.check(header(6, 4, 1), 0), Verdict::Advanced);
        // A restart that leaves the same count of frames is still a restart.
        assert_eq!(w.check(header(6, 3, 2), 0), Verdict::Restarted);
        assert_eq!(w.check(None, 0), Verdict::Restarted);
    }

    #[test]
    fn an_own_commit_is_accounted_for_and_a_slipped_in_one_is_not() {
        let w = Watch::default();
        w.baseline(header(5, 3, 1), 10);
        assert!(w.settle(header(6, 4, 1), 11, 4));
        assert_eq!(w.check(header(6, 4, 1), 11), Verdict::Same);
        // Two commits in the header, one counted: another writer committed.
        assert!(!w.settle(header(8, 6, 1), 12, 5));
        assert_eq!(w.check(header(8, 6, 1), 12), Verdict::Advanced);
        w.accept(12);
        assert_eq!(w.check(header(8, 6, 1), 12), Verdict::Same);
    }

    #[test]
    fn an_own_checkpoint_leaves_an_empty_wal() {
        let w = Watch::default();
        w.baseline(header(5, 3, 1), 4);
        assert!(w.settle(header(5, 0, 2), 4, 0));
        let foreign = Watch::default();
        foreign.baseline(header(5, 3, 1), 4);
        // A writer committed first: `iChange` moved.
        assert!(!foreign.settle(header(6, 0, 2), 4, 0));
        assert_eq!(foreign.check(header(6, 0, 2), 4), Verdict::Restarted);
    }
}
