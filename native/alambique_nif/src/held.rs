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

use std::collections::BTreeMap;
use std::ffi::{c_int, c_void, OsStr};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{fence, Ordering};
use std::sync::Mutex;

use libsqlite3_sys as ffi;

/// Device and inode of a main database file.
pub type FileId = (u64, u64);

/// Who may take a file back that its cell closed and kept held: the process
/// of the cell, as the term of its pid (an immediate, so it identifies it).
pub type Owner = u64;

/// A file in the table: held by an open connection, or kept by its cell while
/// the connection is closed, until `token` is dropped or the cell opens again.
enum Entry {
    Open,
    Kept { owner: Owner, token: u64 },
}

static HELD: Mutex<BTreeMap<FileId, Entry>> = Mutex::new(BTreeMap::new());

/// What `Stamp::of` read of a file: enough to tell that it is the file that
/// was closed, untouched. The inode number alone is not: closing every
/// descriptor lets the file system reuse it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    id: FileId,
    size: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}

impl Stamp {
    /// `None` when the file does not exist.
    pub fn of(path: &Path) -> Option<Stamp> {
        std::fs::metadata(path).ok().map(|m| Stamp {
            id: (m.dev(), m.ino()),
            size: m.size(),
            mtime: (m.mtime(), m.mtime_nsec()),
            ctime: (m.ctime(), m.ctime_nsec()),
        })
    }

    pub fn id(&self) -> FileId {
        self.id
    }
}

thread_local! {
    /// The file the calling thread reopens for its cell: its open is not
    /// refused (`permit`).
    static REOPENING: std::cell::Cell<Option<FileId>> = const { std::cell::Cell::new(None) };
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

/// Whether a cell's connection holds the file, or its cell keeps it.
pub fn is_held(id: FileId) -> bool {
    table().contains_key(&id)
}

fn table() -> std::sync::MutexGuard<'static, BTreeMap<FileId, Entry>> {
    HELD.lock().unwrap_or_else(|e| e.into_inner())
}

/// Takes the file; `false` when it is held already.
pub fn hold(id: FileId) -> bool {
    match table().entry(id) {
        std::collections::btree_map::Entry::Vacant(v) => {
            v.insert(Entry::Open);
            true
        }
        std::collections::btree_map::Entry::Occupied(_) => false,
    }
}

pub fn release(id: FileId) {
    table().remove(&id);
}

/// The cell `owner` closed its connection: the file stays in the table,
/// kept under `token`, so that no other read-write open gets in meanwhile.
pub fn keep(id: FileId, owner: Owner, token: u64) {
    table().insert(id, Entry::Kept { owner, token });
}

/// Gives back a file kept under `token`; a file taken again or kept under
/// another token is not touched.
pub fn drop_kept(id: FileId, token: u64) {
    let mut table = table();
    if matches!(table.get(&id), Some(Entry::Kept { token: t, .. }) if *t == token) {
        table.remove(&id);
    }
}

/// While the value lives, the calling thread's open of `path` is allowed:
/// the file is kept by `owner`, which opens it again.
pub struct Permit;

pub fn permit(path: &Path, owner: Owner) -> Option<Permit> {
    let id = file_id(path)?;
    match table().get(&id) {
        Some(Entry::Kept { owner: o, .. }) if *o == owner => {
            REOPENING.with(|r| r.set(Some(id)));
            Some(Permit)
        }
        _ => None,
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        REOPENING.with(|r| r.set(None));
    }
}

/// The reopened file is held by an open connection again. `false` when it is
/// not kept by `owner`.
pub fn reclaim(id: FileId, owner: Owner) -> bool {
    let mut table = table();
    match table.get(&id) {
        Some(Entry::Kept { owner: o, .. }) if *o == owner => {
            table.insert(id, Entry::Open);
            true
        }
        _ => false,
    }
}

/// The check of the VFS: whether a read-write open of the main database file
/// `path` must be refused. Notes the refusal for `take_refused`.
pub fn refuse_open(path: &Path) -> bool {
    let refused = !bypassed()
        && file_id(path).is_some_and(|id| is_held(id) && REOPENING.with(|r| r.get()) != Some(id));
    if refused {
        REFUSED.with(|r| r.set(true));
    }
    refused
}

/// Whether `ATTACH` of `spec` (a file name or a `file:` URI, as SQLite gets
/// it) opens a held file read-write. The name is read as `sqlite3ParseUri`
/// reads it and the file is the (device, inode) the path leads to, so
/// escapes, `//localhost/`, relative paths, symbolic and hard links name the
/// same file. A URI SQLite would not open is refused.
pub fn attach_refused(spec: &[u8]) -> bool {
    if bypassed() {
        return false;
    }
    match attach_target(spec) {
        Some((path, read_only)) => {
            !read_only && file_id(Path::new(OsStr::from_bytes(&path))).is_some_and(is_held)
        }
        None => true,
    }
}

fn hex(b: u8) -> Option<u8> {
    (b as char).to_digit(16).and_then(|d| u8::try_from(d).ok())
}

/// Decodes `%HH` escapes. `%00` ends the text, as in SQLite.
fn percent_decode(s: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while let Some(&c) = s.get(i) {
        let escaped = match (c, s.get(i + 1), s.get(i + 2)) {
            (b'%', Some(&h), Some(&l)) => hex(h).zip(hex(l)).map(|(h, l)| h << 4 | l),
            _ => None,
        };
        match escaped {
            Some(0) => break,
            Some(byte) => {
                out.push(byte);
                i += 3;
            }
            None => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

/// `bytes` cut at the first `sep`: what is before it and what is after it
/// (empty without one).
fn split_at_byte(bytes: &[u8], sep: u8) -> (&[u8], &[u8]) {
    match bytes.iter().position(|&b| b == sep) {
        Some(at) => {
            let (before, from_sep) = bytes.split_at(at);
            (before, from_sep.get(1..).unwrap_or_default())
        }
        None => (bytes, &[]),
    }
}

/// The path an `ATTACH` name opens and whether it opens it read-only
/// (`mode=ro`, the last `mode`, or a true `immutable`, the first). `None`
/// for a `file:` URI whose authority SQLite refuses.
fn attach_target(spec: &[u8]) -> Option<(Vec<u8>, bool)> {
    let Some(rest) = spec.strip_prefix(b"file:") else {
        return Some((spec.to_vec(), false));
    };
    let rest = match rest.strip_prefix(b"//") {
        Some(after) => {
            let end = after.iter().position(|&b| b == b'/').unwrap_or(after.len());
            let (authority, tail) = after.split_at(end);
            if !authority.is_empty() && authority != b"localhost" {
                return None;
            }
            tail
        }
        None => rest,
    };
    let rest = rest.split(|&b| b == b'#').next().unwrap_or_default();
    let (path, query) = split_at_byte(rest, b'?');
    let mut mode_ro = false;
    let mut immutable = None;
    for pair in query.split(|&b| b == b'&') {
        let (key, value) = split_at_byte(pair, b'=');
        match percent_decode(key).as_slice() {
            b"mode" => mode_ro = percent_decode(value) == b"ro",
            b"immutable" if immutable.is_none() => {
                let value = percent_decode(value).to_ascii_lowercase();
                immutable = Some(matches!(value.as_slice(), b"1" | b"yes" | b"on" | b"true"));
            }
            _ => {}
        }
    }
    Some((percent_decode(path), mode_ro || immutable == Some(true)))
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
    // SAFETY: by the contract.
    let region = unsafe { map_header(db) }?;
    let base = region as *const u32;
    for attempt in 0..64 {
        // SAFETY: the region holds at least the two 48-byte headers and stays
        // mapped while the WAL of the connection is open.
        let a = unsafe { copy(base) };
        fence(Ordering::Acquire);
        // SAFETY: as above.
        let second = unsafe { base.add(HDR_WORDS) };
        // SAFETY: as above.
        let b = unsafe { copy(second) };
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

/// Region 0 of the wal-index of `db`, without creating it: it exists while the
/// WAL of this connection is open.
///
/// # Safety
/// `db` is a live connection.
unsafe fn map_header(db: *mut ffi::sqlite3) -> Option<*mut c_void> {
    let mut file: *mut ffi::sqlite3_file = ptr::null_mut();
    let mut region: *mut c_void = ptr::null_mut();
    // SAFETY: by the contract.
    let mutex = unsafe { ffi::sqlite3_db_mutex(db) };
    // The database mutex keeps another thread from using the connection's
    // file while the mapping is asked for.
    // SAFETY: `mutex` is the live connection's own; it is left below.
    unsafe { ffi::sqlite3_mutex_enter(mutex) };
    // SAFETY: `db` is live and its mutex held; `file` is a valid out-pointer.
    let rc = unsafe {
        ffi::sqlite3_file_control(
            db,
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_FILE_POINTER,
            &mut file as *mut _ as *mut c_void,
        )
    };
    let mut mapped = ffi::SQLITE_ERROR;
    if rc == ffi::SQLITE_OK {
        // SAFETY: `file` is the connection's file, and its mutex is held.
        mapped = unsafe { map_region_zero(file, &mut region) };
    }
    // SAFETY: entered above.
    unsafe { ffi::sqlite3_mutex_leave(mutex) };
    (rc == ffi::SQLITE_OK && mapped == ffi::SQLITE_OK && !region.is_null()).then_some(region)
}

/// # Safety
/// `file` is null or an open file of a live connection.
unsafe fn map_region_zero(file: *mut ffi::sqlite3_file, region: &mut *mut c_void) -> c_int {
    // SAFETY: by the contract.
    let Some(f) = (unsafe { file.as_ref() }) else {
        return ffi::SQLITE_ERROR;
    };
    // SAFETY: an open file points at the methods of its VFS.
    let Some(methods) = (unsafe { f.pMethods.as_ref() }) else {
        return ffi::SQLITE_ERROR;
    };
    let Some(shm_map) = methods.xShmMap.filter(|_| methods.iVersion >= 2) else {
        return ffi::SQLITE_ERROR;
    };
    // SAFETY: the arguments are those of `xShmMap` for region 0 without
    // creating it.
    unsafe { shm_map(file, 0, 32768, 0, region) }
}

/// # Safety
/// `p` points to `HDR_WORDS` readable words.
unsafe fn copy(p: *const u32) -> [u32; HDR_WORDS] {
    let mut out = [0u32; HDR_WORDS];
    for (i, w) in out.iter_mut().enumerate() {
        // SAFETY: by the contract.
        let q = unsafe { p.add(i) };
        // SAFETY: another process may be writing, which is why the read is
        // volatile and the caller compares two copies.
        *w = unsafe { q.read_volatile() };
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
        // `iChange` is a 32-bit counter that wraps, so the own commits since the
        // baseline count modulo 2^32.
        let ok = header.as_ref().map(ident).filter(|id| {
            id[0] == rec[0].wrapping_add((commits.wrapping_sub(s.commits_at) & 0xFFFF_FFFF) as u32)
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
    fn a_kept_file_stays_held_until_its_cell_takes_it_or_the_keeper_drops() {
        let id = (u64::MAX, 424_243);
        assert!(hold(id));
        keep(id, 7, 1);
        assert!(is_held(id));
        assert!(!hold(id), "a kept file is not free");
        // Only the keeper takes it back, and only once.
        assert!(!reclaim(id, 8));
        assert!(reclaim(id, 7));
        assert!(!reclaim(id, 7));
        // The old keeper dropping later does not release the new hold.
        drop_kept(id, 1);
        assert!(is_held(id));
        release(id);

        // A keeper dropped without a reopen gives the file back; a token of
        // another keeping does not.
        assert!(hold(id));
        keep(id, 7, 2);
        drop_kept(id, 1);
        assert!(is_held(id));
        drop_kept(id, 2);
        assert!(!is_held(id));
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "std::fs::copy calls copy_file_range, which Miri cannot run"
    )]
    fn a_stamp_tells_a_changed_or_replaced_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cell.db");
        std::fs::write(&path, b"abc").unwrap();
        let at = |secs| std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs);
        let set = |t| {
            std::fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(t)
                .unwrap()
        };
        set(at(1_000));
        let before = Stamp::of(&path).unwrap();
        assert_eq!(Stamp::of(&path), Some(before));
        // The same size and the same name, written at another time.
        set(at(2_000));
        assert_ne!(Stamp::of(&path), Some(before));

        // Another file of the same size and time under the name: the inode
        // and the change time tell it.
        let copy = dir.path().join("copy");
        std::fs::copy(&path, &copy).unwrap();
        let file = std::fs::File::options().write(true).open(&copy).unwrap();
        file.set_modified(at(2_000)).unwrap();
        drop(file);
        let now = Stamp::of(&path).unwrap();
        std::fs::rename(&copy, &path).unwrap();
        let replaced = Stamp::of(&path).unwrap();
        assert_eq!((replaced.size, replaced.mtime), (now.size, now.mtime));
        assert_ne!(replaced, now);

        std::fs::remove_file(&path).unwrap();
        assert_eq!(Stamp::of(&path), None);
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

    #[test]
    fn an_attach_name_is_read_as_sqlite_reads_it() {
        let t = |s: &str| attach_target(s.as_bytes());
        let rw = |p: &str| Some((p.as_bytes().to_vec(), false));
        assert_eq!(t("/a/cell.db"), rw("/a/cell.db"));
        assert_eq!(t("file:/a/cell.db"), rw("/a/cell.db"));
        assert_eq!(t("file:%2Fa%2Fcell%2edb"), rw("/a/cell.db"));
        assert_eq!(t("file:///a/cell.db?cache=shared#x"), rw("/a/cell.db"));
        assert_eq!(t("file://localhost/a/cell.db"), rw("/a/cell.db"));
        assert_eq!(t("file://host/a/cell.db"), None);
        assert_eq!(t("file:rel/cell.db%00.x"), rw("rel/cell.db"));
        assert_eq!(t("file:c?mode=ro"), Some((b"c".to_vec(), true)));
        assert_eq!(t("file:c?mode=%72%6F"), Some((b"c".to_vec(), true)));
        assert_eq!(t("file:c?mode=ro&mode=rw"), rw("c"));
        assert_eq!(t("file:c?mode=rw&mode=ro"), Some((b"c".to_vec(), true)));
        assert_eq!(t("file:c?immutable=1"), Some((b"c".to_vec(), true)));
        assert_eq!(t("file:c?immutable=0&immutable=1"), rw("c"));
        assert_eq!(t("c?mode=ro"), rw("c?mode=ro"));
    }
}
