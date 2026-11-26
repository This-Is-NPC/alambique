//! NIFs that panic, to prove from Elixir that a panic does not stop the VM
//! (design §2.1). Compiled only with the `test-panic` feature, which the test
//! build enables.

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use libsqlite3_sys as ffi;
use rustler::{Atom, Binary, Encoder, Env, Resource, ResourceArc, Term};

use crate::guard::{guard, probe};
use crate::{sqlite, vfs};

#[rustler::nif]
fn panic_normal() -> Atom {
    panic!("test panic on a normal scheduler")
}

#[rustler::nif(schedule = "DirtyCpu")]
fn panic_dirty_cpu() -> Atom {
    panic!("test panic on a dirty CPU scheduler")
}

#[rustler::nif(schedule = "DirtyIo")]
fn panic_dirty_io() -> Atom {
    panic!("test panic on a dirty I/O scheduler")
}

/// The page sidecar `side` with a wrong page hash and a right seal.
#[rustler::nif]
fn forge_sidecar<'a>(env: Env<'a>, side: Binary) -> Term<'a> {
    match crate::pages::forge(side.as_slice()) {
        Some(forged) => crate::bin(env, &forged),
        None => rustler::types::atom::error().encode(env),
    }
}

/// A resource whose destructor panics, like a buggy `Drop` would.
pub struct PanicOnDrop;

static DROPS: AtomicU64 = AtomicU64::new(0);

#[rustler::resource_impl]
impl Resource for PanicOnDrop {}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        guard("test_drop", (), || {
            DROPS.fetch_add(1, Ordering::SeqCst);
            panic!("test panic in a destructor");
        });
    }
}

#[rustler::nif]
fn panic_resource() -> ResourceArc<PanicOnDrop> {
    ResourceArc::new(PanicOnDrop)
}

/// How many `PanicOnDrop` resources were destroyed.
#[rustler::nif]
fn panic_drops() -> u64 {
    DROPS.load(Ordering::SeqCst)
}

/// Makes the `PRAGMA query_only = ON` that follows the next commit of
/// `sqlite_finish_transaction` on `conn` fail.
#[rustler::nif]
fn fail_next_query_only(conn: ResourceArc<sqlite::Conn>) -> Atom {
    conn.fail_next_query_only();
    rustler::types::atom::ok()
}

/// Commits the transaction of the library on `conn` with the token it holds,
/// as a bug that ended it behind the library's back would. The token is not
/// handed out: tests that need the commit call this.
#[rustler::nif(schedule = "DirtyIo")]
fn commit_behind_back<'a>(env: Env<'a>, conn: ResourceArc<sqlite::Conn>) -> Term<'a> {
    sqlite::finish_armed(env, &conn)
}

/// Opens a database through the Alambique VFS, with the authorizer of the
/// real connections, and runs statements while a panic is armed at `site`.
/// Returns the result codes of the open and of the statements.
#[rustler::nif(schedule = "DirtyIo")]
fn panic_in_callback(site: String, path: String) -> (i32, i32) {
    let site: &'static str = Box::leak(site.into_boxed_str());
    let path = CString::new(path).expect("path without NUL");
    let sql = c"PRAGMA user_version; PRAGMA journal_mode = WAL; \
                CREATE TABLE t (a); INSERT INTO t VALUES (1); SELECT a FROM t";
    let mut db = ptr::null_mut();
    let control = sqlite::Control::new(false, 0);
    let _armed = probe::panic_at(site);
    // SAFETY: `path`, `sql` and the VFS name are NUL-terminated and outlive
    // the calls; `db` is closed once, below.
    unsafe {
        let open = ffi::sqlite3_open_v2(
            path.as_ptr(),
            &mut db,
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
            vfs::NAME.as_ptr(),
        );
        let mut exec: c_int = open;
        if open == ffi::SQLITE_OK {
            ffi::sqlite3_set_authorizer(
                db,
                Some(sqlite::authorize),
                &control as *const sqlite::Control as *mut _,
            );
            exec = ffi::sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut());
        }
        ffi::sqlite3_close(db);
        (open, exec)
    }
}

/// Runs `sql` on a connection of its own, with SQLite's default VFS and
/// without the table of held files: a writer the library cannot prevent
/// (another process, or code that opened the file before the cell did).
pub(crate) fn foreign_exec_raw(path: &str, sql: &str) -> c_int {
    let (Ok(path), Ok(sql)) = (CString::new(path), CString::new(sql)) else {
        return ffi::SQLITE_MISUSE;
    };
    let mut db = ptr::null_mut();
    // SAFETY: both strings are NUL-terminated and outlive the calls; `db` is
    // closed once.
    unsafe {
        let mut rc = ffi::sqlite3_open_v2(
            path.as_ptr(),
            &mut db,
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_URI,
            ptr::null(),
        );
        if rc == ffi::SQLITE_OK {
            let mut out: c_int = 0;
            ffi::sqlite3_db_config(
                db,
                ffi::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
                1 as c_int,
                &mut out as *mut c_int,
            );
            rc = ffi::sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut());
        }
        ffi::sqlite3_close(db);
        rc
    }
}

/// `foreign_exec_raw` as a NIF: the result code.
#[rustler::nif(schedule = "DirtyIo")]
fn foreign_exec(path: String, sql: String) -> i32 {
    foreign_exec_raw(&path, &sql)
}

/// The next transaction of the library on `conn` runs `sql` on a separate
/// connection of `path` right after it compared the wal-index header. With the
/// write lock held the statement is refused as busy; a check made before the
/// lock would let it commit unseen.
#[rustler::nif]
fn set_foreign_commit(conn: ResourceArc<sqlite::Conn>, path: String, sql: String) -> Atom {
    conn.set_foreign_commit(path, sql);
    rustler::types::atom::ok()
}

/// Makes the wal-index header of `conn` read as unreadable (or readable).
#[rustler::nif]
fn set_header_unreadable(conn: ResourceArc<sqlite::Conn>, on: bool) -> Atom {
    conn.set_header_unreadable(on);
    rustler::types::atom::ok()
}

/// Opens `path` read-write with SQLite's default VFS as a writer that the
/// table of held files cannot see: `{:ok, conn, 0}` as `sqlite_open`.
#[rustler::nif(schedule = "DirtyIo")]
fn foreign_open<'a>(env: Env<'a>, path: String) -> Term<'a> {
    crate::held::bypassing(|| {
        sqlite::open(
            env,
            path,
            sqlite::OpenOpts {
                readonly: false,
                vfs: None,
                attach: false,
                timeout_ms: 0,
                gated: false,
                exclusive: false,
                lookaside: None,
            },
        )
    })
}

mod fault_atoms {
    rustler::atoms! {
        commit_frame, frame, sync, ioerr_write, full, ioerr_fsync,
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    /// The page write of a commit frame: the frame is on disk when it fails.
    CommitFrame,
    /// The page write of a frame that does not commit.
    Frame,
    /// Any `xSync` of the file.
    Sync,
}

struct Fault {
    path: Vec<u8>,
    kind: Kind,
    rc: c_int,
    /// Selected writes (or syncs) to let pass before the fault.
    skip: u64,
    times: u64,
    /// Offset of the page write that fails: the frame was selected by its header.
    page_at: Option<i64>,
}

/// A process-global fault: the probe of `guard` is thread-local and does not
/// reach the dirty scheduler thread that runs the statement.
static FAULT: Mutex<Option<Fault>> = Mutex::new(None);
/// One relaxed load when nothing is armed.
static ARMED: AtomicBool = AtomicBool::new(false);
static FIRED: AtomicU64 = AtomicU64::new(0);

/// The file name SQLite gave `xOpen`, kept with the open file; freed by
/// `file_closed`.
pub(crate) fn file_opened(name: *const c_char) -> *mut c_char {
    if name.is_null() {
        return ptr::null_mut();
    }
    // SAFETY: SQLite passes a NUL-terminated name.
    unsafe { CStr::from_ptr(name) }.to_owned().into_raw()
}

/// # Safety
/// `name` came from `file_opened` and is not used again.
pub(crate) unsafe fn file_closed(name: *mut c_char) {
    if !name.is_null() {
        // SAFETY: by the contract.
        drop(unsafe { CString::from_raw(name) });
    }
}

/// The armed fault when it is for the file `name` and for syncs (`sync`) or writes.
fn matching(name: *const c_char, sync: bool) -> Option<MutexGuard<'static, Option<Fault>>> {
    if !ARMED.load(Ordering::Relaxed) || name.is_null() {
        return None;
    }
    let slot = FAULT.lock().unwrap_or_else(|e| e.into_inner());
    // SAFETY: `name` is a NUL-terminated string from `file_opened`.
    let path = unsafe { CStr::from_ptr(name) }.to_bytes();
    let for_this = slot
        .as_ref()
        .is_some_and(|f| f.path == path && (f.kind == Kind::Sync) == sync);
    for_this.then_some(slot)
}

fn fire(slot: &mut Option<Fault>) -> c_int {
    let f = slot.as_mut().expect("armed");
    let rc = f.rc;
    f.page_at = None;
    f.times -= 1;
    FIRED.fetch_add(1, Ordering::SeqCst);
    if f.times == 0 {
        *slot = None;
        ARMED.store(false, Ordering::SeqCst);
    }
    rc
}

/// Called by `xWrite` after the write reached the file: the error to return
/// instead of success. A WAL frame is two writes, a 24-byte header and the
/// page; the header says whether the frame commits (its second word is the
/// database size) and the fault fires after the page write that follows.
pub(crate) fn after_write(
    name: *const c_char,
    buf: *const c_void,
    amt: c_int,
    offset: i64,
) -> Option<c_int> {
    let mut slot = matching(name, false)?;
    let f = slot.as_mut()?;
    if f.page_at == Some(offset) {
        return Some(fire(&mut slot));
    }
    if amt == 24 {
        // SAFETY: SQLite passes `amt` readable bytes.
        let header = unsafe { std::slice::from_raw_parts(buf as *const u8, 24) };
        let commits = header[4..8] != [0; 4];
        if commits == (f.kind == Kind::CommitFrame) {
            if f.skip > 0 {
                f.skip -= 1;
            } else {
                f.page_at = Some(offset + 24);
            }
        }
    }
    None
}

/// Called by `xSync`: the error to return instead of success.
pub(crate) fn after_sync(name: *const c_char) -> Option<c_int> {
    let mut slot = matching(name, true)?;
    let f = slot.as_mut()?;
    if f.skip > 0 {
        f.skip -= 1;
        return None;
    }
    Some(fire(&mut slot))
}

/// Arms a fault on the file `path` (the whole path, as SQLite opens it):
/// `kind` is `:commit_frame`, `:frame` (a frame that does not commit) or
/// `:sync`; the `skip` selected writes before it pass; `rc` is `:ioerr_write`,
/// `:full` or `:ioerr_fsync`; it fires `times` times.
#[rustler::nif]
fn vfs_fault_arm(path: String, kind: Atom, skip: u64, rc: Atom, times: u64) -> Atom {
    use fault_atoms as a;
    let kind = if kind == a::commit_frame() {
        Kind::CommitFrame
    } else if kind == a::frame() {
        Kind::Frame
    } else if kind == a::sync() {
        Kind::Sync
    } else {
        return rustler::types::atom::error();
    };
    let rc = if rc == a::ioerr_write() {
        ffi::SQLITE_IOERR_WRITE
    } else if rc == a::full() {
        ffi::SQLITE_FULL
    } else if rc == a::ioerr_fsync() {
        ffi::SQLITE_IOERR_FSYNC
    } else {
        return rustler::types::atom::error();
    };
    let mut slot = FAULT.lock().unwrap_or_else(|e| e.into_inner());
    FIRED.store(0, Ordering::SeqCst);
    *slot = Some(Fault {
        path: path.into_bytes(),
        kind,
        rc,
        skip,
        times: times.max(1),
        page_at: None,
    });
    ARMED.store(true, Ordering::SeqCst);
    rustler::types::atom::ok()
}

/// Disarms the fault.
#[rustler::nif]
fn vfs_fault_clear() -> Atom {
    let mut slot = FAULT.lock().unwrap_or_else(|e| e.into_inner());
    *slot = None;
    ARMED.store(false, Ordering::SeqCst);
    rustler::types::atom::ok()
}

/// How many times the fault fired since it was armed.
#[rustler::nif]
fn vfs_fault_hits() -> u64 {
    FIRED.load(Ordering::SeqCst)
}
