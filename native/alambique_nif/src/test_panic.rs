//! NIFs that panic, to prove from Elixir that a panic does not stop the VM
//! (design §2.1). Compiled only with the `test-panic` feature, which the test
//! build enables.

use std::ffi::{c_int, CString};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};

use libsqlite3_sys as ffi;
use rustler::{Atom, Env, Resource, ResourceArc, Term};

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
    crate::held::bypassing(|| sqlite::open(env, path, false, None, false, 0, false))
}
