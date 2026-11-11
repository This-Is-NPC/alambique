//! NIFs that panic, to prove from Elixir that a panic does not stop the VM
//! (design §2.1). Compiled only with the `test-panic` feature, which the test
//! build enables.

use std::ffi::{c_int, CString};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};

use libsqlite3_sys as ffi;
use rustler::{Atom, Resource, ResourceArc};

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
