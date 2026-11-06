//! The Alambique VFS (design §16): SQLite's default VFS with one change.
//! Reads of the main database file of a lazily restored cell wait for pages
//! that are not on disk yet (`lazy`); writes mark pages as local before the
//! bytes reach the file. Everything else, including the WAL, its shared
//! memory, journals and locks, is the default VFS.
//!
//! Every callback runs through `guard`: a panic becomes an SQLite error code
//! instead of unwinding into C (design §2.1).

use std::ffi::{c_char, c_int, c_void, CStr};
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Once};

use libsqlite3_sys as ffi;

use crate::guard::guard;
use crate::lazy::{self, Lazy};

pub const NAME: &CStr = c"alambique";

/// The VFS everything is delegated to.
static BASE: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(ptr::null_mut());

/// An open file: SQLite's file header, the lazy state of a hydrating main
/// database (or null), and then the default VFS's own file structure.
#[repr(C)]
struct File {
    base: ffi::sqlite3_file,
    lazy: *const Lazy,
}

const HEADER: usize = size_of::<File>().next_multiple_of(16);

/// The default VFS's part of an open file.
///
/// # Safety
/// `f` points to a file that `x_open` set up, with `szOsFile` bytes behind it.
unsafe fn real(f: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    // SAFETY: the allocation holds `HEADER` bytes of `File` and then the
    // default VFS's structure (`szOsFile` in `register`).
    unsafe { (f as *mut u8).add(HEADER) as *mut ffi::sqlite3_file }
}

/// # Safety
/// `f` points to a file that `x_open` initialized.
unsafe fn lazy_of<'a>(f: *mut ffi::sqlite3_file) -> Option<&'a Lazy> {
    // SAFETY: `lazy` is null or an `Arc` raw pointer that `x_close` releases
    // only after SQLite's last call on the file.
    unsafe { (*(f as *mut File)).lazy.as_ref() }
}

/// The default VFS's method table of an open file.
///
/// # Safety
/// `f` points to a file whose default-VFS part `x_open` opened.
unsafe fn methods(f: *mut ffi::sqlite3_file) -> &'static ffi::sqlite3_io_methods {
    // SAFETY: the default VFS's `xOpen` set `pMethods` to a static table.
    unsafe { &*(*real(f)).pMethods }
}

unsafe extern "C" fn x_open(
    _vfs: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let base = BASE.load(Ordering::Acquire);
    let f = file as *mut File;
    // SAFETY: SQLite passes `szOsFile` writable bytes (`register`); `base` is
    // the default VFS found there and `real(file)` is the room for its file.
    let rc = unsafe {
        (*f).base.pMethods = ptr::null();
        (*f).lazy = ptr::null();
        match (*base).xOpen {
            Some(open) => open(base, name, real(file), flags, out_flags),
            None => ffi::SQLITE_CANTOPEN,
        }
    };
    if rc != ffi::SQLITE_OK {
        return rc;
    }
    // `Err` is a panic: the file is closed again so that no descriptor leaks.
    let lazy = guard("x_open", Err(()), || {
        // SAFETY: a main database has a NUL-terminated file name.
        let path = (flags & ffi::SQLITE_OPEN_MAIN_DB != 0 && !name.is_null())
            .then(|| unsafe { CStr::from_ptr(name) }.to_str().ok())
            .flatten();
        Ok(path.and_then(|p| lazy::lookup(Path::new(p))))
    });
    let Ok(lazy) = lazy else {
        // SAFETY: `real(file)` was opened above and has its method table.
        unsafe {
            let real = real(file);
            ((*(*real).pMethods).xClose.expect("xClose"))(real);
        }
        return ffi::SQLITE_CANTOPEN;
    };
    // SAFETY: `file` is writable (see above); `IO_METHODS` is static.
    unsafe {
        if let Some(lazy) = lazy {
            (*f).lazy = Arc::into_raw(lazy);
        }
        (*f).base.pMethods = &IO_METHODS;
    }
    crate::stats::FILES_OPEN.fetch_add(1, Ordering::Relaxed);
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    guard("x_close", ffi::SQLITE_IOERR_CLOSE, || {
        let f = file as *mut File;
        // SAFETY: `file` is open (SQLite calls `xClose` once, on an open
        // file); `lazy` came from `Arc::into_raw` in `x_open`.
        let rc = unsafe {
            let rc = (methods(file).xClose.expect("xClose"))(real(file));
            if !(*f).lazy.is_null() {
                drop(Arc::from_raw((*f).lazy));
                (*f).lazy = ptr::null();
            }
            (*f).base.pMethods = ptr::null();
            rc
        };
        crate::stats::FILES_OPEN.fetch_sub(1, Ordering::Relaxed);
        rc
    })
}

unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    offset: i64,
) -> c_int {
    guard("x_read", ffi::SQLITE_IOERR_READ, || {
        // SAFETY: `file` is an open file of this VFS; SQLite's arguments go to
        // the default VFS as they came.
        unsafe {
            if let Some(lazy) = lazy_of(file) {
                if !lazy.is_complete() && lazy.before_read(offset as u64, amt as u64).is_err() {
                    return ffi::SQLITE_IOERR_READ;
                }
            }
            (methods(file).xRead.expect("xRead"))(real(file), buf, amt, offset)
        }
    })
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    offset: i64,
) -> c_int {
    guard("x_write", ffi::SQLITE_IOERR_WRITE, || {
        // SAFETY: `file` is an open file of this VFS; SQLite's arguments go to
        // the default VFS as they came.
        unsafe {
            if let Some(lazy) = lazy_of(file) {
                if !lazy.is_complete() && lazy.before_write(offset as u64, amt as u64).is_err() {
                    return ffi::SQLITE_IOERR_WRITE;
                }
            }
            (methods(file).xWrite.expect("xWrite"))(real(file), buf, amt, offset)
        }
    })
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: i64) -> c_int {
    guard("x_truncate", ffi::SQLITE_IOERR_TRUNCATE, || {
        // SAFETY: `file` is an open file of this VFS.
        unsafe {
            if let Some(lazy) = lazy_of(file) {
                lazy.on_truncate(size as u64);
            }
            (methods(file).xTruncate.expect("xTruncate"))(real(file), size)
        }
    })
}

/// Defines a callback that hands the call to the default VFS's method of the
/// same name, with `$fallback` as the answer when it panics.
macro_rules! passthrough {
    ($name:ident, $method:ident, $fallback:expr $(, $arg:ident: $ty:ty)*) => {
        unsafe extern "C" fn $name(file: *mut ffi::sqlite3_file $(, $arg: $ty)*) -> c_int {
            guard(stringify!($name), $fallback, || {
                // SAFETY: `file` is an open file of this VFS and SQLite's
                // arguments go to the default VFS as they came.
                unsafe {
                    (methods(file).$method.expect(stringify!($method)))(real(file) $(, $arg)*)
                }
            })
        }
    };
}

passthrough!(x_sync, xSync, ffi::SQLITE_IOERR_FSYNC, flags: c_int);
passthrough!(x_file_size, xFileSize, ffi::SQLITE_IOERR_FSTAT, size: *mut i64);
passthrough!(x_lock, xLock, ffi::SQLITE_IOERR_LOCK, level: c_int);
passthrough!(x_unlock, xUnlock, ffi::SQLITE_IOERR_UNLOCK, level: c_int);
passthrough!(
    x_check_reserved_lock,
    xCheckReservedLock,
    ffi::SQLITE_IOERR_CHECKRESERVEDLOCK,
    out: *mut c_int
);
passthrough!(x_file_control, xFileControl, ffi::SQLITE_ERROR, op: c_int, arg: *mut c_void);
// SQLite's default sector size and no device characteristics.
passthrough!(x_sector_size, xSectorSize, 4096);
passthrough!(x_device_characteristics, xDeviceCharacteristics, 0);
passthrough!(
    x_shm_map,
    xShmMap,
    ffi::SQLITE_IOERR_SHMMAP,
    page: c_int,
    size: c_int,
    extend: c_int,
    out: *mut *mut c_void
);
passthrough!(
    x_shm_lock,
    xShmLock,
    ffi::SQLITE_IOERR_SHMLOCK,
    offset: c_int,
    n: c_int,
    flags: c_int
);
passthrough!(x_shm_unmap, xShmUnmap, ffi::SQLITE_IOERR, delete: c_int);

unsafe extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    guard("x_shm_barrier", (), || {
        // SAFETY: `file` is an open file of this VFS.
        unsafe { (methods(file).xShmBarrier.expect("xShmBarrier"))(real(file)) }
    })
}

// No memory mapping: reads always go through `x_read`.
unsafe extern "C" fn x_fetch(
    _file: *mut ffi::sqlite3_file,
    _offset: i64,
    _amt: c_int,
    out: *mut *mut c_void,
) -> c_int {
    // SAFETY: SQLite passes a writable pointer for the mapping.
    unsafe { *out = ptr::null_mut() };
    ffi::SQLITE_OK
}

extern "C" fn x_unfetch(_file: *mut ffi::sqlite3_file, _offset: i64, _p: *mut c_void) -> c_int {
    ffi::SQLITE_OK
}

static IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: Some(x_shm_map),
    xShmLock: Some(x_shm_lock),
    xShmBarrier: Some(x_shm_barrier),
    xShmUnmap: Some(x_shm_unmap),
    xFetch: Some(x_fetch),
    xUnfetch: Some(x_unfetch),
};

/// Registers the VFS (once). It is not the default.
pub fn register() -> Result<(), String> {
    static ONCE: Once = Once::new();
    let mut result = Ok(());
    ONCE.call_once(|| {
        // SAFETY: `sqlite3_vfs_find` returns the registered default VFS, which
        // lives for the whole process; the copy is leaked on purpose because
        // SQLite keeps the pointer.
        unsafe {
            let base = ffi::sqlite3_vfs_find(ptr::null());
            if base.is_null() {
                result = Err("no default VFS".to_string());
                return;
            }
            BASE.store(base, Ordering::Release);
            // Same methods as the default VFS, except `xOpen` and the size of
            // the file structure; `pAppData` stays the default VFS's own.
            let mut vfs = *base;
            vfs.pNext = ptr::null_mut();
            vfs.zName = NAME.as_ptr() as *const c_char;
            vfs.szOsFile = (HEADER as c_int) + (*base).szOsFile;
            vfs.xOpen = Some(x_open);
            let vfs = Box::leak(Box::new(vfs));
            let rc = ffi::sqlite3_vfs_register(vfs, 0);
            if rc != ffi::SQLITE_OK {
                result = Err(format!("sqlite3_vfs_register failed: {rc}"));
            }
        }
    });
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ltx::{Encoder, Header, CHECKSUM_FLAG, HEADER_FLAG_NO_CHECKSUM};
    use std::ffi::CString;
    use std::sync::{mpsc, Mutex};
    use std::time::Duration;

    fn exec(db: *mut ffi::sqlite3, sql: &str) -> c_int {
        let sql = CString::new(sql).unwrap();
        // SAFETY: the tests pass a live connection and keep `sql` alive.
        unsafe { ffi::sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut()) }
    }

    fn open(path: &Path, vfs: &CStr) -> *mut ffi::sqlite3 {
        let mut db = ptr::null_mut();
        let name = CString::new(path.to_str().unwrap()).unwrap();
        // SAFETY: `name` and `vfs` are NUL-terminated and outlive the call.
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                name.as_ptr(),
                &mut db,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                vfs.as_ptr(),
            )
        };
        assert_eq!(rc, ffi::SQLITE_OK);
        db
    }

    fn close(db: *mut ffi::sqlite3) {
        // SAFETY: the tests close each connection once, after its last use.
        unsafe { ffi::sqlite3_close(db) };
    }

    fn scalar(db: *mut ffi::sqlite3, sql: &str) -> Result<i64, i32> {
        let sql = CString::new(sql).unwrap();
        let mut stmt = ptr::null_mut();
        // SAFETY: a live connection; the statement is finalized before it
        // leaves the block.
        unsafe {
            assert_eq!(
                ffi::sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                ffi::SQLITE_OK
            );
            let rc = ffi::sqlite3_step(stmt);
            let value = ffi::sqlite3_column_int64(stmt, 0);
            ffi::sqlite3_finalize(stmt);
            if rc == ffi::SQLITE_ROW {
                Ok(value)
            } else {
                Err(rc)
            }
        }
    }

    /// Builds a snapshot LTX file of the database at `path` (closed).
    fn snapshot(path: &Path, ps: u32) -> Vec<u8> {
        let bytes = std::fs::read(path).unwrap();
        let pages = (bytes.len() / ps as usize) as u32;
        let header = Header {
            page_size: ps,
            commit: pages,
            min_txid: 1,
            max_txid: 1,
            timestamp: 1,
            post_apply: CHECKSUM_FLAG | 1,
            flags: HEADER_FLAG_NO_CHECKSUM,
            ..Default::default()
        };
        let mut enc = Encoder::new(header).unwrap();
        for p in 1..=pages {
            enc.add_page(
                p,
                &bytes[(p as usize - 1) * ps as usize..p as usize * ps as usize],
            )
            .unwrap();
        }
        enc.finish().unwrap()
    }

    /// Serves misses from `file` on a thread, with an optional delay and a
    /// switch to fail every fetch.
    fn serve(
        lazy: &Arc<Lazy>,
        file: Vec<u8>,
        delay_ms: u64,
        broken: Arc<std::sync::atomic::AtomicBool>,
    ) {
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        lazy.attach(Arc::new(move |m| tx.lock().unwrap().send(m).unwrap()));
        let lazy = lazy.clone();
        std::thread::spawn(move || {
            while let Ok(m) = rx.recv() {
                std::thread::sleep(Duration::from_millis(delay_ms));
                if broken.load(Ordering::Relaxed) {
                    lazy.fail(m.pgno, "store down".into());
                    continue;
                }
                let frame = &file[m.offset as usize..(m.offset + m.size as u64) as usize];
                let page = lazy::decode_frame(frame, m.pgno, lazy.page_size).unwrap();
                lazy.install(m.pgno, &page).unwrap();
            }
        });
    }

    fn plan(dir: &Path, ltx: &[u8], wait_ms: u64) -> Arc<Lazy> {
        let path = dir.join("lazy.db");
        let lazy = Lazy::create(&path, 4096, Duration::from_millis(wait_ms)).unwrap();
        let total = ltx.len() as u64;
        let staged = lazy::stage_file(&ltx[..100], ltx, total)
            .unwrap()
            .ok()
            .unwrap();
        lazy.stage(0, staged);
        lazy.apply(0).unwrap();
        lazy.ready().unwrap();
        let lazy = Arc::new(lazy);
        lazy::register(&lazy);
        lazy
    }

    fn source(dir: &Path) -> Vec<u8> {
        register().unwrap();
        let path = dir.join("source.db");
        let db = open(&path, NAME);
        assert_eq!(
            exec(db, "PRAGMA page_size = 4096; PRAGMA journal_mode = WAL"),
            0
        );
        assert_eq!(
            exec(
                db,
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB); \
                 WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 400) \
                 INSERT INTO t SELECT x, randomblob(1500) FROM n; PRAGMA wal_checkpoint(TRUNCATE)"
            ),
            0
        );
        close(db);
        snapshot(&path, 4096)
    }

    #[test]
    #[cfg_attr(miri, ignore = "calls SQLite")]
    fn reads_wait_for_pages_and_see_the_same_data() {
        let dir = tempfile::tempdir().unwrap();
        let ltx = source(dir.path());
        let expect = {
            let db = open(&dir.path().join("source.db"), NAME);
            let v = scalar(db, "SELECT sum(length(v)) + count(*) FROM t").unwrap();
            close(db);
            v
        };
        let lazy = plan(dir.path(), &ltx, 5000);
        serve(&lazy, ltx, 1, Arc::default());
        let db = open(&dir.path().join("lazy.db"), NAME);
        assert_eq!(
            scalar(db, "SELECT sum(length(v)) + count(*) FROM t"),
            Ok(expect)
        );
        assert!(scalar(db, "PRAGMA integrity_check = 1").is_ok());
        close(db);
        lazy::unregister(&lazy);
    }

    #[test]
    #[cfg_attr(miri, ignore = "calls SQLite")]
    fn a_failed_fetch_fails_the_statement_and_the_database_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let ltx = source(dir.path());
        let lazy = plan(dir.path(), &ltx, 5000);
        let broken = Arc::new(std::sync::atomic::AtomicBool::new(false));
        serve(&lazy, ltx, 0, broken.clone());
        let db = open(&dir.path().join("lazy.db"), NAME);
        assert!(scalar(db, "PRAGMA schema_version").is_ok());
        broken.store(true, Ordering::Relaxed);
        // A page that is not there fails the statement with an I/O error.
        assert_eq!(scalar(db, "SELECT count(*) FROM t"), Err(ffi::SQLITE_IOERR));
        broken.store(false, Ordering::Relaxed);
        assert_eq!(scalar(db, "SELECT count(*) FROM t"), Ok(400));
        close(db);
        lazy::unregister(&lazy);
    }

    #[test]
    #[cfg_attr(miri, ignore = "calls SQLite")]
    fn a_silent_hydrator_times_the_statement_out() {
        let dir = tempfile::tempdir().unwrap();
        let ltx = source(dir.path());
        let lazy = plan(dir.path(), &ltx, 150);
        let (tx, _rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        lazy.attach(Arc::new(move |m| {
            let _ = tx.lock().unwrap().send(m);
        }));
        // Not even the file header arrives.
        let mut db = ptr::null_mut();
        let name = CString::new(dir.path().join("lazy.db").to_str().unwrap()).unwrap();
        // SAFETY: `name` and the VFS name are NUL-terminated.
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                name.as_ptr(),
                &mut db,
                ffi::SQLITE_OPEN_READWRITE,
                NAME.as_ptr(),
            )
        };
        assert_eq!(rc & 0xff, ffi::SQLITE_IOERR);
        close(db);
        lazy::unregister(&lazy);
    }

    #[test]
    #[cfg_attr(miri, ignore = "calls SQLite")]
    fn writes_and_checkpoints_during_hydration_keep_local_pages() {
        let dir = tempfile::tempdir().unwrap();
        let ltx = source(dir.path());
        let lazy = plan(dir.path(), &ltx, 5000);
        let source = ltx.clone();
        serve(&lazy, ltx, 0, Arc::default());
        let db = open(&dir.path().join("lazy.db"), NAME);
        assert_eq!(exec(db, "PRAGMA journal_mode = WAL"), 0);
        assert_eq!(
            exec(db, "UPDATE t SET v = zeroblob(10) WHERE id % 7 = 0"),
            0
        );
        assert_eq!(exec(db, "DELETE FROM t WHERE id > 380"), 0);
        assert_eq!(exec(db, "PRAGMA wal_checkpoint(TRUNCATE)"), 0);
        // The background fetch runs to the end afterwards; nothing it
        // installs may replace what the database wrote.
        while let Some(run) = lazy.next_batch(u64::MAX) {
            for (pgno, rel, len) in &run.pages {
                let at = (run.offset + *rel as u64) as usize;
                let page =
                    lazy::decode_frame(&source[at..at + *len as usize], *pgno, 4096).unwrap();
                lazy.install(*pgno, &page).unwrap();
            }
        }
        assert!(lazy.is_complete());
        assert_eq!(
            scalar(db, "SELECT count(*) FROM t WHERE length(v) = 10"),
            Ok(54)
        );
        assert_eq!(scalar(db, "SELECT count(*) FROM t"), Ok(380));
        assert_eq!(exec(db, "PRAGMA integrity_check"), 0);
        close(db);
        lazy::unregister(&lazy);
    }

    #[test]
    #[cfg_attr(miri, ignore = "calls SQLite")]
    fn a_plain_open_through_the_vfs_is_an_ordinary_database() {
        register().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir.path().join("plain.db"), NAME);
        assert_eq!(exec(db, "PRAGMA journal_mode = WAL; CREATE TABLE a (b)"), 0);
        assert_eq!(exec(db, "INSERT INTO a VALUES (1)"), 0);
        assert_eq!(scalar(db, "SELECT count(*) FROM a"), Ok(1));
        close(db);
        assert!(dir.path().join("plain.db").exists());
    }
}
