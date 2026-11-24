//! SQLite connections and statements as NIF resources.
//!
//! One bundled SQLite serves every cell, so the Alambique VFS (`vfs`) lives in
//! the same library as the queries. All calls that can block run on dirty
//! schedulers. SQLite runs in serialized mode: a connection may be used from
//! any thread, and `interrupt` may be called while another thread steps.

use std::ffi::{c_char, c_int, CStr, CString};
use std::ptr;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{OnceLock, RwLock};
use std::time::Instant;

use crate::guard::guard;
use crate::stats::{CACHE_HITS, CACHE_MISSES, CONNECTIONS_OPEN};

use libsqlite3_sys as ffi;
use rustler::{Atom, Binary, Encoder, Env, NewBinary, Resource, ResourceArc, Term, TermType};

extern "C" {
    // Not in the generated bindings. Closes now when no statement is open and
    // when the last one is finalized otherwise.
    #[link_name = "sqlite3_close_v2"]
    fn close_v2(db: *mut ffi::sqlite3) -> c_int;
}

/// VM instructions between two looks at the statement deadline.
const PROGRESS_OPS: c_int = 1000;

mod atoms {
    rustler::atoms! {
        ok, error, blob, nil, undefined, idle, transaction,
        connection_closed, argument_error, busy, disk_full,
        statement_timeout, result_too_large, not_authorized, multiple_statements, transaction_ended, cache_used,
        schema_used, stmt_used, lookaside_used, nul_in_statement, not_ddl,
    }
}

struct DbPtr(*mut ffi::sqlite3);

// SAFETY: SQLite is built with SQLITE_THREADSAFE=1 (serialized), so a
// connection may be used from any thread.
unsafe impl Send for DbPtr {}
// SAFETY: as above.
unsafe impl Sync for DbPtr {}

/// Microseconds on a clock that starts at the first call, never 0.
fn now_us() -> u64 {
    static BASE: OnceLock<Instant> = OnceLock::new();
    BASE.get_or_init(Instant::now).elapsed().as_micros() as u64 + 1
}

/// What the authorizer and the progress handler of a connection consult. Its
/// address is the callbacks' context, so it lives in a `Box` that outlives
/// the connection (`Conn` closes the database before its fields drop).
pub(crate) struct Control {
    /// Set for the duration of a read-only call (`Call`).
    read_only: AtomicBool,
    /// Set for the duration of a call that runs a user's text: BEGIN, COMMIT,
    /// END and ROLLBACK are refused, so the text cannot end the transaction
    /// of the cell's write (`Call`).
    no_txn_control: AtomicBool,
    /// A transaction of the library is open on the connection
    /// (`sqlite_expect_transaction`): a statement that finds it gone does not
    /// run (`check_transaction`), and no statement of any call may begin,
    /// commit or roll it back (`authorize`). Only the library's own final
    /// `COMMIT` or `ROLLBACK`, after it turned this off, may.
    expect_txn: AtomicBool,
    /// Frames in the WAL after the latest commit on the connection (the WAL
    /// hook), and the value when the transaction of the library began.
    wal_frames: AtomicU64,
    txn_frames: AtomicU64,
    /// `now_us()` after which the running statement is stopped; 0 for none.
    deadline: AtomicU64,
    /// The progress handler stopped a statement at the deadline.
    timed_out: AtomicBool,
    /// `ATTACH`, `DETACH` and `VACUUM` are allowed (`attach: true`).
    allow_attach: bool,
    /// Deadline in ms of a call that sets none; 0 for none.
    default_timeout_ms: u64,
}

impl Control {
    pub(crate) fn new(allow_attach: bool, default_timeout_ms: u64) -> Self {
        Control {
            read_only: AtomicBool::new(false),
            no_txn_control: AtomicBool::new(false),
            expect_txn: AtomicBool::new(false),
            wal_frames: AtomicU64::new(0),
            txn_frames: AtomicU64::new(0),
            deadline: AtomicU64::new(0),
            timed_out: AtomicBool::new(false),
            allow_attach,
            default_timeout_ms,
        }
    }
}

/// An open connection. `None` once closed.
pub struct Conn {
    db: RwLock<Option<DbPtr>>,
    control: Box<Control>,
}

#[rustler::resource_impl]
impl Resource for Conn {}

// ERTS runs `drop` from a C callback that rustler does not protect, so it
// must not panic (design §2.1).
impl Drop for Conn {
    fn drop(&mut self) {
        guard("conn_drop", (), || {
            if let Some(DbPtr(db)) = self.db.get_mut().unwrap_or_else(|e| e.into_inner()).take() {
                // SAFETY: `Conn` owned the connection; it is closed once.
                unsafe { close(db) };
            }
        });
    }
}

struct StmtPtr(*mut ffi::sqlite3_stmt);

// SAFETY: a statement is only used by the call that prepared it, and SQLite
// is serialized.
unsafe impl Send for StmtPtr {}

/// Finalizes a statement that its owner took out of its slot.
fn finalize(s: *mut ffi::sqlite3_stmt) {
    // SAFETY: the owner held the only pointer to a live statement.
    unsafe { ffi::sqlite3_finalize(s) };
}

/// Why a call failed: a term to return as `{:error, term}`.
enum Failure {
    Closed,
    Message(String),
    Argument(String),
    Timeout,
    TooLarge,
    Denied,
    Multiple(String),
    NotDdl(String),
    NulInStatement,
    TransactionEnded,
    Busy,
    DiskFull,
}

impl Failure {
    fn term<'a>(self, env: Env<'a>) -> Term<'a> {
        let reason = match self {
            Failure::Closed => atoms::connection_closed().encode(env),
            Failure::Message(m) => m.encode(env),
            Failure::Argument(m) => (atoms::argument_error(), m).encode(env),
            Failure::Timeout => atoms::statement_timeout().encode(env),
            Failure::TooLarge => atoms::result_too_large().encode(env),
            Failure::Denied => atoms::not_authorized().encode(env),
            Failure::TransactionEnded => atoms::transaction_ended().encode(env),
            Failure::Multiple(tail) => (atoms::multiple_statements(), tail).encode(env),
            Failure::NotDdl(text) => (atoms::not_ddl(), text).encode(env),
            Failure::NulInStatement => atoms::nul_in_statement().encode(env),
            Failure::Busy => atoms::busy().encode(env),
            Failure::DiskFull => atoms::disk_full().encode(env),
        };
        (atoms::error(), reason).encode(env)
    }
}

type Outcome<'a> = Result<Term<'a>, Failure>;

fn reply<'a>(env: Env<'a>, outcome: Outcome<'a>) -> Term<'a> {
    outcome.unwrap_or_else(|f| f.term(env))
}

fn message(db: *mut ffi::sqlite3) -> String {
    // SAFETY: `db` is a live connection; `sqlite3_errmsg` returns a
    // NUL-terminated string that is valid until the next call on `db`.
    unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(db)) }
        .to_string_lossy()
        .into_owned()
}

/// The failure of the latest call on `db`, told by its result code: the
/// authorizer's refusal, a full disk (`SQLITE_FULL`, which an `ENOSPC` of the
/// database, the WAL or a temporary file becomes), or SQLite's message.
fn failure(db: *mut ffi::sqlite3) -> Failure {
    // SAFETY: `db` is a live connection.
    match unsafe { ffi::sqlite3_errcode(db) } & 0xff {
        ffi::SQLITE_AUTH => Failure::Denied,
        ffi::SQLITE_FULL => Failure::DiskFull,
        _ => Failure::Message(message(db)),
    }
}

/// Pragmas that only describe the schema. A read-only call may give them an
/// argument (`PRAGMA table_info(t)`); every other pragma with an argument
/// sets something.
const INTROSPECTION: [&[u8]; 16] = [
    b"table_info",
    b"table_xinfo",
    b"table_list",
    b"index_list",
    b"index_info",
    b"index_xinfo",
    b"foreign_key_list",
    b"foreign_key_check",
    b"function_list",
    b"collation_list",
    b"module_list",
    b"pragma_list",
    b"database_list",
    b"integrity_check",
    b"quick_check",
    b"compile_options",
];

/// Decides every statement of the connection when it is prepared, whatever
/// its syntax (virtual tables prepare theirs when they run, so the read-only
/// flag stays up for the whole call). A panic denies the statement.
///
/// - Always: the pragmas that would checkpoint, turn automatic checkpoints
///   back on or leave WAL mode: WAL frames reach the database file only
///   through `sqlite_checkpoint` (design §6).
/// - Unless the connection was opened with `attach: true`, or in a read-only
///   call: `ATTACH` and `DETACH`, which `VACUUM` also raises. An attached
///   file is another cell's data or a file the SQL creates.
/// - In a read-only call: transaction and savepoint statements, and every
///   pragma with an argument except the introspection ones.
/// - In a call that runs a user's text (`no_txn_control`), and in every call
///   while a transaction of the library is open (`expect_txn`): `BEGIN`,
///   `COMMIT`, `END` and `ROLLBACK` (`SQLITE_TRANSACTION`). Savepoints stay:
///   `ROLLBACK TO` and `RELEASE` are `SQLITE_SAVEPOINT` and never end the
///   transaction.
pub(crate) unsafe extern "C" fn authorize(
    ctx: *mut std::ffi::c_void,
    action: c_int,
    name: *const c_char,
    arg: *const c_char,
    _: *const c_char,
    _: *const c_char,
) -> c_int {
    guard("authorize", ffi::SQLITE_DENY, || {
        // SAFETY: the context is the `Control` of this connection (see there).
        let ctl = unsafe { &*(ctx as *const Control) };
        let read_only = ctl.read_only.load(Relaxed);
        let allowed = match action {
            ffi::SQLITE_ATTACH | ffi::SQLITE_DETACH => ctl.allow_attach && !read_only,
            ffi::SQLITE_TRANSACTION => {
                !read_only && !ctl.no_txn_control.load(Relaxed) && !ctl.expect_txn.load(Relaxed)
            }
            ffi::SQLITE_SAVEPOINT => !read_only,
            ffi::SQLITE_PRAGMA if !name.is_null() => {
                // SAFETY: for a pragma SQLite passes NUL-terminated names,
                // `arg` possibly null.
                let (name, arg) = unsafe {
                    (
                        CStr::from_ptr(name).to_bytes(),
                        (!arg.is_null()).then(|| CStr::from_ptr(arg).to_bytes()),
                    )
                };
                pragma_allowed(name, arg, read_only)
            }
            _ => true,
        };
        if allowed {
            ffi::SQLITE_OK
        } else {
            ffi::SQLITE_DENY
        }
    })
}

fn pragma_allowed(name: &[u8], arg: Option<&[u8]>, read_only: bool) -> bool {
    if name.eq_ignore_ascii_case(b"wal_checkpoint") {
        false
    } else if name.eq_ignore_ascii_case(b"wal_autocheckpoint") {
        arg.is_none()
    } else if name.eq_ignore_ascii_case(b"journal_mode") {
        arg.is_none_or(|a| a.eq_ignore_ascii_case(b"wal")) && !(read_only && arg.is_some())
    } else {
        !read_only || arg.is_none() || INTROSPECTION.iter().any(|p| name.eq_ignore_ascii_case(p))
    }
}

/// Notes the size of the WAL after every commit. It never checkpoints:
/// automatic checkpoints are off (design §6).
unsafe extern "C" fn wal_committed(
    ctx: *mut std::ffi::c_void,
    _: *mut ffi::sqlite3,
    _: *const c_char,
    frames: c_int,
) -> c_int {
    guard("wal_hook", ffi::SQLITE_OK, || {
        // SAFETY: the context is the `Control` of this connection (see there).
        let ctl = unsafe { &*(ctx as *const Control) };
        ctl.wal_frames.store(frames as u64, Relaxed);
        ffi::SQLITE_OK
    })
}

/// Stops the running statement once its deadline has passed.
unsafe extern "C" fn progress(ctx: *mut std::ffi::c_void) -> c_int {
    guard("progress", 1, || {
        // SAFETY: the context is the `Control` of this connection (see there).
        let ctl = unsafe { &*(ctx as *const Control) };
        let deadline = ctl.deadline.load(Relaxed);
        if deadline != 0 && now_us() >= deadline {
            ctl.timed_out.store(true, Relaxed);
            1
        } else {
            0
        }
    })
}

/// User statements running now, node-wide (design §7). The slot is taken and
/// given back inside the NIF, so it is exact even when the caller is killed
/// while the dirty scheduler runs the call.
static USER_STATEMENTS: AtomicU64 = AtomicU64::new(0);

struct Slot;

impl Slot {
    /// Takes a slot when fewer than `max` are taken; 0 takes none and
    /// counts nothing (the library's own statements).
    fn take(max: u64) -> Result<Option<Slot>, Failure> {
        if max == 0 {
            return Ok(None);
        }
        // A refused call never raises the count, so it never exceeds `max`.
        USER_STATEMENTS
            .try_update(AcqRel, Acquire, |n| (n < max).then_some(n + 1))
            .map(|_| Some(Slot))
            .map_err(|_| Failure::Busy)
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        USER_STATEMENTS.fetch_sub(1, AcqRel);
    }
}

/// The limits of one `sqlite_run` call; 0 is no limit.
#[derive(Clone, Copy, Default)]
struct Limits {
    read_only: bool,
    no_txn_control: bool,
    timeout_ms: u64,
    max_rows: u64,
    max_bytes: u64,
}

/// Arms the connection for one call and disarms it when dropped. In a
/// read-only call values over `max_bytes` cannot even be built (`zeroblob`).
struct Call<'a> {
    ctl: &'a Control,
    db: *mut ffi::sqlite3,
    length: Option<c_int>,
}

impl<'a> Call<'a> {
    fn new(ctl: &'a Control, db: *mut ffi::sqlite3, limits: &Limits) -> Self {
        ctl.timed_out.store(false, Relaxed);
        ctl.read_only.store(limits.read_only, Relaxed);
        ctl.no_txn_control.store(limits.no_txn_control, Relaxed);
        let timeout_ms = if limits.timeout_ms > 0 {
            limits.timeout_ms
        } else {
            ctl.default_timeout_ms
        };
        if timeout_ms > 0 {
            let deadline = now_us().saturating_add(timeout_ms.saturating_mul(1000));
            ctl.deadline.store(deadline, Relaxed);
        }
        let length = (limits.read_only && limits.max_bytes > 0).then(|| {
            let cap = limits.max_bytes.min(c_int::MAX as u64) as c_int;
            // SAFETY: `db` is a live connection (callers hold the lock).
            unsafe { ffi::sqlite3_limit(db, ffi::SQLITE_LIMIT_LENGTH, cap) }
        });
        Call { ctl, db, length }
    }
}

impl Drop for Call<'_> {
    fn drop(&mut self) {
        self.ctl.read_only.store(false, Relaxed);
        self.ctl.no_txn_control.store(false, Relaxed);
        self.ctl.deadline.store(0, Relaxed);
        if let Some(old) = self.length {
            // SAFETY: as in `new`; the lock is still held.
            unsafe { ffi::sqlite3_limit(self.db, ffi::SQLITE_LIMIT_LENGTH, old) };
        }
    }
}

/// Adds the page cache hits and misses of `db` since the last call to the
/// global counters.
fn count_cache(db: *mut ffi::sqlite3) {
    for (op, counter) in [
        (ffi::SQLITE_DBSTATUS_CACHE_HIT, &CACHE_HITS),
        (ffi::SQLITE_DBSTATUS_CACHE_MISS, &CACHE_MISSES),
    ] {
        let (mut cur, mut high) = (0, 0);
        // SAFETY: `db` is a live connection (callers hold the lock).
        unsafe { ffi::sqlite3_db_status(db, op, &mut cur, &mut high, 1) };
        counter.fetch_add(cur as u64, Relaxed);
    }
}

/// While a transaction of the library is expected, a statement does not run
/// once SQLite has ended it (a conflict or a trigger that rolls back, an
/// interrupt, an I/O error): what it would write would commit by itself, out
/// of the write that the transaction belongs to. Called with the lock held,
/// before the statement.
fn check_transaction(ctl: &Control, db: *mut ffi::sqlite3) -> Result<(), Failure> {
    // SAFETY: `db` is a live connection (callers hold the lock).
    if ctl.expect_txn.load(Relaxed) && unsafe { ffi::sqlite3_get_autocommit(db) } != 0 {
        return Err(Failure::TransactionEnded);
    }
    Ok(())
}

/// Closes a connection that `Conn` owned.
///
/// # Safety
/// `db` is a live connection that nothing uses afterwards.
unsafe fn close(db: *mut ffi::sqlite3) {
    count_cache(db);
    // SAFETY: by the contract above.
    unsafe { close_v2(db) };
    CONNECTIONS_OPEN.fetch_sub(1, Relaxed);
}

/// Runs `f` with the raw connection while holding the shared lock, which
/// keeps `close` from freeing it underneath.
fn with_db<T>(
    conn: &ResourceArc<Conn>,
    f: impl FnOnce(*mut ffi::sqlite3) -> Result<T, Failure>,
) -> Result<T, Failure> {
    let guard = conn.db.read().unwrap_or_else(|e| e.into_inner());
    match guard.as_ref() {
        Some(DbPtr(db)) => {
            let result = f(*db);
            count_cache(*db);
            result
        }
        None => Err(Failure::Closed),
    }
}

fn binary<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    let mut b = NewBinary::new(env, bytes.len());
    b.as_mut_slice().copy_from_slice(bytes);
    Binary::from(b).to_term(env)
}

fn column<'a>(env: Env<'a>, s: *mut ffi::sqlite3_stmt, i: c_int) -> Term<'a> {
    // SAFETY: `s` is a live statement positioned on a row and `i` is a column
    // index; a column pointer is read before the next call on `s`.
    unsafe {
        match ffi::sqlite3_column_type(s, i) {
            ffi::SQLITE_INTEGER => ffi::sqlite3_column_int64(s, i).encode(env),
            ffi::SQLITE_FLOAT => ffi::sqlite3_column_double(s, i).encode(env),
            ffi::SQLITE_TEXT | ffi::SQLITE_BLOB => {
                let data = ffi::sqlite3_column_blob(s, i) as *const u8;
                let len = ffi::sqlite3_column_bytes(s, i) as usize;
                if data.is_null() || len == 0 {
                    binary(env, &[])
                } else {
                    binary(env, std::slice::from_raw_parts(data, len))
                }
            }
            _ => atoms::nil().encode(env),
        }
    }
}

fn column_count(s: *mut ffi::sqlite3_stmt) -> c_int {
    // SAFETY: `s` is a live statement.
    unsafe { ffi::sqlite3_column_count(s) }
}

fn row<'a>(env: Env<'a>, s: *mut ffi::sqlite3_stmt) -> Term<'a> {
    let n = column_count(s);
    let cells: Vec<Term> = (0..n).map(|i| column(env, s, i)).collect();
    cells.encode(env)
}

fn column_names<'a>(env: Env<'a>, s: *mut ffi::sqlite3_stmt) -> Term<'a> {
    let n = column_count(s);
    let names: Vec<Term> = (0..n)
        .map(|i| {
            // SAFETY: `s` is a live statement and `i` a column index; the name
            // is NUL-terminated and valid until the next call on `s`.
            unsafe {
                let name = ffi::sqlite3_column_name(s, i);
                if name.is_null() {
                    binary(env, &[])
                } else {
                    binary(env, CStr::from_ptr(name).to_bytes())
                }
            }
        })
        .collect();
    names.encode(env)
}

/// Bytes a row adds to the result: text and blobs by length, 8 for the rest.
fn row_bytes(s: *mut ffi::sqlite3_stmt) -> u64 {
    (0..column_count(s))
        .map(|i| {
            // SAFETY: `s` is a live statement positioned on a row.
            unsafe {
                match ffi::sqlite3_column_type(s, i) {
                    ffi::SQLITE_TEXT | ffi::SQLITE_BLOB => ffi::sqlite3_column_bytes(s, i) as u64,
                    _ => 8,
                }
            }
        })
        .sum()
}

/// Steps to the end and returns every row, up to the limits.
fn all_rows<'a>(
    env: Env<'a>,
    db: *mut ffi::sqlite3,
    s: *mut ffi::sqlite3_stmt,
    ctl: &Control,
    limits: &Limits,
) -> Result<Term<'a>, Failure> {
    let mut rows: Vec<Term> = Vec::new();
    let mut bytes = 0u64;
    loop {
        // SAFETY: `s` is a live statement of `db`.
        match unsafe { ffi::sqlite3_step(s) } {
            ffi::SQLITE_ROW => {
                bytes += row_bytes(s);
                if (limits.max_rows > 0 && rows.len() as u64 >= limits.max_rows)
                    || (limits.max_bytes > 0 && bytes > limits.max_bytes)
                {
                    return Err(Failure::TooLarge);
                }
                rows.push(row(env, s));
            }
            ffi::SQLITE_DONE => return Ok(rows.encode(env)),
            ffi::SQLITE_INTERRUPT if ctl.timed_out.load(Relaxed) => return Err(Failure::Timeout),
            ffi::SQLITE_TOOBIG if limits.max_bytes > 0 => return Err(Failure::TooLarge),
            _ => return Err(failure(db)),
        }
    }
}

/// A length for SQLite, which takes an `int`.
fn c_len(len: usize) -> Result<c_int, Failure> {
    c_int::try_from(len).map_err(|_| Failure::Argument("value over 2 GiB".into()))
}

/// Prepares the first statement of `sql`: the statement (`None` for blank
/// text, comments or a lone `;`) and the bytes it consumed.
fn prepare_at(db: *mut ffi::sqlite3, sql: &[u8]) -> Result<(Option<StmtPtr>, usize), Failure> {
    let len = c_len(sql.len())?;
    let mut s: *mut ffi::sqlite3_stmt = ptr::null_mut();
    let mut tail: *const c_char = ptr::null();
    // SAFETY: `db` is a live connection; `sql` is valid for its length.
    let rc = unsafe {
        ffi::sqlite3_prepare_v2(db, sql.as_ptr() as *const c_char, len, &mut s, &mut tail)
    };
    if rc != ffi::SQLITE_OK {
        return Err(failure(db));
    }
    let used = if tail.is_null() {
        sql.len()
    } else {
        // `tail` points into `sql`.
        (tail as usize - sql.as_ptr() as usize).min(sql.len())
    };
    Ok((if s.is_null() { None } else { Some(StmtPtr(s)) }, used))
}

/// The first statement of `sql` that is not empty, and the bytes up to its
/// end (`;` included). `None` when only blanks, comments and `;` are there.
fn next_statement(db: *mut ffi::sqlite3, sql: &[u8]) -> Result<(Option<StmtPtr>, usize), Failure> {
    let mut at = 0;
    while !sql[at..].iter().all(u8::is_ascii_whitespace) {
        let (stmt, used) = prepare_at(db, &sql[at..])?;
        at += used;
        if stmt.is_some() || used == 0 {
            return Ok((stmt, at));
        }
    }
    Ok((None, sql.len()))
}

/// What of a refused tail the error carries.
const TAIL_SHOWN: usize = 100;

/// Refuses the text after the statement, unless it holds no statement: the
/// rest of a call is never silently ignored.
fn refuse_tail(db: *mut ffi::sqlite3, tail: &[u8]) -> Result<(), Failure> {
    match next_statement(db, tail) {
        Ok((None, _)) => Ok(()),
        Ok((Some(StmtPtr(s)), _)) => {
            finalize(s);
            Err(Failure::tail(tail))
        }
        Err(_) => Err(Failure::tail(tail)),
    }
}

impl Failure {
    /// The start of `text`, trimmed, for an error message.
    fn shown(text: &[u8]) -> String {
        let text = String::from_utf8_lossy(text);
        let text = text.trim();
        let shown: String = text.chars().take(TAIL_SHOWN).collect();
        if shown.len() < text.len() {
            shown + "..."
        } else {
            shown
        }
    }

    /// `Multiple` with the start of `tail`, trimmed.
    fn tail(tail: &[u8]) -> Failure {
        Failure::Multiple(Self::shown(tail))
    }
}

/// Finalizes the statement when dropped.
struct Finalize(*mut ffi::sqlite3_stmt);

impl Drop for Finalize {
    fn drop(&mut self) {
        finalize(self.0);
    }
}

/// Whether the statement that SQLite delimited starts with `CREATE`, `DROP` or
/// `ALTER`, after the blanks and comments SQLite skips (the UTF-8 BOM is one).
/// The text is one statement already, so nothing can follow the keyword that
/// the caller does not see.
fn starts_ddl(text: &[u8]) -> bool {
    let mut i = 0;
    loop {
        match text.get(i..) {
            Some([b' ' | b'\t' | b'\n' | 0x0c | b'\r', ..]) => i += 1,
            Some([0xEF, 0xBB, 0xBF, ..]) => i += 3,
            Some([b'-', b'-', ..]) => {
                i += text[i..]
                    .iter()
                    .position(|&c| c == b'\n')
                    .map_or(text.len() - i, |n| n + 1)
            }
            Some([b'/', b'*', ..]) => {
                i += text[i + 2..]
                    .windows(2)
                    .position(|w| w == b"*/")
                    .map_or(text.len() - i, |n| n + 4)
            }
            _ => break,
        }
    }
    let word: Vec<u8> = text[i.min(text.len())..]
        .iter()
        .take_while(|c| c.is_ascii_alphanumeric() || **c == b'_')
        .map(u8::to_ascii_lowercase)
        .collect();
    matches!(word.as_slice(), b"create" | b"drop" | b"alter")
}

#[cfg(test)]
fn prepare(db: *mut ffi::sqlite3, sql: &[u8]) -> Result<Option<StmtPtr>, Failure> {
    prepare_at(db, sql).map(|(stmt, _)| stmt)
}

/// Binds `params` (a list) to `s`: integers, floats, `nil`, binaries and
/// iodata as text, other atoms as their name, `{:blob, binary | iodata}` as
/// blobs.
fn bind_all(db: *mut ffi::sqlite3, s: *mut ffi::sqlite3_stmt, params: Term) -> Result<(), Failure> {
    let params: Vec<Term> = params
        .decode()
        .map_err(|_| Failure::Argument("parameters must be a list".into()))?;
    // SAFETY: `s` is a live statement.
    let expected = unsafe { ffi::sqlite3_bind_parameter_count(s) } as usize;
    if expected != params.len() {
        return Err(Failure::Argument(format!(
            "expected {expected} arguments, got {}",
            params.len()
        )));
    }
    // SAFETY: as above.
    unsafe { ffi::sqlite3_reset(s) };
    for (i, term) in params.into_iter().enumerate() {
        let rc = bind_one(s, i as c_int + 1, term)?;
        if rc != ffi::SQLITE_OK {
            return Err(Failure::Message(message(db)));
        }
    }
    Ok(())
}

/// A parameter, as SQLite takes it.
enum Value<'a> {
    Int(i64),
    Float(f64),
    Null,
    Text(&'a [u8]),
    Blob(&'a [u8]),
}

fn bind_value(s: *mut ffi::sqlite3_stmt, idx: c_int, value: Value) -> Result<c_int, Failure> {
    // SAFETY: `s` is a live statement; SQLITE_TRANSIENT makes SQLite copy the
    // bytes before the call returns.
    unsafe {
        Ok(match value {
            Value::Int(i) => ffi::sqlite3_bind_int64(s, idx, i),
            Value::Float(f) => ffi::sqlite3_bind_double(s, idx, f),
            Value::Null => ffi::sqlite3_bind_null(s, idx),
            Value::Text(b) => ffi::sqlite3_bind_text(
                s,
                idx,
                b.as_ptr() as *const c_char,
                c_len(b.len())?,
                ffi::SQLITE_TRANSIENT(),
            ),
            Value::Blob(b) => ffi::sqlite3_bind_blob(
                s,
                idx,
                b.as_ptr() as *const _,
                c_len(b.len())?,
                ffi::SQLITE_TRANSIENT(),
            ),
        })
    }
}

fn iodata(term: Term) -> Option<Binary> {
    match term.get_type() {
        TermType::Binary => term.decode().ok(),
        TermType::List => Binary::from_iolist(term).ok(),
        _ => None,
    }
}

fn bind_one(s: *mut ffi::sqlite3_stmt, idx: c_int, term: Term) -> Result<c_int, Failure> {
    let unsupported = |term: Term| Failure::Argument(format!("unsupported type: {term:?}"));
    // Own the bytes that the value borrows.
    let (text, name, blob);
    let value = match term.get_type() {
        TermType::Integer => Value::Int(term.decode().map_err(|_| unsupported(term))?),
        TermType::Float => Value::Float(term.decode().map_err(|_| unsupported(term))?),
        TermType::Binary | TermType::List => {
            text = iodata(term).ok_or_else(|| unsupported(term))?;
            Value::Text(text.as_slice())
        }
        TermType::Atom => {
            let atom: Atom = term.decode().map_err(|_| unsupported(term))?;
            if atom == atoms::nil() || atom == atoms::undefined() {
                Value::Null
            } else {
                name = term.atom_to_string().map_err(|_| unsupported(term))?;
                Value::Text(name.as_bytes())
            }
        }
        TermType::Tuple => {
            let (tag, bytes): (Atom, Term) = term.decode().map_err(|_| unsupported(term))?;
            if tag != atoms::blob() {
                return Err(unsupported(term));
            }
            blob = iodata(bytes).ok_or_else(|| unsupported(term))?;
            Value::Blob(blob.as_slice())
        }
        _ => return Err(unsupported(term)),
    };
    bind_value(s, idx, value)
}

/// Opens `path` (a file name or a `file:` URI). `readonly` opens an existing
/// file for reading; otherwise the file is created when missing. `vfs` names
/// a registered VFS, `None` is SQLite's default. `attach` allows `ATTACH`,
/// `DETACH` and `VACUUM`, which the connection refuses otherwise.
/// `timeout_ms` is the deadline of every `sqlite_run` and `sqlite_execute`
/// call that sets none, 0 for none.
#[rustler::nif(schedule = "DirtyIo")]
fn sqlite_open<'a>(
    env: Env<'a>,
    path: String,
    readonly: bool,
    vfs: Option<String>,
    attach: bool,
    timeout_ms: u64,
) -> Term<'a> {
    let _t = crate::stats::SQLITE_CALLS.time();
    let run = || -> Outcome<'a> {
        let path = CString::new(path).map_err(|_| Failure::Message("invalid file name".into()))?;
        let vfs = vfs
            .map(|v| CString::new(v).map_err(|_| Failure::Message("invalid vfs name".into())))
            .transpose()?;
        let mut flags = ffi::SQLITE_OPEN_URI;
        flags |= if readonly {
            ffi::SQLITE_OPEN_READONLY
        } else {
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE
        };
        let mut db: *mut ffi::sqlite3 = ptr::null_mut();
        // SAFETY: `path` and `vfs` are NUL-terminated and outlive the call.
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                path.as_ptr(),
                &mut db,
                flags,
                vfs.as_ref().map_or(ptr::null(), |v| v.as_ptr()),
            )
        };
        if rc != ffi::SQLITE_OK {
            let failure = if db.is_null() {
                Failure::Message("out of memory".into())
            } else {
                failure(db)
            };
            // SAFETY: `db` is the handle `sqlite3_open_v2` returned.
            unsafe { close_v2(db) };
            return Err(failure);
        }
        // SAFETY: `db` is a live connection that nothing else uses yet.
        unsafe {
            // Match the options the cells were built with: no double-quoted
            // string literals.
            let mut out: c_int = 0;
            ffi::sqlite3_db_config(
                db,
                ffi::SQLITE_DBCONFIG_DQS_DML,
                0 as c_int,
                &mut out as *mut c_int,
            );
            ffi::sqlite3_db_config(
                db,
                ffi::SQLITE_DBCONFIG_DQS_DDL,
                0 as c_int,
                &mut out as *mut c_int,
            );
            // Only an explicit checkpoint moves WAL frames into the file
            // (design §6): not closing, which also happens when the
            // connection is garbage collected or its owner crashes, and not
            // the automatic checkpoint after a commit.
            ffi::sqlite3_db_config(
                db,
                ffi::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
                1 as c_int,
                &mut out as *mut c_int,
            );
            ffi::sqlite3_wal_autocheckpoint(db, 0);
            // Hardening against a hostile file or statement: no writes to
            // the schema or shadow tables, no unsafe functions in views and
            // triggers, no extension loading.
            for (op, on) in [
                (ffi::SQLITE_DBCONFIG_DEFENSIVE, 1 as c_int),
                (ffi::SQLITE_DBCONFIG_TRUSTED_SCHEMA, 0),
                (ffi::SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 0),
            ] {
                ffi::sqlite3_db_config(db, op, on, &mut out as *mut c_int);
            }
        }
        CONNECTIONS_OPEN.fetch_add(1, Relaxed);
        let conn = ResourceArc::new(Conn {
            db: RwLock::new(Some(DbPtr(db))),
            control: Box::new(Control::new(attach, timeout_ms)),
        });
        let ctx = &*conn.control as *const Control as *mut std::ffi::c_void;
        // SAFETY: `db` is live and not shared yet; `ctx` outlives it (see
        // `Control`).
        unsafe {
            ffi::sqlite3_set_authorizer(db, Some(authorize), ctx);
            ffi::sqlite3_progress_handler(db, PROGRESS_OPS, Some(progress), ctx);
            ffi::sqlite3_wal_hook(db, Some(wal_committed), ctx);
        }
        Ok((atoms::ok(), conn).encode(env))
    };
    reply(env, run())
}

/// Closes the connection. Statements that are still alive report
/// `:connection_closed`.
#[rustler::nif(schedule = "DirtyIo")]
fn sqlite_close(conn: ResourceArc<Conn>) -> Atom {
    let _t = crate::stats::SQLITE_CALLS.time();
    // Closing ends everything: stop the running statement first, because a
    // waiting writer keeps `sqlite_interrupt` from taking the read lock.
    interrupt(&conn);
    if let Some(DbPtr(db)) = conn.db.write().unwrap_or_else(|e| e.into_inner()).take() {
        // SAFETY: the write lock waited for every user; the pointer is out of
        // the slot, so nothing can use it again.
        unsafe { close(db) };
    }
    atoms::ok()
}

/// Runs one or more statements without reading rows. `no_txn_control` says
/// the text is a user's: transaction control is refused (see `authorize`).
#[rustler::nif(schedule = "DirtyIo")]
fn sqlite_execute<'a>(
    env: Env<'a>,
    conn: ResourceArc<Conn>,
    sql: Binary,
    no_txn_control: bool,
) -> Term<'a> {
    let _t = crate::stats::SQLITE_CALLS.time();
    let run = || -> Outcome<'a> {
        let sql = CString::new(sql.as_slice()).map_err(|_| Failure::NulInStatement)?;
        with_db(&conn, |db| {
            check_transaction(&conn.control, db)?;
            let limits = Limits {
                no_txn_control,
                ..Limits::default()
            };
            let _call = Call::new(&conn.control, db, &limits);
            // SAFETY: `db` is live and `sql` is NUL-terminated.
            let rc = unsafe {
                ffi::sqlite3_exec(db, sql.as_ptr(), None, ptr::null_mut(), ptr::null_mut())
            };
            if rc == ffi::SQLITE_OK {
                Ok(atoms::ok().encode(env))
            } else if rc == ffi::SQLITE_INTERRUPT && conn.control.timed_out.load(Relaxed) {
                Err(Failure::Timeout)
            } else {
                Err(failure(db))
            }
        })
    };
    reply(env, run())
}

/// Prepares, binds, runs and finalizes one statement: `{:ok, columns, rows}`.
/// Text after the statement that holds another statement is refused with
/// `{:error, {:multiple_statements, tail}}` before anything runs.
/// `limits` is `{read_only, timeout_ms, max_rows, max_bytes, max_concurrent,
/// no_txn_control}`, 0 for no limit (`no_txn_control` as in `sqlite_execute`); over `max_concurrent` user statements running the call
/// gives `{:error, :busy}` without touching the connection.
#[rustler::nif(schedule = "DirtyIo")]
fn sqlite_run<'a>(
    env: Env<'a>,
    conn: ResourceArc<Conn>,
    sql: Binary,
    params: Term<'a>,
    limits: (bool, u64, u64, u64, u64, bool),
) -> Term<'a> {
    let _t = crate::stats::SQLITE_CALLS.time();
    let max_concurrent = limits.4;
    let limits = Limits {
        read_only: limits.0,
        no_txn_control: limits.5,
        timeout_ms: limits.1,
        max_rows: limits.2,
        max_bytes: limits.3,
    };
    let run = || -> Outcome<'a> {
        if sql.as_slice().contains(&0) {
            return Err(Failure::NulInStatement);
        }
        let _slot = Slot::take(max_concurrent)?;
        with_db(&conn, |db| {
            check_transaction(&conn.control, db)?;
            let _call = Call::new(&conn.control, db, &limits);
            let sql = sql.as_slice();
            let (stmt, end) = next_statement(db, sql)?;
            let Some(StmtPtr(s)) = stmt else {
                return Ok((atoms::ok(), Vec::<Term>::new(), Vec::<Term>::new()).encode(env));
            };
            let _guard = Finalize(s);
            refuse_tail(db, &sql[end..])?;
            bind_all(db, s, params)?;
            let columns = column_names(env, s);
            let rows = all_rows(env, db, s, &conn.control, &limits)?;
            Ok((atoms::ok(), columns, rows).encode(env))
        })
    };
    reply(env, run())
}

/// Runs a user's script of schema statements, one at a time as SQLite
/// delimits them (the tail of each prepare), without reading rows. A statement
/// that is not `CREATE`, `DROP` or `ALTER` is refused before it runs with
/// `{:error, {:not_ddl, statement}}` and nothing after it runs. Transaction
/// control is refused as in a call with `no_txn_control`.
#[rustler::nif(schedule = "DirtyIo")]
fn sqlite_script<'a>(env: Env<'a>, conn: ResourceArc<Conn>, sql: Binary) -> Term<'a> {
    let _t = crate::stats::SQLITE_CALLS.time();
    let run = || -> Outcome<'a> {
        let sql = sql.as_slice();
        if sql.contains(&0) {
            return Err(Failure::NulInStatement);
        }
        with_db(&conn, |db| {
            let limits = Limits {
                no_txn_control: true,
                ..Limits::default()
            };
            let _call = Call::new(&conn.control, db, &limits);
            let mut at = 0;
            while at < sql.len() {
                check_transaction(&conn.control, db)?;
                let (stmt, used) = prepare_at(db, &sql[at..])?;
                let text = &sql[at..at + used];
                at += used;
                let Some(StmtPtr(s)) = stmt else {
                    if used == 0 {
                        break;
                    }
                    continue;
                };
                let _guard = Finalize(s);
                if !starts_ddl(text) {
                    return Err(Failure::NotDdl(Failure::shown(text)));
                }
                step_to_end(db, s, &conn.control)?;
            }
            Ok(atoms::ok().encode(env))
        })
    };
    reply(env, run())
}

/// Steps `s` to its end, dropping the rows.
fn step_to_end(
    db: *mut ffi::sqlite3,
    s: *mut ffi::sqlite3_stmt,
    ctl: &Control,
) -> Result<(), Failure> {
    loop {
        // SAFETY: `s` is a live statement of `db`.
        match unsafe { ffi::sqlite3_step(s) } {
            ffi::SQLITE_ROW => {}
            ffi::SQLITE_DONE => return Ok(()),
            ffi::SQLITE_INTERRUPT if ctl.timed_out.load(Relaxed) => return Err(Failure::Timeout),
            _ => return Err(failure(db)),
        }
    }
}

/// Says whether a transaction of the library is open on the connection: while
/// it is, `sqlite_run` and `sqlite_execute` answer `{:error, :transaction_ended}`
/// without running when SQLite has ended the transaction. Off at the final
/// `COMMIT` or `ROLLBACK`.
#[rustler::nif]
fn sqlite_expect_transaction(conn: ResourceArc<Conn>, on: bool) -> Atom {
    if on {
        let frames = conn.control.wal_frames.load(Relaxed);
        conn.control.txn_frames.store(frames, Relaxed);
    }
    conn.control.expect_txn.store(on, Relaxed);
    atoms::ok()
}

/// Whether a commit reached the WAL since the transaction of the library
/// began (`sqlite_expect_transaction` with `true`): frames that no write of
/// the cell accounts for when that transaction failed.
#[rustler::nif]
fn sqlite_wal_advanced(conn: ResourceArc<Conn>) -> bool {
    let ctl = &conn.control;
    ctl.wal_frames.load(Relaxed) != ctl.txn_frames.load(Relaxed)
}

/// User statements running now (`sqlite_run` calls with a `max_concurrent`).
#[rustler::nif]
fn sqlite_user_statements() -> u64 {
    USER_STATEMENTS.load(Acquire)
}

/// Makes the statement that is running on this connection stop with
/// "interrupted". Safe to call from any process while another one steps.
#[rustler::nif]
fn sqlite_interrupt(conn: ResourceArc<Conn>) -> Atom {
    interrupt(&conn);
    atoms::ok()
}

fn interrupt(conn: &Conn) {
    // `try_read`: a close that is waiting for the step must not block us.
    if let Ok(guard) = conn.db.try_read() {
        if let Some(DbPtr(db)) = guard.as_ref() {
            // SAFETY: the read lock keeps `close` from freeing `db`;
            // `sqlite3_interrupt` is safe while another thread steps.
            unsafe { ffi::sqlite3_interrupt(*db) };
        }
    }
}

/// Copies every WAL frame into the database file and truncates the WAL
/// (`SQLITE_CHECKPOINT_TRUNCATE`): `:ok`, or `{:error, :busy}` when a reader
/// kept it from finishing.
#[rustler::nif(schedule = "DirtyIo")]
fn sqlite_checkpoint<'a>(env: Env<'a>, conn: ResourceArc<Conn>) -> Term<'a> {
    let _t = crate::stats::SQLITE_CALLS.time();
    reply(
        env,
        with_db(&conn, |db| {
            let (mut log, mut done) = (0, 0);
            // SAFETY: `db` is live (`with_db`); the out pointers are locals.
            let rc = unsafe {
                ffi::sqlite3_wal_checkpoint_v2(
                    db,
                    ptr::null(),
                    ffi::SQLITE_CHECKPOINT_TRUNCATE,
                    &mut log,
                    &mut done,
                )
            };
            match rc {
                ffi::SQLITE_OK => {
                    // The WAL starts over: the count must not equal a later one by chance.
                    conn.control.wal_frames.store(0, Relaxed);
                    Ok(atoms::ok().encode(env))
                }
                ffi::SQLITE_BUSY => Ok((atoms::error(), atoms::busy()).encode(env)),
                _ => Err(failure(db)),
            }
        }),
    )
}

/// Rows changed by the latest statement that changed rows.
#[rustler::nif]
fn sqlite_changes<'a>(env: Env<'a>, conn: ResourceArc<Conn>) -> Term<'a> {
    reply(
        env,
        with_db(&conn, |db| {
            // SAFETY: `db` is live (`with_db`).
            Ok((atoms::ok(), unsafe { ffi::sqlite3_changes64(db) }).encode(env))
        }),
    )
}

/// `:transaction` inside an open transaction, `:idle` otherwise.
#[rustler::nif]
fn sqlite_transaction_status<'a>(env: Env<'a>, conn: ResourceArc<Conn>) -> Term<'a> {
    reply(
        env,
        with_db(&conn, |db| {
            // SAFETY: `db` is live (`with_db`).
            let status = if unsafe { ffi::sqlite3_get_autocommit(db) } == 0 {
                atoms::transaction()
            } else {
                atoms::idle()
            };
            Ok((atoms::ok(), status).encode(env))
        }),
    )
}

/// Bytes the connection holds, by purpose (`sqlite3_db_status`): the page
/// cache, the schema, the prepared statements and the lookaside slots in use.
#[rustler::nif]
fn sqlite_memory<'a>(env: Env<'a>, conn: ResourceArc<Conn>) -> Term<'a> {
    reply(
        env,
        with_db(&conn, |db| {
            let used = |op| {
                let (mut cur, mut high) = (0, 0);
                // SAFETY: `db` is live (`with_db`).
                unsafe { ffi::sqlite3_db_status(db, op, &mut cur, &mut high, 0) };
                cur as i64
            };
            let map = [
                (atoms::cache_used(), ffi::SQLITE_DBSTATUS_CACHE_USED),
                (atoms::schema_used(), ffi::SQLITE_DBSTATUS_SCHEMA_USED),
                (atoms::stmt_used(), ffi::SQLITE_DBSTATUS_STMT_USED),
                (atoms::lookaside_used(), ffi::SQLITE_DBSTATUS_LOOKASIDE_USED),
            ]
            .into_iter()
            .fold(Term::map_new(env), |map, (key, op)| {
                map.map_put(key.encode(env), used(op).encode(env)).unwrap()
            });
            Ok((atoms::ok(), map).encode(env))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Opens an in-memory database and runs `sql` for its side effects.
    fn mem() -> *mut ffi::sqlite3 {
        let mut db = ptr::null_mut();
        let name = CString::new(":memory:").unwrap();
        // SAFETY: `name` is NUL-terminated and outlives the call.
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                name.as_ptr(),
                &mut db,
                ffi::SQLITE_OPEN_READWRITE,
                ptr::null(),
            )
        };
        assert_eq!(rc, ffi::SQLITE_OK);
        db
    }

    #[test]
    #[cfg_attr(miri, ignore = "calls SQLite")]
    fn bundled_build_options() {
        let db = mem();
        // No default foreign keys, math functions present, double quotes are
        // identifiers only.
        for (sql, expected) in [
            ("PRAGMA foreign_keys", 0i64),
            ("SELECT floor(2.5)", 2),
            ("SELECT sqlite_compileoption_used('DQS=0')", 1),
        ] {
            let s = prepare(db, sql.as_bytes()).ok().flatten().expect(sql).0;
            // SAFETY: `s` is a live statement of `db`.
            let (rc, value) = unsafe { (ffi::sqlite3_step(s), ffi::sqlite3_column_int64(s, 0)) };
            assert_eq!(rc, ffi::SQLITE_ROW, "{sql}");
            assert_eq!(value, expected, "{sql}");
            finalize(s);
        }
        // SAFETY: `db` is live and not used afterwards.
        unsafe { close_v2(db) };
    }

    #[test]
    fn pragmas_follow_the_mode() {
        for (name, arg, read_only, allowed) in [
            ("wal_checkpoint", None, false, false),
            ("wal_autocheckpoint", Some("1"), false, false),
            ("wal_autocheckpoint", None, true, true),
            ("journal_mode", Some("DELETE"), false, false),
            ("Journal_Mode", Some("wal"), false, true),
            ("journal_mode", Some("wal"), true, false),
            ("foreign_keys", Some("0"), false, true),
            ("foreign_keys", Some("0"), true, false),
            ("foreign_keys", None, true, true),
            ("TABLE_INFO", Some("t"), true, true),
            ("user_version", Some("9"), true, false),
        ] {
            let allowed_now = pragma_allowed(name.as_bytes(), arg.map(str::as_bytes), read_only);
            assert_eq!(allowed_now, allowed, "{name} {arg:?} read_only={read_only}");
        }
    }

    /// Runs `sql` on a database whose authorizer and progress handler follow
    /// `ctl`; `Ok` when it was prepared and ran.
    fn run(db: *mut ffi::sqlite3, sql: &str) -> Result<(), ()> {
        let s = prepare(db, sql.as_bytes()).map_err(|_| ())?;
        let Some(StmtPtr(s)) = s else { return Ok(()) };
        // SAFETY: `s` is a live statement of `db`.
        let rc = unsafe { ffi::sqlite3_step(s) };
        finalize(s);
        if rc == ffi::SQLITE_ROW || rc == ffi::SQLITE_DONE {
            Ok(())
        } else {
            Err(())
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "calls SQLite")]
    fn the_authorizer_reads_the_connection_mode() {
        let db = mem();
        let ctl = Box::new(Control::new(false, 0));
        let ctx = &*ctl as *const Control as *mut std::ffi::c_void;
        // SAFETY: `db` is live and `ctl` outlives it.
        unsafe { ffi::sqlite3_set_authorizer(db, Some(authorize), ctx) };

        assert!(run(db, "CREATE TABLE t (a)").is_ok());
        assert!(run(db, "BEGIN").is_ok());
        assert!(run(db, "ROLLBACK").is_ok());
        assert!(run(db, "ATTACH ':memory:' AS x").is_err());
        assert!(run(db, "VACUUM").is_err());

        ctl.read_only.store(true, Relaxed);
        assert!(run(db, "SELECT * FROM t").is_ok());
        assert!(run(db, "BEGIN").is_err());
        assert!(run(db, "SAVEPOINT s").is_err());
        assert!(run(db, "PRAGMA recursive_triggers(0)").is_err());
        assert!(run(db, "PRAGMA table_info(t)").is_ok());
        assert!(run(db, "SELECT * FROM pragma_table_info('t')").is_ok());
        ctl.read_only.store(false, Relaxed);
        assert!(run(db, "PRAGMA recursive_triggers(0)").is_ok());

        // A user's text cannot end the transaction; savepoints are allowed.
        ctl.no_txn_control.store(true, Relaxed);
        for sql in [
            "BEGIN",
            "BEGIN IMMEDIATE",
            "COMMIT",
            "END",
            "END TRANSACTION",
            "ROLLBACK",
        ] {
            assert!(run(db, sql).is_err(), "{sql}");
        }
        for sql in [
            "SAVEPOINT s",
            "ROLLBACK TO s",
            "RELEASE s",
            "INSERT INTO t VALUES (1)",
        ] {
            assert!(run(db, sql).is_ok(), "{sql}");
        }
        ctl.no_txn_control.store(false, Relaxed);
        assert!(run(db, "BEGIN").is_ok());
        assert!(run(db, "ROLLBACK").is_ok());

        // While a transaction of the library is open, no call may control it.
        assert!(run(db, "BEGIN").is_ok());
        ctl.expect_txn.store(true, Relaxed);
        for sql in ["BEGIN", "COMMIT", "END", "ROLLBACK"] {
            assert!(run(db, sql).is_err(), "{sql}");
        }
        assert!(run(db, "SAVEPOINT s").is_ok());
        assert!(run(db, "RELEASE s").is_ok());
        ctl.expect_txn.store(false, Relaxed);
        assert!(run(db, "COMMIT").is_ok());

        // SAFETY: `db` is live and not used afterwards.
        unsafe { close_v2(db) };
    }

    type Seen = Vec<(c_int, Option<String>, Option<String>)>;

    unsafe extern "C" fn record(
        ctx: *mut std::ffi::c_void,
        action: c_int,
        a: *const c_char,
        b: *const c_char,
        _: *const c_char,
        _: *const c_char,
    ) -> c_int {
        let text = |p: *const c_char| {
            // SAFETY: SQLite passes NUL-terminated strings or null.
            (!p.is_null()).then(|| unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
        };
        // SAFETY: the context is the `Seen` of the test.
        unsafe { &mut *(ctx as *mut Seen) }.push((action, text(a), text(b)));
        ffi::SQLITE_OK
    }

    /// The layout `authorize` relies on: transaction control is
    /// `SQLITE_TRANSACTION` named BEGIN, COMMIT or ROLLBACK (END is a COMMIT);
    /// savepoints are `SQLITE_SAVEPOINT` named BEGIN, RELEASE or ROLLBACK
    /// with the savepoint's name second.
    #[test]
    #[cfg_attr(miri, ignore = "calls SQLite")]
    fn the_authorizer_arguments_of_transaction_control() {
        let db = mem();
        let mut seen: Seen = Vec::new();
        let ctx = &mut seen as *mut Seen as *mut std::ffi::c_void;
        // SAFETY: `db` is live and `seen` outlives it.
        unsafe { ffi::sqlite3_set_authorizer(db, Some(record), ctx) };
        let name = |s: &str| Some(s.to_string());
        let txn = ffi::SQLITE_TRANSACTION;
        let sp = ffi::SQLITE_SAVEPOINT;

        for (sql, expected) in [
            ("BEGIN", (txn, name("BEGIN"), None)),
            ("COMMIT", (txn, name("COMMIT"), None)),
            ("END", (txn, name("COMMIT"), None)),
            ("ROLLBACK", (txn, name("ROLLBACK"), None)),
            ("SAVEPOINT s", (sp, name("BEGIN"), name("s"))),
            ("RELEASE s", (sp, name("RELEASE"), name("s"))),
            ("ROLLBACK TO s", (sp, name("ROLLBACK"), name("s"))),
        ] {
            seen.clear();
            let _ = run(db, sql);
            assert_eq!(seen.first(), Some(&expected), "{sql}");
        }
        // SAFETY: `db` is live and not used afterwards.
        unsafe { close_v2(db) };
    }

    #[test]
    #[cfg_attr(miri, ignore = "calls SQLite")]
    fn the_progress_handler_stops_a_statement_at_its_deadline() {
        let db = mem();
        let ctl = Box::new(Control::new(false, 0));
        let ctx = &*ctl as *const Control as *mut std::ffi::c_void;
        // SAFETY: `db` is live and `ctl` outlives it.
        unsafe { ffi::sqlite3_progress_handler(db, PROGRESS_OPS, Some(progress), ctx) };
        let sql = "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n \
                   WHERE x < 1000000000) SELECT count(*) FROM n";

        assert!(run(db, "SELECT 1").is_ok());
        ctl.deadline.store(now_us() + 50_000, Relaxed);
        let started = Instant::now();
        assert!(run(db, sql).is_err());
        assert!(ctl.timed_out.load(Relaxed));
        assert!(started.elapsed().as_secs() < 5);

        ctl.deadline.store(0, Relaxed);
        assert!(run(db, "SELECT 1").is_ok());
        // SAFETY: `db` is live and not used afterwards.
        unsafe { close_v2(db) };
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(24))]

        /// Whichever way the deadline is set, the call of the statement
        /// (`Call`) arms it: a call's own, or the connection's when the call
        /// sets none (migrations, scripts and the library's own statements).
        /// The statement ends near it and never runs on.
        #[test]
        #[cfg_attr(miri, ignore = "calls SQLite")]
        fn no_statement_runs_past_its_deadline(
            call_ms in proptest::option::of(20u64..120),
            default_ms in 0u64..120,
        ) {
            let expected = call_ms.unwrap_or(default_ms);
            proptest::prop_assume!(expected > 0);
            let db = mem();
            let ctl = Box::new(Control::new(false, default_ms));
            let ctx = &*ctl as *const Control as *mut std::ffi::c_void;
            // SAFETY: `db` is live and `ctl` outlives it.
            unsafe { ffi::sqlite3_progress_handler(db, PROGRESS_OPS, Some(progress), ctx) };
            let limits = Limits { timeout_ms: call_ms.unwrap_or(0), ..Limits::default() };

            let started = Instant::now();
            let ran = {
                let _call = Call::new(&ctl, db, &limits);
                run(db, "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n \
                         WHERE x < 1000000000) SELECT count(*) FROM n")
            };
            let took = started.elapsed().as_millis() as u64;

            proptest::prop_assert!(ran.is_err());
            proptest::prop_assert!(ctl.timed_out.load(Relaxed));
            proptest::prop_assert!(took >= expected, "stopped after {took} ms, before {expected}");
            proptest::prop_assert!(took < expected + 1000, "ran {took} ms for {expected}");
            // SAFETY: `db` is live and not used afterwards.
            unsafe { close_v2(db) };
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "calls SQLite")]
    fn only_blank_text_may_follow_a_statement() {
        let db = mem();
        let next = |sql: &str| next_statement(db, sql.as_bytes()).ok().expect(sql);
        let kinds = |sql: &str| {
            let (stmt, end) = next(sql);
            let tail = &sql.as_bytes()[end..];
            let found = stmt.map(|StmtPtr(s)| finalize(s)).is_some();
            (found, refuse_tail(db, tail).is_ok())
        };
        for (sql, statement, tail_ok) in [
            ("SELECT 1", true, true),
            ("SELECT 1;", true, true),
            ("SELECT 1; \n", true, true),
            ("SELECT 1; -- c", true, true),
            ("SELECT 1 /* c */; ; /* d */", true, true),
            ("; SELECT 1", true, true),
            (";;; /* c */ SELECT 1; ;", true, true),
            ("", false, true),
            (" ; -- c", false, true),
            ("SELECT 1; SELECT 2", true, false),
            ("SELECT 1; nonsense", true, false),
            ("SELECT 1; COMMIT", true, false),
        ] {
            assert_eq!(kinds(sql), (statement, tail_ok), "{sql:?}");
        }
        let long = format!("SELECT 1; SELECT '{}'", "x".repeat(500));
        let end = next(&long).1;
        match refuse_tail(db, &long.as_bytes()[end..]) {
            Err(Failure::Multiple(tail)) => {
                assert!(tail.len() <= TAIL_SHOWN + 3 && tail.ends_with("..."))
            }
            _ => panic!("tail accepted"),
        }
        // SAFETY: `db` is live and not used afterwards.
        unsafe { close_v2(db) };
    }

    #[test]
    fn a_schema_statement_starts_with_create_drop_or_alter() {
        for (sql, ddl) in [
            ("CREATE TABLE t (a)", true),
            ("  drop table t", true),
            ("-- c\nAlter TABLE t ADD b", true),
            ("/* x */ create /* y */ index i on t(a)", true),
            ("\u{FEFF}CREATE TABLE t (a)", true),
            (
                "\u{FEFF}\u{FEFF} /* c */ \u{FEFF}create view v as select 1",
                true,
            ),
            ("INSERT INTO t VALUES (1)", false),
            ("WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c", false),
            ("EXPLAIN CREATE TABLE t (a)", false),
            ("createx", false),
            ("create_table", false),
            ("COMMIT", false),
            ("", false),
            ("/* create */ SELECT 1", false),
            ("-- create", false),
            ("/* create", false),
        ] {
            assert_eq!(starts_ddl(sql.as_bytes()), ddl, "{sql:?}");
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "calls SQLite")]
    fn a_statement_does_not_run_once_the_expected_transaction_ended() {
        let db = mem();
        let ctl = Control::new(false, 0);
        assert!(check_transaction(&ctl, db).is_ok());
        assert!(run(db, "CREATE TABLE t (a)").is_ok());
        assert!(run(db, "BEGIN").is_ok());
        ctl.expect_txn.store(true, Relaxed);
        assert!(check_transaction(&ctl, db).is_ok());
        assert!(run(db, "COMMIT").is_ok());
        assert!(matches!(
            check_transaction(&ctl, db),
            Err(Failure::TransactionEnded)
        ));
        ctl.expect_txn.store(false, Relaxed);
        assert!(check_transaction(&ctl, db).is_ok());
        // SAFETY: `db` is live and not used afterwards.
        unsafe { close_v2(db) };
    }

    #[test]
    fn lengths_over_an_int_are_refused() {
        assert!(c_len(c_int::MAX as usize).is_ok());
        assert!(c_len(c_int::MAX as usize + 1).is_err());
    }
}
