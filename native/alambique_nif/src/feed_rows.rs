//! `sqlite_feed_rows`: reads the captured rows of the change feed and builds
//! the text of their records, which it sends to the feed sender (design §13).
//!
//! The text is what `Alambique.Feed.Record` splices for the rows of a batch:
//! `{"n":N,"table":<JSON string>,"op":"<op>","key":<key>,"values":<values>}`
//! per row, joined by commas, one binary per batch. `key` and `values` are the
//! column bytes as SQLite holds them, never decoded: invalid UTF-8 is replaced
//! over the whole object body later, in Elixir. The binaries live in a
//! separately allocated environment, so they are reference-counted and never
//! on the heap of the cell that called.

use std::io::Write;
use std::sync::atomic::Ordering::Relaxed;

use libsqlite3_sys as ffi;
use rustler::{Encoder, Env, LocalPid, OwnedEnv, ResourceArc, Term};

use super::{
    binary, bind_all, check_transaction, check_user, failure, next_statement, refuse_tail, reply,
    with_db, Call, Conn, Control, Failure, Finalize, Limits, Outcome, StmtPtr,
};

mod atoms {
    rustler::atoms! { ok, rows }
}

/// What the cell asked for, besides the statement.
struct Plan {
    /// Rows at most; `None`: all.
    bound: Option<u64>,
    /// The `n` of the next row when the first batch read goes on one that
    /// began in an earlier call; `None` when it starts a batch.
    resumed: Option<u64>,
    /// Batches that may begin; `None`: any number. The batch of `resumed`
    /// takes none.
    room: Option<u64>,
}

/// The rows of one batch (one TXID) in one call.
struct Group {
    txid: i64,
    ts: i64,
    rows: u64,
    last_seq: i64,
    /// The `n` of the next row.
    next_n: u64,
    body: Vec<u8>,
    /// The call read the start of the batch.
    first: bool,
    /// More rows of the batch follow the ones read.
    partial: bool,
    /// The batch is the marker of a rewind: its row is not a record.
    rewind: bool,
}

struct Read {
    groups: Vec<Group>,
    /// Rows were left, because of the bound or the room.
    more: bool,
    /// The room, not the bound, ended the read.
    cut: bool,
}

fn int(s: *mut ffi::sqlite3_stmt, i: i32) -> i64 {
    // SAFETY: `s` is a live statement positioned on a row.
    unsafe { ffi::sqlite3_column_int64(s, i) }
}

/// The bytes of a text or blob column, `None` for SQL NULL. An empty text is
/// `Some(&[])`: the pointer SQLite gives for it may be null.
fn bytes<'s>(s: *mut ffi::sqlite3_stmt, i: i32) -> Option<&'s [u8]> {
    // SAFETY: `s` is a live statement positioned on a row; the pointer and
    // the length are read before the next call on `s` and the slice is used
    // before it (the caller copies it).
    unsafe {
        if ffi::sqlite3_column_type(s, i) == ffi::SQLITE_NULL {
            return None;
        }
        let data = ffi::sqlite3_column_blob(s, i) as *const u8;
        let len = ffi::sqlite3_column_bytes(s, i) as usize;
        if data.is_null() || len == 0 {
            Some(&[])
        } else {
            Some(std::slice::from_raw_parts(data, len))
        }
    }
}

/// `Jason.encode!/1` of a valid UTF-8 string, without the quotes: `"` and `\`
/// escaped, `\b \t \n \f \r` short, other bytes under 0x20 as `\u00XX` in
/// upper case; `/`, 0x7F and U+2028 stay as they are.
fn escape(out: &mut Vec<u8>, text: &str) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &b in text.as_bytes() {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x09 => out.extend_from_slice(b"\\t"),
            0x0a => out.extend_from_slice(b"\\n"),
            0x0c => out.extend_from_slice(b"\\f"),
            0x0d => out.extend_from_slice(b"\\r"),
            0x00..=0x1f => {
                out.extend_from_slice(b"\\u00");
                out.push(HEX[(b >> 4) as usize]);
                out.push(HEX[(b & 15) as usize]);
            }
            _ => out.push(b),
        }
    }
}

/// Appends the record of one row to the text of its batch. A table name that
/// is not UTF-8 is what `Jason.encode!/1` raises on: `BadTable`.
fn push_row(
    out: &mut Vec<u8>,
    n: u64,
    table: &[u8],
    op: &[u8],
    key: &[u8],
    values: Option<&[u8]>,
) -> Result<(), Failure> {
    let table = std::str::from_utf8(table).map_err(|_| Failure::BadTable)?;
    if !out.is_empty() {
        out.push(b',');
    }
    // Writing to a `Vec` does not fail.
    let _ = write!(out, "{{\"n\":{n},\"table\":\"");
    escape(out, table);
    out.extend_from_slice(b"\",\"op\":\"");
    out.extend_from_slice(op);
    out.extend_from_slice(b"\",\"key\":");
    out.extend_from_slice(key);
    out.extend_from_slice(b",\"values\":");
    out.extend_from_slice(values.unwrap_or(b"null"));
    out.push(b'}');
    Ok(())
}

/// Steps the statement (`txid, ts, tbl, op, key, vals, seq` in order of
/// `txid, seq`) and builds the batches. The bound is on rows and is checked
/// before the room, as the cell does when it trims what it read; a batch cut
/// by the bound is `partial` when its next row is the one left over.
fn read_groups(
    db: *mut ffi::sqlite3,
    ctl: &Control,
    s: *mut ffi::sqlite3_stmt,
    plan: &Plan,
) -> Result<Read, Failure> {
    let mut read = Read {
        groups: Vec::new(),
        more: false,
        cut: false,
    };
    let (mut taken, mut fresh) = (0u64, 0u64);
    loop {
        // SAFETY: `s` is a live statement of `db`.
        match unsafe { ffi::sqlite3_step(s) } {
            ffi::SQLITE_ROW => {
                let txid = int(s, 0);
                let same = read.groups.last().is_some_and(|g| g.txid == txid);
                if plan.bound.is_some_and(|bound| taken >= bound) {
                    read.more = true;
                    if let Some(last) = read.groups.last_mut() {
                        last.partial = same;
                    }
                    return Ok(read);
                }
                if !same {
                    let resumed = read.groups.is_empty() && plan.resumed.is_some();
                    if !resumed {
                        if plan.room.is_some_and(|room| fresh >= room) {
                            read.more = true;
                            read.cut = true;
                            return Ok(read);
                        }
                        fresh += 1;
                    }
                    read.groups.push(Group {
                        txid,
                        ts: int(s, 1),
                        rows: 0,
                        last_seq: 0,
                        next_n: if resumed {
                            plan.resumed.unwrap_or(0)
                        } else {
                            0
                        },
                        body: Vec::new(),
                        first: !resumed,
                        partial: false,
                        rewind: false,
                    });
                }
                taken += 1;
                add_row(s, &mut read.groups)?;
            }
            ffi::SQLITE_DONE => return Ok(read),
            ffi::SQLITE_INTERRUPT if ctl.timed_out.load(Relaxed) => return Err(Failure::Timeout),
            _ => return Err(failure(db)),
        }
    }
}

/// Adds the row the statement is on to the last group.
fn add_row(s: *mut ffi::sqlite3_stmt, groups: &mut [Group]) -> Result<(), Failure> {
    let Some(group) = groups.last_mut() else {
        return Err(Failure::Message("a row without a batch".into()));
    };
    let table = bytes(s, 2).unwrap_or(&[]);
    let op = bytes(s, 3).unwrap_or(&[]);
    let key = bytes(s, 4).unwrap_or(&[]);
    let values = bytes(s, 5);
    group.last_seq = int(s, 6);
    if group.rewind {
        return Err(Failure::Message("a rewind batch holds more rows".into()));
    }
    // The marker of a rewind is the only row of its batch and goes out as it is.
    if group.rows == 0 && group.first && table.is_empty() && op == b"rewind" {
        group.rewind = true;
        group.body.extend_from_slice(values.unwrap_or(b"null"));
    } else {
        push_row(&mut group.body, group.next_n, table, op, key, values)?;
        group.next_n += 1;
    }
    group.rows += 1;
    Ok(())
}

/// Sends `{:rows, token, [binary]}` to `pid`: one binary per batch.
///
/// The message is built in an environment of its own, so the binaries are
/// reference-counted outside every heap, and sent with the environment of the
/// calling process: `OwnedEnv::send_and_clear` panics on a thread of the VM,
/// and `Env::send` would build the binaries on the heap of the caller.
fn deliver(env: Env, pid: &LocalPid, token: Term, groups: &[Group]) -> bool {
    let message_env = OwnedEnv::new();
    let (raw_env, message) = message_env.run(|menv| {
        let bodies: Vec<Term> = groups.iter().map(|g| binary(menv, &g.body)).collect();
        let message = (atoms::rows(), token.in_env(menv), bodies).encode(menv);
        (menv.as_c_arg(), message.as_c_arg())
    });
    // SAFETY: `env` is the environment of the process running this NIF and
    // this is a thread of the VM, as `enif_send` requires; `pid` is a local
    // pid; `raw_env` is a process-independent environment that holds
    // `message`, and nothing else uses it. `enif_send` empties it; dropping
    // `message_env` frees it.
    unsafe { rustler::sys::enif_send(env.as_c_arg(), pid.as_c_arg(), raw_env, message) == 1 }
}

/// `{txid, ts, rows, last_seq, byte_len, {first, partial, rewind}}` of a batch.
fn describe<'a>(env: Env<'a>, g: &Group) -> Term<'a> {
    (
        g.txid,
        g.ts,
        g.rows,
        g.last_seq,
        g.body.len(),
        (g.first, g.partial, g.rewind),
    )
        .encode(env)
}

/// Runs `sql`, which selects `txid, ts, tbl, op, key, vals, seq` of the feed's
/// rows in order of `txid, seq`, on the connection of the caller, and sends the
/// text of the records to `pid` (`deliver`). Rows are read up to `bound` of
/// them; a batch that begins after `room` others have begun is not read.
/// `resumed` is the `n` of the next row when the first batch goes on one cut
/// by an earlier call. Replies `{:ok, [batch], more, cut}`; `{:error,
/// :sender_gone}` when `pid` was gone, `{:error, :bad_table}` for a table name
/// that is not UTF-8. Nothing is sent when no row was read.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::too_many_arguments)]
fn sqlite_feed_rows<'a>(
    env: Env<'a>,
    conn: ResourceArc<Conn>,
    sql: rustler::Binary,
    params: Term<'a>,
    bound: Option<u64>,
    resumed: Option<u64>,
    room: Option<u64>,
    pid: LocalPid,
    token: Term<'a>,
) -> Term<'a> {
    let _t = crate::stats::SQLITE_CALLS.time_for(&conn);
    let plan = Plan {
        bound,
        resumed,
        room,
    };
    let run = || -> Outcome<'a> {
        check_user(env, &conn)?;
        if sql.as_slice().contains(&0) {
            return Err(Failure::NulInStatement);
        }
        let read = with_db(&conn, |db| {
            check_transaction(&conn.control, db)?;
            let limits = Limits::default();
            let _call = Call::new(&conn.control, db, &limits);
            let sql = sql.as_slice();
            let (stmt, end) = next_statement(db, sql)?;
            let Some(StmtPtr(s)) = stmt else {
                return Ok(Read {
                    groups: Vec::new(),
                    more: false,
                    cut: false,
                });
            };
            let _guard = Finalize(s);
            refuse_tail(db, &sql[end..])?;
            bind_all(db, s, params)?;
            read_groups(db, &conn.control, s, &plan)
        })?;
        if !read.groups.is_empty() && !deliver(env, &pid, token, &read.groups) {
            return Err(Failure::SenderGone);
        }
        let groups: Vec<Term> = read.groups.iter().map(|g| describe(env, g)).collect();
        Ok((atoms::ok(), groups, read.more, read.cut).encode(env))
    };
    reply(env, run())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(f: impl FnOnce(&mut Vec<u8>)) -> String {
        let mut out = Vec::new();
        f(&mut out);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn escapes_as_jason_does() {
        let all: String = (0u8..0x20).map(char::from).collect();
        assert_eq!(
            text(|o| escape(o, &all)),
            "\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\b\\t\\n\\u000B\\f\\r\
             \\u000E\\u000F\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017\
             \\u0018\\u0019\\u001A\\u001B\\u001C\\u001D\\u001E\\u001F"
        );
        assert_eq!(
            text(|o| escape(o, "a\"b\\c/d\u{7f}\u{2028}é")),
            "a\\\"b\\\\c/d\u{7f}\u{2028}é"
        );
    }

    #[test]
    fn a_row_is_the_text_the_record_splices() {
        let mut out = Vec::new();
        assert!(push_row(
            &mut out,
            0,
            b"t\"x",
            b"insert",
            b"{\"id\":1}",
            Some(b"{\"v\":2}")
        )
        .is_ok());
        assert!(push_row(&mut out, 1, b"t", b"delete", b"{\"id\":1}", None).is_ok());
        assert!(push_row(&mut out, 2, b"t", b"update", b"{}", Some(b"")).is_ok());
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"n\":0,\"table\":\"t\\\"x\",\"op\":\"insert\",\"key\":{\"id\":1},\"values\":{\"v\":2}},\
             {\"n\":1,\"table\":\"t\",\"op\":\"delete\",\"key\":{\"id\":1},\"values\":null},\
             {\"n\":2,\"table\":\"t\",\"op\":\"update\",\"key\":{},\"values\":}"
        );
    }

    #[test]
    fn a_table_that_is_not_utf8_is_refused() {
        let mut out = Vec::new();
        let refused = push_row(&mut out, 0, b"t\xff", b"insert", b"{}", None);
        assert!(matches!(refused, Err(Failure::BadTable)));
    }

    #[test]
    fn key_and_values_keep_their_bytes() {
        let mut out = Vec::new();
        assert!(push_row(
            &mut out,
            0,
            b"t",
            b"insert",
            b"{\"k\":\"\xff\x00\"}",
            Some(b"[\xc3]")
        )
        .is_ok());
        assert_eq!(
            out,
            b"{\"n\":0,\"table\":\"t\",\"op\":\"insert\",\"key\":{\"k\":\"\xff\x00\"},\"values\":[\xc3]}"
        );
    }
}
