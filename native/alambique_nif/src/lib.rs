//! Byte-level replication work for Alambique: WAL reading, LTX encoding and
//! decoding, page application, checksums and merging. No network access.

mod alloc;
mod capture;
mod crc;
mod dbsum;
mod error;
#[cfg(any(test, feature = "fuzzing"))]
pub mod fuzzing;
mod guard;
mod lazy;
mod lazy_nif;
mod ltx;
mod merge;
mod reflink;
mod sqlite;
mod stats;
mod sums;
#[cfg(feature = "test-panic")]
mod test_panic;
mod vfs;
mod wal;

use std::path::Path;

use rustler::{Binary, Encoder, Env, NewBinary, ResourceArc, Term};

use error::Error;
use sums::Sums;
use wal::WalPos;

mod atoms {
    rustler::atoms! {
        ok, error, none, invalid, io, pre_apply_mismatch, post_apply_mismatch,
    }
}

type PosTuple = (u64, u32, u32, u32, u32);

fn pos_from(t: PosTuple) -> WalPos {
    WalPos {
        offset: t.0,
        salt1: t.1,
        salt2: t.2,
        cksum1: t.3,
        cksum2: t.4,
    }
}

fn pos_to(p: WalPos) -> PosTuple {
    (p.offset, p.salt1, p.salt2, p.cksum1, p.cksum2)
}

/// A persistent page-sums vector (see `sums.rs`). It never changes, so a
/// replicator state keeps its sums valid after later captures.
pub(crate) struct SumsRes(pub(crate) Sums);

#[rustler::resource_impl]
impl rustler::Resource for SumsRes {}

pub(crate) fn sums_term<'a>(env: Env<'a>, s: Sums) -> Term<'a> {
    ResourceArc::new(SumsRes(s)).encode(env)
}

pub(crate) fn bin<'a>(env: Env<'a>, data: &[u8]) -> Term<'a> {
    let mut b = NewBinary::new(env, data.len());
    b.as_mut_slice().copy_from_slice(data);
    Binary::from(b).to_term(env)
}

pub(crate) fn error_term<'a>(env: Env<'a>, e: Error) -> Term<'a> {
    let reason = match e {
        Error::Invalid(m) => (atoms::invalid(), m).encode(env),
        Error::Io(m) => (atoms::io(), m).encode(env),
        Error::PreApplyMismatch { expected, got } => {
            (atoms::pre_apply_mismatch(), expected, got).encode(env)
        }
        Error::PostApplyMismatch { expected, got } => {
            (atoms::post_apply_mismatch(), expected, got).encode(env)
        }
    };
    (atoms::error(), reason).encode(env)
}

fn output_term<'a>(env: Env<'a>, o: capture::Output) -> Term<'a> {
    (
        atoms::ok(),
        bin(env, &o.segment),
        pos_to(o.pos),
        sums_term(env, o.sums),
        o.checksum,
        o.commit,
    )
        .encode(env)
}

#[allow(clippy::too_many_arguments)]
fn mk_args<'a>(
    db: &'a str,
    wal: &'a str,
    page_size: u32,
    pos: PosTuple,
    sums: &'a Sums,
    pre: u64,
    txid: u64,
    timestamp: i64,
) -> capture::Args<'a> {
    capture::Args {
        db_path: Path::new(db),
        wal_path: Path::new(wal),
        page_size,
        pos: pos_from(pos),
        sums,
        pre,
        txid,
        timestamp,
    }
}

/// Encodes the committed WAL frames after `pos` as the segment for `txid`.
/// Returns `:none` when there are none.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::too_many_arguments)]
fn capture_incremental<'a>(
    env: Env<'a>,
    db: String,
    wal: String,
    page_size: u32,
    pos: PosTuple,
    sums: ResourceArc<SumsRes>,
    pre: u64,
    txid: u64,
    timestamp: i64,
) -> Term<'a> {
    let _t = crate::stats::REPLICATION_CALLS.time();
    let run = || -> Result<Term<'a>, Error> {
        Ok(
            match capture::incremental(&mk_args(
                &db, &wal, page_size, pos, &sums.0, pre, txid, timestamp,
            ))? {
                Some(o) => output_term(env, o),
                None => atoms::none().encode(env),
            },
        )
    };
    run().unwrap_or_else(|e| error_term(env, e))
}

/// Encodes a segment without pages (the fence).
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::too_many_arguments)]
fn capture_empty<'a>(
    env: Env<'a>,
    db: String,
    wal: String,
    page_size: u32,
    pos: PosTuple,
    sums: ResourceArc<SumsRes>,
    pre: u64,
    txid: u64,
    timestamp: i64,
) -> Term<'a> {
    let _t = crate::stats::REPLICATION_CALLS.time();
    let run = || -> Result<Term<'a>, Error> {
        Ok(output_term(
            env,
            capture::empty(&mk_args(
                &db, &wal, page_size, pos, &sums.0, pre, txid, timestamp,
            ))?,
        ))
    };
    run().unwrap_or_else(|e| error_term(env, e))
}

/// Encodes the whole database as of `txid`. With a `limit` position, WAL
/// frames from that position on are left out.
#[rustler::nif(schedule = "DirtyIo")]
fn capture_full<'a>(
    env: Env<'a>,
    db: String,
    wal: String,
    page_size: u32,
    txid: u64,
    timestamp: i64,
    limit: Option<PosTuple>,
) -> Term<'a> {
    let _t = crate::stats::REPLICATION_CALLS.time();
    let run = || -> Result<Term<'a>, Error> {
        let empty = Sums::default();
        let a = mk_args(
            &db,
            &wal,
            page_size,
            (0, 0, 0, 0, 0),
            &empty,
            0,
            txid,
            timestamp,
        );
        Ok(output_term(env, capture::full(&a, limit.map(pos_from))?))
    };
    run().unwrap_or_else(|e| error_term(env, e))
}

/// Encodes the whole database as the segment for `txid` that replaces the
/// state whose checksum is `pre` (a rewind, design §20).
#[rustler::nif(schedule = "DirtyIo")]
fn capture_replace<'a>(
    env: Env<'a>,
    db: String,
    wal: String,
    page_size: u32,
    pre: u64,
    txid: u64,
    timestamp: i64,
) -> Term<'a> {
    let _t = crate::stats::REPLICATION_CALLS.time();
    let run = || -> Result<Term<'a>, Error> {
        let empty = Sums::default();
        let a = mk_args(
            &db,
            &wal,
            page_size,
            (0, 0, 0, 0, 0),
            &empty,
            pre,
            txid,
            timestamp,
        );
        Ok(output_term(env, capture::replace(&a)?))
    };
    run().unwrap_or_else(|e| error_term(env, e))
}

/// True when committed frames exist after `pos`.
#[rustler::nif(schedule = "DirtyIo")]
fn wal_pending<'a>(env: Env<'a>, wal: String, page_size: u32, pos: PosTuple) -> Term<'a> {
    let _t = crate::stats::REPLICATION_CALLS.time();
    match capture::pending(Path::new(&wal), page_size, pos_from(pos)) {
        Ok(b) => (atoms::ok(), b).encode(env),
        Err(e) => error_term(env, e),
    }
}

/// A sums resource from the little-endian sums binary of `file_state`.
#[rustler::nif(schedule = "DirtyCpu")]
fn sums_new<'a>(env: Env<'a>, sums: Binary) -> Term<'a> {
    match Sums::from_le_bytes(sums.as_slice()) {
        Some(s) => (atoms::ok(), sums_term(env, s)).encode(env),
        None => error_term(
            env,
            Error::Invalid("page sums must be a multiple of 8 bytes".into()),
        ),
    }
}

/// A sums resource of `pages` zeros (a lazy restore knows no sum yet).
#[rustler::nif]
fn sums_zeroed<'a>(env: Env<'a>, pages: u32) -> Term<'a> {
    sums_term(env, Sums::zeroed(pages as usize))
}

/// Page checksums and database checksum of a database file.
#[rustler::nif(schedule = "DirtyIo")]
fn file_state<'a>(env: Env<'a>, path: String, page_size: u32) -> Term<'a> {
    let _t = crate::stats::REPLICATION_CALLS.time();
    match capture::file_state(Path::new(&path), page_size) {
        Ok((sums, checksum)) => (
            atoms::ok(),
            bin(
                env,
                &sums
                    .iter()
                    .flat_map(|s| s.to_le_bytes())
                    .collect::<Vec<u8>>(),
            ),
            checksum,
            sums.len() as u32,
        )
            .encode(env),
        Err(e) => error_term(env, e),
    }
}

/// Clones `src` into the existing empty file `dst`: `{:ok, true}`, or
/// `{:ok, false}` when the filesystem has no reflinks.
#[rustler::nif(schedule = "DirtyIo")]
fn reflink<'a>(env: Env<'a>, src: String, dst: String) -> Term<'a> {
    match reflink::clone_file(Path::new(&src), Path::new(&dst)) {
        Ok(cloned) => (atoms::ok(), cloned).encode(env),
        Err(e) => error_term(env, e),
    }
}

/// Applies an LTX file to the database file at `path`.
#[rustler::nif(schedule = "DirtyIo")]
fn apply_ltx<'a>(
    env: Env<'a>,
    path: String,
    data: Binary,
    current: u64,
    page_size: u32,
) -> Term<'a> {
    let _t = crate::stats::REPLICATION_CALLS.time();
    match dbsum::apply(Path::new(&path), data.as_slice(), current, page_size) {
        Ok(a) => (atoms::ok(), header_tuple(&a.header), a.post_apply).encode(env),
        Err(e) => error_term(env, e),
    }
}

pub(crate) type HeaderTuple = (u64, u64, i64, u32, u64, u32);

pub(crate) fn header_tuple(h: &ltx::Header) -> HeaderTuple {
    (
        h.min_txid,
        h.max_txid,
        h.timestamp,
        h.commit,
        h.pre_apply,
        h.page_size,
    )
}

/// Header and trailer of an LTX file, optionally verifying the whole file.
#[rustler::nif(schedule = "DirtyIo")]
fn ltx_info<'a>(env: Env<'a>, data: Binary, verify: bool) -> Term<'a> {
    let _t = crate::stats::REPLICATION_CALLS.time();
    let run = || -> Result<Term<'a>, Error> {
        let (h, t) = if verify {
            dbsum::verify(data.as_slice())?
        } else {
            ltx::peek(data.as_slice())?
        };
        Ok((atoms::ok(), header_tuple(&h), t.post_apply).encode(env))
    };
    run().unwrap_or_else(|e| error_term(env, e))
}

/// Merges consecutive LTX files into one.
#[rustler::nif(schedule = "DirtyCpu")]
fn merge_ltx<'a>(env: Env<'a>, inputs: Vec<Binary>) -> Term<'a> {
    let _t = crate::stats::REPLICATION_CALLS.time();
    let slices: Vec<&[u8]> = inputs.iter().map(|b| b.as_slice()).collect();
    match merge::merge(&slices) {
        Ok(out) => (atoms::ok(), bin(env, &out)).encode(env),
        Err(e) => error_term(env, e),
    }
}

#[cfg(not(any(test, feature = "fuzzing")))]
#[global_allocator]
static ALLOCATOR: alloc::Counted = alloc::Counted;

fn on_load(_env: Env, _info: Term) -> bool {
    alloc::install_sqlite() && vfs::register().is_ok()
}

rustler::init!("Elixir.Alambique.Replication.Native", load = on_load);
