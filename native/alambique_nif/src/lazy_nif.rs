//! NIF surface of lazy restore: the page map, the page bitmap the VFS reads,
//! and the calls the hydrator process makes (design §16).

use std::path::Path;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rustler::{Atom, Binary, Encoder, Env, LocalPid, OwnedEnv, Resource, ResourceArc, Term};

use crate::error::Error;
use crate::guard::guard;
use crate::lazy::{self, Lazy, MissRequest, Run};
use crate::{error_term, header_tuple, sums_term, wal, SumsRes};

mod atoms {
    rustler::atoms! {
        ok, error, none, page_miss, need_tail, missing, incomplete, checksum,
    }
}

pub struct LazyRes(Arc<Lazy>);

#[rustler::resource_impl]
impl Resource for LazyRes {}

// ERTS runs `drop` from a C callback that rustler does not protect, so it
// must not panic (design §2.1).
impl Drop for LazyRes {
    fn drop(&mut self) {
        guard("lazy_drop", (), || lazy::unregister(&self.0));
    }
}

/// One thread for all cells sends the page-miss messages: a thread of the VM
/// (the dirty scheduler that runs the blocked read) cannot send them itself.
fn notifier() -> &'static Sender<(LocalPid, MissRequest)> {
    static TX: OnceLock<Sender<(LocalPid, MissRequest)>> = OnceLock::new();
    TX.get_or_init(|| {
        let (tx, rx) = channel::<(LocalPid, MissRequest)>();
        std::thread::Builder::new()
            .name("alambique-page-miss".into())
            .spawn(move || {
                let mut env = OwnedEnv::new();
                while let Ok((pid, m)) = rx.recv() {
                    // A panic loses one message, not the thread that serves
                    // every cell.
                    guard("page_miss_send", (), || {
                        let _ = env.send_and_clear(&pid, |env| {
                            (atoms::page_miss(), m.pgno, m.file, m.offset, m.size).encode(env)
                        });
                    });
                }
            })
            .expect("page-miss thread");
        tx
    })
}

fn run_term<'a>(env: Env<'a>, run: Run) -> Term<'a> {
    (run.file, run.offset, run.len, run.pages).encode(env)
}

/// A new plan for the database file at `path`, which is created empty.
#[rustler::nif(schedule = "DirtyIo")]
fn lazy_new<'a>(env: Env<'a>, path: String, page_size: u32, wait_ms: u64) -> Term<'a> {
    let _t = crate::stats::LAZY_CALLS.time();
    match Lazy::create(Path::new(&path), page_size, Duration::from_millis(wait_ms)) {
        Ok(l) => (atoms::ok(), ResourceArc::new(LazyRes(Arc::new(l)))).encode(env),
        Err(e) => error_term(env, e),
    }
}

/// Reads the header, trailer and page index of the LTX file `idx`. `head` is
/// the start of the file, `tail` its end, `total` its size.
#[rustler::nif(schedule = "DirtyCpu")]
fn lazy_stage<'a>(
    env: Env<'a>,
    lazy: ResourceArc<LazyRes>,
    idx: u32,
    head: Binary,
    tail: Binary,
    total: u64,
) -> Term<'a> {
    let _t = crate::stats::LAZY_CALLS.time();
    match lazy::stage_file(head.as_slice(), tail.as_slice(), total) {
        Ok(Ok(staged)) => {
            let reply = (atoms::ok(), header_tuple(&staged.header), staged.post_apply);
            lazy.0.stage(idx, staged);
            reply.encode(env)
        }
        Ok(Err(need)) => (atoms::need_tail(), need).encode(env),
        Err(e) => error_term(env, e),
    }
}

/// Applies the staged file `idx` to the map.
#[rustler::nif(schedule = "DirtyCpu")]
fn lazy_apply<'a>(env: Env<'a>, lazy: ResourceArc<LazyRes>, idx: u32) -> Term<'a> {
    let _t = crate::stats::LAZY_CALLS.time();
    match lazy.0.apply(idx) {
        Ok(()) => atoms::ok().encode(env),
        Err(e) => error_term(env, e),
    }
}

/// Ends the plan and makes the VFS serve the file through it.
#[rustler::nif(schedule = "DirtyIo")]
fn lazy_ready<'a>(env: Env<'a>, lazy: ResourceArc<LazyRes>) -> Term<'a> {
    let _t = crate::stats::LAZY_CALLS.time();
    match lazy.0.ready() {
        Ok(()) => {
            lazy::register(&lazy.0);
            atoms::ok().encode(env)
        }
        Err(e) => error_term(env, e),
    }
}

/// Sends `{:page_miss, pgno, file, offset, size}` to `pid` for every page a
/// read is waiting for.
#[rustler::nif]
fn lazy_attach(lazy: ResourceArc<LazyRes>, pid: LocalPid) -> Atom {
    lazy.0.attach(Arc::new(move |m| {
        let _ = notifier().send((pid, m));
    }));
    atoms::ok()
}

/// Installs the pages of a fetched run. `data` starts at the run's offset;
/// `pages` are `{pgno, offset in data, frame length}`. Returns `{:ok, done}`.
#[rustler::nif(schedule = "DirtyCpu")]
fn lazy_deliver<'a>(
    env: Env<'a>,
    lazy: ResourceArc<LazyRes>,
    data: Binary,
    pages: Vec<(u32, u32, u32)>,
) -> Term<'a> {
    let _t = crate::stats::LAZY_CALLS.time();
    let l = &lazy.0;
    let mut done = false;
    for (pgno, rel, len) in pages {
        let frame = data
            .as_slice()
            .get(rel as usize..rel as usize + len as usize)
            .ok_or_else(|| Error::Invalid("frame outside the fetched data".into()));
        let installed = frame
            .and_then(|f| lazy::decode_frame(f, pgno, l.page_size))
            .and_then(|page| l.install(pgno, &page).map_err(Error::Io));
        match installed {
            Ok(d) => done |= d,
            Err(e) => return error_term(env, e),
        }
    }
    (atoms::ok(), done).encode(env)
}

/// The run a missed page belongs to, or `:none` when it is there already.
#[rustler::nif]
fn lazy_miss_run<'a>(
    env: Env<'a>,
    lazy: ResourceArc<LazyRes>,
    pgno: u32,
    max_pages: usize,
) -> Term<'a> {
    match lazy.0.miss_run(pgno, max_pages) {
        Some(run) => run_term(env, run),
        None => atoms::none().encode(env),
    }
}

/// The next run for the background fetch, or `:none` when none is left.
#[rustler::nif]
fn lazy_next_batch<'a>(env: Env<'a>, lazy: ResourceArc<LazyRes>, max_bytes: u64) -> Term<'a> {
    match lazy.0.next_batch(max_bytes) {
        Some(run) => run_term(env, run),
        None => atoms::none().encode(env),
    }
}

/// Gives the pages of a failed background fetch back to the plan.
#[rustler::nif]
fn lazy_requeue(lazy: ResourceArc<LazyRes>, pages: Vec<u32>) -> Atom {
    lazy.0.requeue(&pages);
    atoms::ok()
}

/// Fails the reads that wait for `pgno`.
#[rustler::nif]
fn lazy_fail(lazy: ResourceArc<LazyRes>, pgno: u32, message: String) -> Atom {
    lazy.0.fail(pgno, message);
    atoms::ok()
}

/// Fails every read that waits for a page, now and later.
#[rustler::nif]
fn lazy_abort(lazy: ResourceArc<LazyRes>, message: String) -> Atom {
    lazy.0.abort(message);
    atoms::ok()
}

/// Stops serving the file lazily (the file stays).
#[rustler::nif]
fn lazy_release(lazy: ResourceArc<LazyRes>) -> Atom {
    lazy::unregister(&lazy.0);
    atoms::ok()
}

/// `{pages in the plan, pages not fetched yet}`.
#[rustler::nif]
fn lazy_progress(lazy: ResourceArc<LazyRes>) -> (u32, u32) {
    lazy.0.progress()
}

/// Checks the database checksum of the restored state against `expected`:
/// `:ok`, `:incomplete`, or `{:error, {:checksum, expected, got}}`.
#[rustler::nif(schedule = "DirtyCpu")]
fn lazy_verify<'a>(env: Env<'a>, lazy: ResourceArc<LazyRes>, expected: u64) -> Term<'a> {
    match lazy.0.verify(expected) {
        Ok(true) => atoms::ok().encode(env),
        Ok(false) => atoms::incomplete().encode(env),
        Err(got) => (atoms::error(), (atoms::checksum(), expected, got)).encode(env),
    }
}

/// Fills the page sums of `pages` from fetched pages, in a copy of `sums`
/// (which stays as it was). `{:ok, sums}` when all are known,
/// `{:missing, pages, sums}` otherwise.
#[rustler::nif]
fn lazy_prepare_sums<'a>(
    env: Env<'a>,
    lazy: ResourceArc<LazyRes>,
    sums: ResourceArc<SumsRes>,
    pages: Vec<u32>,
) -> Term<'a> {
    let mut sums = sums.0.clone();
    let missing = lazy.0.prepare_sums(&mut sums, &pages);
    let out = sums_term(env, sums);
    if missing.is_empty() {
        (atoms::ok(), out).encode(env)
    } else {
        (atoms::missing(), missing, out).encode(env)
    }
}

/// The replicator replaced the sums of `pages`.
#[rustler::nif]
fn lazy_mark_sums(lazy: ResourceArc<LazyRes>, pages: Vec<u32>) -> Atom {
    lazy.0.mark_sums(&pages);
    atoms::ok()
}

/// Fills every page sum that is still unknown, once all pages are fetched:
/// `{:ok, sums, checksum}` with the database checksum of the filled sums.
#[rustler::nif(schedule = "DirtyCpu")]
fn lazy_finish_sums<'a>(
    env: Env<'a>,
    lazy: ResourceArc<LazyRes>,
    sums: ResourceArc<SumsRes>,
) -> Term<'a> {
    let mut s = sums.0.clone();
    lazy.0.finish_sums(&mut s);
    let checksum = crate::ltx::CHECKSUM_FLAG | s.fold();
    (atoms::ok(), sums_term(env, s), checksum).encode(env)
}

/// The pages the committed WAL frames after `pos` write, and the database
/// size in pages after the last commit (`nil` when nothing is committed).
#[rustler::nif(schedule = "DirtyIo")]
fn wal_changed<'a>(env: Env<'a>, wal: String, page_size: u32, pos: crate::PosTuple) -> Term<'a> {
    let _t = crate::stats::LAZY_CALLS.time();
    match wal::scan(Path::new(&wal), page_size, Some(crate::pos_from(pos)), None) {
        Ok(scan) => {
            let pages: Vec<u32> = scan.pages.keys().copied().collect();
            (atoms::ok(), scan.commit, pages).encode(env)
        }
        Err(e) => error_term(env, e),
    }
}
