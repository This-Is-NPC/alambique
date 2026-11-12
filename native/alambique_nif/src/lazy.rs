//! Page map and page bitmap of a database file that is restored lazily
//! (design §16).
//!
//! The file starts as a sparse file of the final size. `map` says where each
//! page lives in the store (which LTX file, byte range of its frame). `local`
//! says which pages are on disk. The VFS (`vfs`) blocks a read of a page that
//! is not local until `install` writes it. Every page that arrives from the
//! store also leaves its CRC in `crc`, so that the database checksum of a
//! full restore can be recomputed (`verify`) and the replicator can get the
//! CRCs of pages it replaces without reading them (`prepare_sums`).

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::dbsum::fold;
use crate::error::{invalid, Result};
use crate::ltx::{
    get_uvarint, lock_pgno, page_crc, pages_added, trailer_post, valid_page_size, Header,
    HEADER_SIZE, PAGE_HEADER_SIZE, TRAILER_SIZE,
};
use crate::sums::Sums;

const NO_FILE: u32 = u32::MAX;
/// The most bytes of other frames a run reads to join two pages of one file.
const MAX_GAP: u64 = 64 * 1024;
/// The base CRC of the page (what the store holds) is known.
const F_CRC: u8 = 1;
/// The replicator's CRC list holds the current CRC of the page.
const F_SUM: u8 = 2;
/// A background fetch of the page is under way.
const F_INFLIGHT: u8 = 4;

#[derive(Clone, Copy)]
struct Entry {
    offset: u64,
    size: u32,
    file: u32,
}

const EMPTY: Entry = Entry {
    offset: 0,
    size: 0,
    file: NO_FILE,
};

/// A page the VFS needs and does not have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissRequest {
    pub pgno: u32,
    pub file: u32,
    pub offset: u64,
    pub size: u32,
}

/// Consecutive frames of one store object, fetched with one ranged read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    pub file: u32,
    pub offset: u64,
    pub len: u64,
    /// Page number, offset inside the run, frame length.
    pub pages: Vec<(u32, u32, u32)>,
}

pub type Notify = Arc<dyn Fn(MissRequest) + Send + Sync>;

/// Longest encoding of a page index entry: a page number (5 bytes), an offset
/// and a size (10 bytes each).
const MAX_INDEX_ENTRY: u64 = 25;

/// What an LTX file contributes to the map.
pub struct Staged {
    pub header: Header,
    pub post_apply: u64,
    entries: Vec<(u32, u64, u32)>,
}

/// Parses the header, the trailer and the page index of an LTX file from its
/// first bytes (`head`) and its last bytes (`tail`, which end at the end of the
/// file of `total` bytes). `Err(n)` asks for a tail of at least `n` bytes.
pub fn stage_file(
    head: &[u8],
    tail: &[u8],
    total: u64,
) -> Result<std::result::Result<Staged, usize>> {
    let header = Header::from_bytes(head)?;
    header.validate()?;
    if total < (HEADER_SIZE + PAGE_HEADER_SIZE + TRAILER_SIZE + 9) as u64 {
        return invalid("short file");
    }
    if tail.len() < TRAILER_SIZE + 8 {
        return Ok(Err(TRAILER_SIZE + 8));
    }
    let t = &tail[tail.len() - TRAILER_SIZE..];
    let post_apply = trailer_post(&header, u64::from_be_bytes(t[..8].try_into().unwrap()))?;
    let size_at = tail.len() - TRAILER_SIZE - 8;
    let index_len = u64::from_be_bytes(tail[size_at..size_at + 8].try_into().unwrap());
    // The most a valid index takes: at most `commit` entries of three varints,
    // and the terminator. A larger claim would make the host read the file.
    let need = index_len
        .checked_add((TRAILER_SIZE + 8) as u64)
        .filter(|n| *n <= total && index_len <= MAX_INDEX_ENTRY * header.commit as u64 + 1)
        .ok_or_else(|| crate::error::Error::Invalid("page index size out of range".into()))?;
    if (tail.len() as u64) < need {
        return Ok(Err(need as usize));
    }
    let index = &tail[size_at - index_len as usize..size_at];
    let mut entries = Vec::new();
    let (mut p, mut prev) = (0usize, 0u32);
    loop {
        let pgno = get_uvarint(index, &mut p)?;
        if pgno == 0 {
            break;
        }
        let offset = get_uvarint(index, &mut p)?;
        let size = get_uvarint(index, &mut p)?;
        if pgno > header.commit as u64 || pgno as u32 <= prev {
            return invalid("page index out of order");
        }
        let min_size = (PAGE_HEADER_SIZE + 4) as u64;
        if offset < HEADER_SIZE as u64 || size < min_size || offset.saturating_add(size) > total {
            return invalid("page frame outside the file");
        }
        prev = pgno as u32;
        entries.push((prev, offset, size as u32));
    }
    if p != index.len() {
        return invalid("page index size mismatch");
    }
    Ok(Ok(Staged {
        header,
        post_apply,
        entries,
    }))
}

/// Decompresses one page frame (page header, size prefix, LZ4 block).
pub fn decode_frame(frame: &[u8], pgno: u32, page_size: u32) -> Result<Vec<u8>> {
    let head = PAGE_HEADER_SIZE + 4;
    if frame.len() < head {
        return invalid("short page frame");
    }
    if u32::from_be_bytes(frame[0..4].try_into().unwrap()) != pgno {
        return invalid(format!("frame is not page {pgno}"));
    }
    let n = u32::from_be_bytes(frame[PAGE_HEADER_SIZE..head].try_into().unwrap()) as usize;
    let payload = frame
        .get(head..head + n)
        .ok_or_else(|| crate::error::Error::Invalid("truncated page frame".into()))?;
    let mut page = vec![0u8; page_size as usize];
    let got = lz4_flex::block::decompress_into(payload, &mut page)
        .map_err(|e| crate::error::Error::Invalid(format!("decompress page {pgno}: {e}")))?;
    if got != page.len() {
        return invalid(format!("page {pgno} decompressed to {got} bytes"));
    }
    Ok(page)
}

struct Inner {
    map: Vec<Entry>,
    staged: HashMap<u32, Staged>,
    /// Pages the plan started with (the size of the restored database).
    orig: u32,
    /// Current size of the file in pages.
    pages: u32,
    local: Vec<u64>,
    flags: Vec<u8>,
    crc: Vec<u64>,
    /// Pages without a base CRC: still to fetch.
    unknown: u32,
    /// Page numbers by (file, offset), and each page's position in it.
    order: Vec<u32>,
    rank: Vec<u32>,
    cursor: usize,
    retry: VecDeque<u32>,
    requested: HashSet<u32>,
    failed: HashMap<u32, String>,
    aborted: Option<String>,
}

impl Inner {
    fn is_local(&self, pgno: u32) -> bool {
        pgno == 0
            || pgno > self.orig
            || self.local[(pgno - 1) as usize / 64] >> ((pgno - 1) % 64) & 1 == 1
    }

    /// Marks the page local; true when it was not.
    fn set_local(&mut self, pgno: u32) -> bool {
        if pgno == 0 || pgno > self.orig || self.is_local(pgno) {
            return false;
        }
        self.local[(pgno - 1) as usize / 64] |= 1 << ((pgno - 1) % 64);
        true
    }

    fn flag(&self, pgno: u32) -> u8 {
        self.flags[pgno as usize - 1]
    }
}

pub struct Lazy {
    pub path: PathBuf,
    pub page_size: u32,
    wait: Duration,
    complete: AtomicBool,
    inner: Mutex<Inner>,
    cond: Condvar,
    notify: Mutex<Option<Notify>>,
    file: File,
}

impl Lazy {
    /// Creates (and empties) the sparse file at `path`.
    pub fn create(path: &Path, page_size: u32, wait: Duration) -> Result<Lazy> {
        if !valid_page_size(page_size) {
            return invalid("invalid page size");
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Lazy {
            path: path.to_path_buf(),
            page_size,
            wait,
            complete: AtomicBool::new(false),
            inner: Mutex::new(Inner {
                map: Vec::new(),
                staged: HashMap::new(),
                orig: 0,
                pages: 0,
                local: Vec::new(),
                flags: Vec::new(),
                crc: Vec::new(),
                unknown: 0,
                order: Vec::new(),
                rank: Vec::new(),
                cursor: 0,
                retry: VecDeque::new(),
                requested: HashSet::new(),
                failed: HashMap::new(),
                aborted: None,
            }),
            cond: Condvar::new(),
            notify: Mutex::new(None),
            file,
        })
    }

    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }

    /// Who is told about pages the VFS misses.
    pub fn attach(&self, notify: Notify) {
        *self.notify.lock().unwrap_or_else(|e| e.into_inner()) = Some(notify);
    }

    // Planning

    pub fn stage(&self, idx: u32, staged: Staged) {
        self.inner().staged.insert(idx, staged);
    }

    /// Applies the staged file `idx` to the map, as `dbsum::apply` would apply
    /// it to a database file.
    pub fn apply(&self, idx: u32) -> Result<()> {
        let mut g = self.inner();
        let Some(s) = g.staged.remove(&idx) else {
            return invalid("file was not staged");
        };
        if s.header.page_size != self.page_size {
            return invalid("page size differs between files");
        }
        let commit = s.header.commit as usize;
        let old_len = if s.header.is_snapshot() {
            0
        } else {
            g.map.len()
        };
        // The file must carry every page the map grows by, as `dbsum::apply`
        // requires: that keeps the map within what the file holds.
        let added = s.entries.iter().filter(|e| e.0 as usize > old_len).count() as u64;
        let lock = lock_pgno(self.page_size);
        if added != pages_added(old_len as u32, s.header.commit, lock) {
            return invalid("file lacks pages the database grows by");
        }
        if s.header.is_snapshot() {
            g.map.clear();
        }
        g.map.truncate(commit);
        g.map.resize(commit, EMPTY);
        for (pgno, offset, size) in s.entries {
            g.map[pgno as usize - 1] = Entry {
                offset,
                size,
                file: idx,
            };
        }
        Ok(())
    }

    /// Ends the plan: every page except the lock page must have a source.
    /// Sizes the file and builds the bitmap and the fetch order.
    pub fn ready(&self) -> Result<()> {
        let mut g = self.inner();
        let n = g.map.len() as u32;
        let lock = lock_pgno(self.page_size);
        if let Some(p) = (1..=n).find(|&p| p != lock && g.map[p as usize - 1].file == NO_FILE) {
            return invalid(format!("no file holds page {p}"));
        }
        self.file.set_len(n as u64 * self.page_size as u64)?;
        g.orig = n;
        g.pages = n;
        g.local = vec![0; (n as usize).div_ceil(64)];
        g.flags = vec![0; n as usize];
        g.crc = vec![0; n as usize];
        g.unknown = n;
        if (1..=n).contains(&lock) {
            g.flags[lock as usize - 1] = F_CRC | F_SUM;
            g.set_local(lock);
            g.unknown -= 1;
        }
        let mut order: Vec<u32> = (1..=n).filter(|&p| p != lock).collect();
        // The newest files first: they hold the pages written last, which
        // the cell reads first.
        order.sort_unstable_by_key(|&p| {
            let e = g.map[p as usize - 1];
            (std::cmp::Reverse(e.file), e.offset)
        });
        let mut rank = vec![0u32; n as usize];
        for (i, &p) in order.iter().enumerate() {
            rank[p as usize - 1] = i as u32;
        }
        g.order = order;
        g.rank = rank;
        if g.unknown == 0 {
            self.finish(&mut g);
        }
        Ok(())
    }

    /// Pages in the plan and pages still to fetch.
    pub fn progress(&self) -> (u32, u32) {
        let g = self.inner();
        (g.orig, g.unknown)
    }

    // Reads and writes through the VFS

    fn miss_error(g: &Inner, p: u32) -> Option<String> {
        g.aborted.clone().or_else(|| g.failed.get(&p).cloned())
    }

    /// Blocks until pages `first..=last` are local, or fails after the wait
    /// time, when the fetch failed, or when the plan was aborted.
    pub fn ensure(&self, first: u32, last: u32) -> std::result::Result<(), String> {
        if self.is_complete() {
            return Ok(());
        }
        let deadline = Instant::now() + self.wait;
        let mut g = self.inner();
        for p in first..=last {
            loop {
                if g.is_local(p) {
                    break;
                }
                if let Some(msg) = Self::miss_error(&g, p) {
                    g.failed.remove(&p);
                    return Err(msg);
                }
                if g.requested.insert(p) {
                    let e = g.map[p as usize - 1];
                    let notify = self
                        .notify
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone();
                    match notify {
                        Some(f) => f(MissRequest {
                            pgno: p,
                            file: e.file,
                            offset: e.offset,
                            size: e.size,
                        }),
                        None => {
                            g.requested.remove(&p);
                            return Err("no hydrator attached".into());
                        }
                    }
                }
                let now = Instant::now();
                if now >= deadline {
                    g.requested.remove(&p);
                    return Err(format!("page {p} was not fetched in time"));
                }
                g = self
                    .cond
                    .wait_timeout(g, deadline - now)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        }
        Ok(())
    }

    fn page_range(&self, offset: u64, amt: u64) -> (u32, u32) {
        let ps = self.page_size as u64;
        (
            (offset / ps) as u32 + 1,
            ((offset + amt.max(1) - 1) / ps) as u32 + 1,
        )
    }

    /// Waits for the pages a read of `amt` bytes at `offset` touches.
    pub fn before_read(&self, offset: u64, amt: u64) -> std::result::Result<(), String> {
        let (first, last) = self.page_range(offset, amt);
        self.ensure(first, last)
    }

    /// Marks the pages a write covers entirely as local, before the bytes are
    /// written, so that a late fetch cannot replace them; pages it covers only
    /// in part must be fetched first.
    pub fn before_write(&self, offset: u64, amt: u64) -> std::result::Result<(), String> {
        if self.is_complete() {
            return Ok(());
        }
        let ps = self.page_size as u64;
        let (first, last) = self.page_range(offset, amt);
        for p in first..=last {
            let whole = offset <= (p as u64 - 1) * ps && offset + amt >= p as u64 * ps;
            if whole {
                self.inner().set_local(p);
            } else {
                self.ensure(p, p)?;
            }
        }
        Ok(())
    }

    /// The file shrinks to `size` bytes: pages past it no longer matter.
    pub fn on_truncate(&self, size: u64) {
        if self.is_complete() {
            return;
        }
        let new_pages = size.div_ceil(self.page_size as u64) as u32;
        let mut g = self.inner();
        for p in new_pages + 1..=g.orig {
            g.set_local(p);
        }
        g.pages = new_pages;
        self.cond.notify_all();
    }

    // Fetching

    /// Stores a page that came from the store. A page the database already
    /// wrote is not replaced. True when this was the last missing page.
    pub fn install(&self, pgno: u32, page: &[u8]) -> crate::error::Result<bool> {
        let crc = page_crc(pgno, page);
        let mut g = self.inner();
        if pgno == 0 || pgno > g.orig || page.len() != self.page_size as usize {
            return crate::error::invalid(format!("page {pgno} is not part of the plan"));
        }
        if g.set_local(pgno) {
            if let Err(e) = self
                .file
                .write_all_at(page, (pgno as u64 - 1) * self.page_size as u64)
            {
                // Not installed: leave the page missing.
                let i = (pgno - 1) as usize / 64;
                g.local[i] &= !(1 << ((pgno - 1) % 64));
                return Err(e.into());
            }
        }
        let f = &mut g.flags[pgno as usize - 1];
        *f &= !F_INFLIGHT;
        if *f & F_CRC == 0 {
            *f |= F_CRC;
            g.crc[pgno as usize - 1] = crc;
            g.unknown -= 1;
        }
        g.requested.remove(&pgno);
        g.failed.remove(&pgno);
        let done = g.unknown == 0;
        if done {
            self.finish(&mut g);
        }
        self.cond.notify_all();
        Ok(done)
    }

    fn finish(&self, g: &mut Inner) {
        g.map = Vec::new();
        g.order = Vec::new();
        g.rank = Vec::new();
        g.retry = VecDeque::new();
        self.complete.store(true, Ordering::Release);
    }

    fn build_run(&self, g: &Inner, from: usize, max_pages: usize, max_bytes: u64, bg: bool) -> Run {
        let first = g.order[from];
        let e0 = g.map[first as usize - 1];
        let mut run = Run {
            file: e0.file,
            offset: e0.offset,
            len: 0,
            pages: Vec::new(),
        };
        let mut next = e0.offset;
        for &p in &g.order[from..] {
            let e = g.map[p as usize - 1];
            let f = g.flag(p);
            let fresh = f & F_CRC == 0 && !(bg && f & F_INFLIGHT != 0);
            // The frames in between belong to older versions of pages that
            // another file replaced; reading them costs less than a request.
            if e.file != run.file || e.offset < next || e.offset - next > MAX_GAP || !fresh {
                break;
            }
            let end = e.offset + e.size as u64;
            if !run.pages.is_empty()
                && (run.pages.len() >= max_pages || end - run.offset > max_bytes)
            {
                break;
            }
            run.pages.push((p, (e.offset - run.offset) as u32, e.size));
            run.len = end - run.offset;
            next = end;
        }
        run
    }

    /// The run a missed page belongs to: the page and the pages after it in
    /// the same object, up to `max_pages`. `None` when it is already there.
    pub fn miss_run(&self, pgno: u32, max_pages: usize) -> Option<Run> {
        let g = self.inner();
        if self.is_complete() || pgno == 0 || pgno > g.orig || g.flag(pgno) & F_CRC != 0 {
            return None;
        }
        Some(self.build_run(
            &g,
            g.rank[pgno as usize - 1] as usize,
            max_pages,
            u64::MAX,
            false,
        ))
    }

    /// The next run for the background fetch, at most `max_bytes` long, and
    /// marked in flight. `None` when the plan is exhausted.
    pub fn next_batch(&self, max_bytes: u64) -> Option<Run> {
        let mut g = self.inner();
        if self.is_complete() {
            return None;
        }
        let from = loop {
            match g.retry.pop_front() {
                Some(p) if g.flag(p) & F_CRC == 0 => break Some(g.rank[p as usize - 1] as usize),
                Some(_) => continue,
                None => break None,
            }
        };
        let from = match from {
            Some(f) => f,
            None => {
                while g.cursor < g.order.len() {
                    let f = g.flag(g.order[g.cursor]);
                    if f & (F_CRC | F_INFLIGHT) == 0 {
                        break;
                    }
                    g.cursor += 1;
                }
                if g.cursor >= g.order.len() {
                    return None;
                }
                g.cursor
            }
        };
        let run = self.build_run(&g, from, usize::MAX, max_bytes, true);
        for (p, _, _) in &run.pages {
            g.flags[*p as usize - 1] |= F_INFLIGHT;
        }
        Some(run)
    }

    /// Gives pages of a failed background fetch back to the plan.
    pub fn requeue(&self, pages: &[u32]) {
        let mut g = self.inner();
        if self.is_complete() {
            return;
        }
        for &p in pages {
            if p >= 1 && p <= g.orig {
                g.flags[p as usize - 1] &= !F_INFLIGHT;
                g.retry.push_back(p);
            }
        }
    }

    /// The fetch of a missed page failed: the statement that waits fails.
    pub fn fail(&self, pgno: u32, message: String) {
        let mut g = self.inner();
        g.requested.remove(&pgno);
        g.failed.insert(pgno, message);
        self.cond.notify_all();
    }

    /// Fails every read that waits and every later one.
    pub fn abort(&self, message: String) {
        let mut g = self.inner();
        g.aborted = Some(message);
        self.cond.notify_all();
    }

    // Checksums

    /// Fills `sums` (per-page CRCs, as the replicator keeps them) for `pages`
    /// from the CRCs of fetched pages. Returns the pages that were not fetched
    /// yet. A page whose sum the replicator already replaced (`mark_sums`) is
    /// left alone; filling the others is repeatable, so that a batch that
    /// failed to upload and is captured again finds its sums.
    pub fn prepare_sums(&self, sums: &mut Sums, pages: &[u32]) -> Vec<u32> {
        let g = self.inner();
        let mut missing = Vec::new();
        for &p in pages {
            if p == 0 || p > g.orig || p as usize > sums.len() {
                continue;
            }
            let f = g.flag(p);
            if f & F_SUM != 0 {
                continue;
            }
            if f & F_CRC != 0 {
                sums.set(p as usize - 1, g.crc[p as usize - 1]);
            } else {
                missing.push(p);
            }
        }
        missing
    }

    /// The replicator replaced the sums of `pages` (a batch was captured):
    /// the fetched CRCs are no longer theirs.
    pub fn mark_sums(&self, pages: &[u32]) {
        let mut g = self.inner();
        for &p in pages {
            if p >= 1 && p <= g.orig {
                g.flags[p as usize - 1] |= F_SUM;
            }
        }
    }

    /// Fills every CRC that `sums` still lacks, once all pages are fetched.
    pub fn finish_sums(&self, sums: &mut Sums) {
        let mut g = self.inner();
        for p in 1..=g.orig {
            let i = p as usize - 1;
            if g.flags[i] & F_SUM == 0 && g.flags[i] & F_CRC != 0 && i < sums.len() {
                sums.set(i, g.crc[i]);
                g.flags[i] |= F_SUM;
            }
        }
    }

    /// The database checksum of the restored state, from the CRCs of the pages
    /// as the store holds them. `Err(got)` when it is not `expected`; `Ok(None)`
    /// when pages are still missing.
    pub fn verify(&self, expected: u64) -> std::result::Result<bool, u64> {
        let g = self.inner();
        if g.unknown > 0 {
            return Ok(false);
        }
        let got = fold(&g.crc);
        if got == expected {
            Ok(true)
        } else {
            Err(got)
        }
    }
}

// Registry of the files the VFS serves lazily, by path.

static REGISTRY: Mutex<Option<HashMap<PathBuf, Arc<Lazy>>>> = Mutex::new(None);

fn key(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Makes the VFS serve the database file at `lazy.path` through `lazy`.
pub fn register(lazy: &Arc<Lazy>) {
    REGISTRY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_with(HashMap::new)
        .insert(key(&lazy.path), lazy.clone());
}

/// Stops serving the file through `lazy`, unless a newer plan replaced it.
pub fn unregister(lazy: &Arc<Lazy>) {
    if let Some(m) = REGISTRY.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
        let k = key(&lazy.path);
        if m.get(&k).is_some_and(|l| Arc::ptr_eq(l, lazy)) {
            m.remove(&k);
        }
    }
}

/// The lazy state of the database file at `path`, if it is hydrating.
pub fn lookup(path: &Path) -> Option<Arc<Lazy>> {
    let guard = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    let lazy = guard.as_ref()?.get(&key(path))?.clone();
    (!lazy.is_complete()).then_some(lazy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ltx::{Encoder, Header, CHECKSUM_FLAG, HEADER_FLAG_NO_CHECKSUM};
    use std::sync::mpsc;

    pub(super) const PS: u32 = 512;

    pub(super) fn page(pgno: u32, fill: u8) -> Vec<u8> {
        (0..PS)
            .map(|i| fill.wrapping_add((i % 13) as u8) ^ pgno as u8)
            .collect()
    }

    /// An LTX file with `pages` (in ascending order) and database size `commit`.
    pub(super) fn ltx(min: u64, commit: u32, pages: &[(u32, u8)]) -> Vec<u8> {
        let h = Header {
            page_size: PS,
            commit,
            min_txid: min,
            max_txid: min,
            timestamp: 1,
            pre_apply: if min == 1 { 0 } else { CHECKSUM_FLAG | 1 },
            post_apply: CHECKSUM_FLAG | 1,
            flags: HEADER_FLAG_NO_CHECKSUM,
            ..Default::default()
        };
        let mut enc = Encoder::new(h).unwrap();
        for (p, f) in pages {
            enc.add_page(*p, &page(*p, *f)).unwrap();
        }
        enc.finish().unwrap()
    }

    pub(super) fn stage_all(lazy: &Lazy, idx: u32, file: &[u8]) {
        let total = file.len() as u64;
        let tail = &file[file.len().saturating_sub(40)..];
        let staged = match stage_file(&file[..HEADER_SIZE], tail, total).unwrap() {
            Ok(s) => s,
            Err(n) => stage_file(&file[..HEADER_SIZE], &file[file.len() - n..], total)
                .unwrap()
                .ok()
                .unwrap(),
        };
        let snapshot = staged.header.is_snapshot();
        lazy.stage(idx, staged);
        lazy.apply(idx).unwrap();
        assert_eq!(snapshot, idx == 0);
    }

    #[test]
    fn an_index_entry_that_wraps_around_is_rejected() {
        let header = Header {
            page_size: PS,
            commit: 1,
            min_txid: 1,
            max_txid: 1,
            flags: crate::ltx::HEADER_FLAG_NO_CHECKSUM,
            post_apply: crate::ltx::CHECKSUM_FLAG | 1,
            ..Default::default()
        };
        let varint = |mut v: u64| {
            let mut out = Vec::new();
            while v >= 0x80 {
                out.push(v as u8 | 0x80);
                v >>= 7;
            }
            out.push(v as u8);
            out
        };
        let index: Vec<u8> = [1, u64::MAX - 5, 100, 0]
            .into_iter()
            .flat_map(varint)
            .collect();
        let mut tail = index.clone();
        tail.extend_from_slice(&(index.len() as u64).to_be_bytes());
        tail.extend_from_slice(&[0; TRAILER_SIZE]);
        let total = 1 << 40;
        assert!(stage_file(&header.to_bytes(), &tail, total).is_err());
    }

    #[test]
    fn a_commit_without_matching_pages_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let lazy = Lazy::create(&dir.path().join("db"), PS, Duration::from_secs(1)).unwrap();
        // A snapshot of 2^24 pages (a 256 MiB map) that carries one page.
        let huge = ltx(1, 1 << 24, &[(1, 1)]);
        let staged = stage_file(&huge[..HEADER_SIZE], &huge, huge.len() as u64)
            .unwrap()
            .ok()
            .unwrap();
        lazy.stage(0, staged);
        assert!(lazy.apply(0).is_err());
        assert!(lazy.inner().map.capacity() < 1 << 20);
    }

    #[test]
    fn an_increment_that_skips_the_pages_it_adds_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let lazy = Lazy::create(&dir.path().join("db"), PS, Duration::from_secs(1)).unwrap();
        let (a, _) = files();
        stage_all(&lazy, 0, &a);
        // Grows from 6 to 1 << 24 pages with the page 7 only.
        let b = ltx(2, 1 << 24, &[(7, 1)]);
        let staged = stage_file(&b[..HEADER_SIZE], &b, b.len() as u64)
            .unwrap()
            .ok()
            .unwrap();
        lazy.stage(1, staged);
        assert!(lazy.apply(1).is_err());
        assert_eq!(lazy.inner().map.len(), 6, "the map is left as it was");
    }

    #[test]
    fn an_index_size_beyond_what_commit_allows_is_rejected() {
        let f = ltx(1, 3, &[(1, 1), (2, 1), (3, 1)]);
        let total = f.len() as u64;
        let mut tail = f.clone();
        let at = tail.len() - TRAILER_SIZE - 8;
        tail[at..at + 8].copy_from_slice(&(total - 24).to_be_bytes());
        // Only the end of the file is at hand: the index would need all of it.
        let tail = &tail[tail.len() - 40..];
        assert!(stage_file(&f[..HEADER_SIZE], tail, total).is_err());
    }

    pub(super) fn frame<'a>(file: &'a [u8], run: &Run, i: usize) -> &'a [u8] {
        let (_, rel, len) = run.pages[i];
        let start = run.offset as usize + rel as usize;
        &file[start..start + len as usize]
    }

    fn files() -> (Vec<u8>, Vec<u8>) {
        // Snapshot of 6 pages, then a file that rewrites 2 and grows to 7.
        (
            ltx(1, 6, &(1..=6).map(|p| (p, 1)).collect::<Vec<_>>()),
            ltx(2, 7, &[(2, 9), (7, 9)]),
        )
    }

    #[test]
    fn newest_file_wins_and_shrinks_apply() {
        let dir = tempfile::tempdir().unwrap();
        let lazy = Lazy::create(&dir.path().join("db"), PS, Duration::from_secs(1)).unwrap();
        let (a, b) = files();
        stage_all(&lazy, 0, &a);
        stage_all(&lazy, 1, &b);
        // A third file shrinks the database to 5 pages and rewrites page 1.
        let c = ltx(3, 5, &[(1, 4)]);
        let staged = stage_file(&c[..HEADER_SIZE], &c, c.len() as u64)
            .unwrap()
            .ok()
            .unwrap();
        lazy.stage(2, staged);
        lazy.apply(2).unwrap();
        lazy.ready().unwrap();
        assert_eq!(lazy.progress(), (5, 5));
        assert_eq!(
            std::fs::metadata(dir.path().join("db")).unwrap().len(),
            5 * PS as u64
        );
        let g = lazy.inner();
        assert_eq!(g.map[0].file, 2);
        assert_eq!(g.map[1].file, 1);
        assert_eq!(g.map[2].file, 0);
    }

    #[test]
    fn a_page_without_a_source_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let lazy = Lazy::create(&dir.path().join("db"), PS, Duration::from_secs(1)).unwrap();
        // No snapshot: page 1 of 2 has no source.
        let b = ltx(2, 2, &[(2, 1)]);
        let staged = stage_file(&b[..HEADER_SIZE], &b, b.len() as u64)
            .unwrap()
            .ok()
            .unwrap();
        lazy.stage(0, staged);
        assert!(lazy.apply(0).is_err());
    }

    #[test]
    fn a_small_tail_asks_for_more() {
        let (a, _) = files();
        let total = a.len() as u64;
        let need = stage_file(&a[..HEADER_SIZE], &a[a.len() - 24..], total)
            .unwrap()
            .err()
            .unwrap();
        assert!(need > 24);
        assert!(stage_file(&a[..HEADER_SIZE], &a[a.len() - need..], total)
            .unwrap()
            .is_ok());
    }

    #[test]
    fn install_makes_pages_local_and_completes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let lazy = Lazy::create(&path, PS, Duration::from_secs(1)).unwrap();
        let (a, b) = files();
        stage_all(&lazy, 0, &a);
        stage_all(&lazy, 1, &b);
        lazy.ready().unwrap();
        assert_eq!(lazy.progress(), (7, 7));
        let mut order = Vec::new();
        while let Some(run) = lazy.next_batch(u64::MAX) {
            let file = if run.file == 0 { &a } else { &b };
            for (i, (p, _, _)) in run.pages.iter().enumerate() {
                let data = decode_frame(frame(file, &run, i), *p, PS).unwrap();
                order.push(*p);
                lazy.install(*p, &data).unwrap();
            }
        }
        assert!(lazy.is_complete());
        // Pages 2 and 7 come from the second file.
        let disk = std::fs::read(&path).unwrap();
        assert_eq!(&disk[PS as usize..2 * PS as usize], &page(2, 9)[..]);
        assert_eq!(&disk[2 * PS as usize..3 * PS as usize], &page(3, 1)[..]);
        assert_eq!(order.len(), 7);
        let expected = fold(
            &(1..=7)
                .map(|p| page_crc(p, &page(p, if p == 2 || p == 7 { 9 } else { 1 })))
                .collect::<Vec<_>>(),
        );
        assert_eq!(lazy.verify(expected), Ok(true));
        assert!(lazy.verify(expected ^ 1).is_err());
    }

    #[test]
    fn runs_join_adjacent_pages_of_one_file() {
        let dir = tempfile::tempdir().unwrap();
        let lazy = Lazy::create(&dir.path().join("db"), PS, Duration::from_secs(1)).unwrap();
        let (a, b) = files();
        stage_all(&lazy, 0, &a);
        stage_all(&lazy, 1, &b);
        lazy.ready().unwrap();
        let run = lazy.miss_run(3, 3).unwrap();
        assert_eq!(
            (run.file, run.pages.iter().map(|p| p.0).collect::<Vec<_>>()),
            (0, vec![3, 4, 5])
        );
        assert_eq!(run.len, run.pages.iter().map(|p| p.2 as u64).sum::<u64>());
        // The background walks the files from the newest, in runs of the given
        // size. Page 2 sits between pages 1 and 3 in the first file but lives
        // in the second: its old frame is read to join the pages around it.
        let pages = |r: &Run| r.pages.iter().map(|p| p.0).collect::<Vec<_>>();
        let newest = lazy.next_batch(u64::MAX).unwrap();
        assert_eq!((newest.file, pages(&newest)), (1, vec![2, 7]));
        let oldest = lazy.next_batch(u64::MAX).unwrap();
        assert_eq!((oldest.file, pages(&oldest)), (0, vec![1, 3, 4, 5, 6]));
        // The run spans the old frame of page 2 as well.
        assert!(oldest.len > oldest.pages.iter().map(|p| p.2 as u64).sum::<u64>());
        assert!(lazy.next_batch(u64::MAX).is_none());
        // A failed fetch gives its pages back.
        lazy.requeue(&[1, 3]);
        assert_eq!(lazy.next_batch(u64::MAX).unwrap().pages[0].0, 1);
    }

    #[test]
    fn a_read_waits_for_its_page_and_asks_once() {
        let dir = tempfile::tempdir().unwrap();
        let lazy =
            Arc::new(Lazy::create(&dir.path().join("db"), PS, Duration::from_secs(5)).unwrap());
        let (a, b) = files();
        stage_all(&lazy, 0, &a);
        stage_all(&lazy, 1, &b);
        lazy.ready().unwrap();
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        lazy.attach(Arc::new(move |m| tx.lock().unwrap().send(m).unwrap()));

        let reader = {
            let lazy = lazy.clone();
            std::thread::spawn(move || lazy.before_read(2 * PS as u64 + 10, 20))
        };
        let miss = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(miss.pgno, 3);
        assert_eq!(miss.file, 0);
        let second = {
            let lazy = lazy.clone();
            std::thread::spawn(move || lazy.before_read(2 * PS as u64, PS as u64))
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            rx.try_recv().is_err(),
            "the second reader must not ask again"
        );
        let data = decode_frame(
            &a[miss.offset as usize..(miss.offset + miss.size as u64) as usize],
            3,
            PS,
        )
        .unwrap();
        lazy.install(3, &data).unwrap();
        assert_eq!(reader.join().unwrap(), Ok(()));
        assert_eq!(second.join().unwrap(), Ok(()));
    }

    #[test]
    fn a_failed_or_late_fetch_fails_the_read_and_the_next_one_asks_again() {
        let dir = tempfile::tempdir().unwrap();
        // Miri runs slowly enough to outlast a short wait.
        let wait = Duration::from_millis(if cfg!(miri) { 3000 } else { 200 });
        let lazy = Arc::new(Lazy::create(&dir.path().join("db"), PS, wait).unwrap());
        let (a, b) = files();
        stage_all(&lazy, 0, &a);
        stage_all(&lazy, 1, &b);
        lazy.ready().unwrap();
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        lazy.attach(Arc::new(move |m| tx.lock().unwrap().send(m).unwrap()));

        // Nobody answers: the wait ends.
        assert!(lazy
            .before_read(0, 10)
            .unwrap_err()
            .contains("not fetched in time"));
        assert_eq!(rx.try_recv().unwrap().pgno, 1);
        // An explicit failure ends the wait at once and names the reason.
        let l2 = lazy.clone();
        let t = std::thread::spawn(move || l2.before_read(0, 10));
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap().pgno, 1);
        lazy.fail(1, "store down".into());
        assert_eq!(t.join().unwrap(), Err("store down".into()));
        // Later reads ask again.
        let _ = lazy.before_read(0, 10);
        assert_eq!(rx.try_recv().unwrap().pgno, 1);
        // Aborting fails everything.
        lazy.abort("cell stopped".into());
        assert_eq!(lazy.before_read(0, 10), Err("cell stopped".into()));
    }

    #[test]
    fn local_writes_are_never_replaced_by_a_late_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let lazy = Lazy::create(&path, PS, Duration::from_secs(1)).unwrap();
        let (a, b) = files();
        stage_all(&lazy, 0, &a);
        stage_all(&lazy, 1, &b);
        lazy.ready().unwrap();
        // The database writes whole page 3 locally.
        lazy.before_write(2 * PS as u64, PS as u64).unwrap();
        let new = vec![0xEE; PS as usize];
        std::os::unix::fs::FileExt::write_all_at(
            &File::options().write(true).open(&path).unwrap(),
            &new,
            2 * PS as u64,
        )
        .unwrap();
        // The old page arrives late.
        let e = lazy.inner().map[2];
        let old = decode_frame(&a[e.offset as usize..][..e.size as usize], 3, PS).unwrap();
        lazy.install(3, &old).unwrap();
        assert_eq!(
            &std::fs::read(&path).unwrap()[2 * PS as usize..3 * PS as usize],
            &new[..]
        );
        // Its base CRC is still the store's, for the final check.
        assert_eq!(lazy.inner().crc[2], page_crc(3, &old));
        // A partial write to a missing page waits for it.
        assert!(lazy.before_write(PS as u64 + 5, 10).is_err());
    }

    #[test]
    fn truncation_drops_the_pages_past_the_end() {
        let dir = tempfile::tempdir().unwrap();
        let lazy = Lazy::create(&dir.path().join("db"), PS, Duration::from_secs(1)).unwrap();
        let (a, b) = files();
        stage_all(&lazy, 0, &a);
        stage_all(&lazy, 1, &b);
        lazy.ready().unwrap();
        lazy.on_truncate(3 * PS as u64);
        assert!(lazy.inner().is_local(5));
        assert!(!lazy.inner().is_local(2));
        // A late page 5 does not extend the file.
        let e = lazy.inner().map[4];
        let data = decode_frame(&a[e.offset as usize..][..e.size as usize], 5, PS).unwrap();
        lazy.install(5, &data).unwrap();
        assert_eq!(std::fs::metadata(&lazy.path).unwrap().len(), 7 * PS as u64);
        assert_eq!(lazy.inner().pages, 3);
    }

    #[test]
    fn sums_are_handed_over_once_per_page() {
        let dir = tempfile::tempdir().unwrap();
        let lazy = Lazy::create(&dir.path().join("db"), PS, Duration::from_secs(1)).unwrap();
        let (a, b) = files();
        stage_all(&lazy, 0, &a);
        stage_all(&lazy, 1, &b);
        lazy.ready().unwrap();
        let mut sums = Sums::zeroed(7);
        assert_eq!(lazy.prepare_sums(&mut sums, &[2, 4]), vec![2, 4]);
        let e = lazy.inner().map[3];
        let data = decode_frame(&a[e.offset as usize..][..e.size as usize], 4, PS).unwrap();
        lazy.install(4, &data).unwrap();
        assert_eq!(lazy.prepare_sums(&mut sums, &[2, 4]), vec![2]);
        assert_eq!(sums.get(3), page_crc(4, &data));
        // A batch that failed to upload is prepared again from a state without
        // the sums: they are filled again.
        let mut again = Sums::zeroed(7);
        assert_eq!(lazy.prepare_sums(&mut again, &[4]), Vec::<u32>::new());
        assert_eq!(again.get(3), page_crc(4, &data));
        // Once the batch is captured, the replicator owns the sum.
        lazy.mark_sums(&[4]);
        sums.set(3, 77);
        assert_eq!(lazy.prepare_sums(&mut sums, &[4]), Vec::<u32>::new());
        assert_eq!(sums.get(3), 77);
    }
}

#[cfg(test)]
mod props {
    use super::tests::{frame, ltx, page, stage_all, PS};
    use super::*;
    use proptest::prelude::*;

    /// A snapshot size, then per increment the new size and rewritten pages.
    fn plan() -> impl Strategy<Value = (u32, Vec<(u32, Vec<u32>)>)> {
        (
            1u32..8,
            prop::collection::vec((1u32..8, prop::collection::vec(1u32..8, 0..4)), 0..4),
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// The background plan fetches every page of the final database once,
        /// from the newest file that has it, and nothing else.
        #[test]
        #[cfg_attr(miri, ignore = "proptest is too slow under Miri")]
        fn the_plan_fetches_each_needed_page_once_from_its_newest_file(
            (first, steps) in plan(),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let lazy = Lazy::create(&path, PS, Duration::from_secs(1)).unwrap();

            // `fills[p - 1]` is the fill of the newest version of page `p`.
            let mut fills: Vec<u8> = vec![0; first as usize];
            let mut files = vec![ltx(1, first, &(1..=first).map(|p| (p, 0)).collect::<Vec<_>>())];
            for (i, (size, writes)) in steps.iter().enumerate() {
                let fill = i as u8 + 1;
                let old = fills.len() as u32;
                let mut touched: Vec<u32> = (old + 1..=*size).collect();
                touched.extend(writes.iter().filter(|p| **p <= *size));
                touched.sort_unstable();
                touched.dedup();
                fills.resize(*size as usize, fill);
                for p in &touched {
                    fills[*p as usize - 1] = fill;
                }
                let pages: Vec<(u32, u8)> = touched.iter().map(|p| (*p, fill)).collect();
                files.push(ltx(i as u64 + 2, *size, &pages));
            }
            for (i, f) in files.iter().enumerate() {
                stage_all(&lazy, i as u32, f);
            }
            lazy.ready().unwrap();

            let mut fetched = Vec::new();
            while let Some(run) = lazy.next_batch(u64::MAX) {
                for (i, (p, _, _)) in run.pages.iter().enumerate() {
                    let data = decode_frame(frame(&files[run.file as usize], &run, i), *p, PS)
                        .unwrap();
                    prop_assert_eq!(&data, &page(*p, fills[*p as usize - 1]));
                    fetched.push(*p);
                    lazy.install(*p, &data).unwrap();
                }
            }
            fetched.sort_unstable();
            prop_assert_eq!(fetched, (1..=fills.len() as u32).collect::<Vec<_>>());
            prop_assert!(lazy.is_complete());
            let expected: Vec<u8> = fills
                .iter()
                .enumerate()
                .flat_map(|(i, f)| page(i as u32 + 1, *f))
                .collect();
            prop_assert_eq!(std::fs::read(&path).unwrap(), expected);
        }
    }
}
