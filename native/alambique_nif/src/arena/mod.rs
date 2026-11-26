//! Page arena: one reserved address range for SQLite's page-cache blocks.
//!
//! Every cached page is one `sqlite3Malloc` of a fixed size (page size plus
//! SQLite's per-page headers). Those blocks come from here instead of the
//! ERTS carriers, which almost never empty, so a trim can give the OS pages
//! of free slots back with `madvise(MADV_DONTNEED)`.
//!
//! A connection (a region) takes its slots from extents it owns, so the
//! pages of an idle cell sit next to each other and whole OS pages free up.
//! The hooks (`alloc`, `free`) make no system call and allocate nothing; a
//! dedicated thread runs the trims, in bounded rounds, so a scheduler never
//! waits for `madvise`.

mod book;
mod sys;

use std::io;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

pub use book::CLASSES;
use book::{Book, Claim, Geometry, MAX_CLAIMS};

/// Extents one trim round looks at, and pages one round releases at most:
/// they bound how long the lock is held and how much one `madvise` batch
/// asks of the kernel.
const SCAN: usize = 64;
const ROUND_PAGES: usize = 256;

#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Bytes of address space reserved.
    pub reserved: usize,
    /// Target bytes of an extent (a class takes whole slots, at least one).
    pub extent_bytes: usize,
    /// Resident bytes beyond the blocks in use that a trim leaves alone.
    pub keep: usize,
    /// Free resident bytes, above `keep` and the last trim's remainder, that
    /// make `wants_trim` true.
    pub threshold: usize,
    /// Most bytes one trim cycle releases.
    pub max_trim: usize,
    /// Most regions open at once, region 0 included.
    pub max_regions: usize,
}

pub struct Arena {
    /// Start of the range, kept as a pointer so blocks derive from it.
    base: *mut u8,
    range: usize,
    shift: u32,
    page: usize,
    cfg: Config,
    book: Mutex<Book>,
    in_use: AtomicU64,
    resident: AtomicU64,
    mapped: AtomicU64,
    regions: AtomicU64,
    /// Free resident bytes the last trim could not release.
    residue: AtomicU64,
    released: AtomicU64,
    trims: AtomicU64,
    trim_ns_max: AtomicU64,
    fallbacks: AtomicU64,
    trimmer: OnceLock<Thread>,
}

// SAFETY: the address range is only touched through the slots handed out and
// by `madvise` on free slots; every other field is atomic or behind the mutex.
unsafe impl Send for Arena {}
// SAFETY: as above.
unsafe impl Sync for Arena {}

impl Arena {
    /// Reserves the range. `strides` are the slot bytes of the classes.
    pub fn new(cfg: Config, strides: [usize; CLASSES]) -> io::Result<Arena> {
        Arena::with_reserve(cfg, strides, sys::reserve)
    }

    /// `new` with the reservation of the range given, so tests can make it fail.
    fn with_reserve(
        cfg: Config,
        strides: [usize; CLASSES],
        reserve: impl FnOnce(usize) -> io::Result<*mut u8>,
    ) -> io::Result<Arena> {
        let page = sys::page_size();
        let book = Book::new(Geometry {
            page,
            reserved: cfg.reserved,
            extent_bytes: cfg.extent_bytes,
            strides,
            max_regions: cfg.max_regions,
        })
        .ok_or(io::ErrorKind::InvalidInput)?;
        let range = book.range_bytes();
        let base = reserve(range)?;
        Ok(Arena {
            base,
            range,
            shift: book.extent_bytes().trailing_zeros(),
            page,
            cfg,
            book: Mutex::new(book),
            in_use: AtomicU64::new(0),
            resident: AtomicU64::new(0),
            mapped: AtomicU64::new(0),
            regions: AtomicU64::new(0),
            residue: AtomicU64::new(0),
            released: AtomicU64::new(0),
            trims: AtomicU64::new(0),
            trim_ns_max: AtomicU64::new(0),
            fallbacks: AtomicU64::new(0),
            trimmer: OnceLock::new(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Book> {
        self.book.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Publishes the gauges `wants_trim` and the stats read without the lock.
    fn publish(&self, b: &Book) {
        self.in_use.store(b.in_use(), Relaxed);
        self.resident.store(b.resident_bytes(), Relaxed);
        self.mapped
            .store((b.extents() << self.shift) as u64, Relaxed);
        self.regions.store(b.open_regions(), Relaxed);
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn range_bytes(&self) -> usize {
        self.range
    }

    pub fn in_use_bytes(&self) -> u64 {
        self.in_use.load(Relaxed)
    }

    pub fn resident_bytes(&self) -> u64 {
        self.resident.load(Relaxed)
    }

    pub fn mapped_bytes(&self) -> u64 {
        self.mapped.load(Relaxed)
    }

    pub fn open_regions(&self) -> u64 {
        self.regions.load(Relaxed)
    }

    pub fn residue_bytes(&self) -> u64 {
        self.residue.load(Relaxed)
    }

    pub fn released_bytes(&self) -> u64 {
        self.released.load(Relaxed)
    }

    pub fn trims(&self) -> u64 {
        self.trims.load(Relaxed)
    }

    pub fn trim_ns_max(&self) -> u64 {
        self.trim_ns_max.load(Relaxed)
    }

    pub fn fallbacks(&self) -> u64 {
        self.fallbacks.load(Relaxed)
    }

    /// Whether `p` lies in the reserved range.
    pub fn contains(&self, p: *const u8) -> bool {
        (p as usize).wrapping_sub(self.base as usize) < self.range
    }

    /// The extent `p` lies in (tests).
    #[cfg(test)]
    pub fn extent_of(&self, p: *const u8) -> usize {
        (p as usize - self.base as usize) >> self.shift
    }

    /// A new region (one connection): its slot and its id, or `None` when
    /// the table is full.
    pub fn open_region(&self) -> Option<(u32, u64)> {
        let mut b = self.lock();
        let r = b.open_region();
        self.publish(&b);
        r
    }

    pub fn close_region(&self, slot: u32, id: u64) {
        let mut b = self.lock();
        b.close_region(slot, id);
        self.publish(&b);
    }

    /// A slot of `class` for `region`, or null when the range is exhausted
    /// (counted).
    pub fn alloc(&self, class: usize, region: u32) -> *mut u8 {
        let mut b = self.lock();
        let off = b.alloc(class, region);
        self.publish(&b);
        drop(b);
        match off {
            Some(off) => self.base.wrapping_add(off),
            None => {
                self.fallbacks.fetch_add(1, Relaxed);
                std::ptr::null_mut()
            }
        }
    }

    /// Gives back a slot from `alloc`, from any thread.
    ///
    /// # Safety
    /// `p` came from `alloc` and is not used afterwards.
    pub unsafe fn free(&self, p: *mut u8) {
        let mut b = self.lock();
        b.free(p as usize - self.base as usize);
        self.publish(&b);
    }

    /// Free resident bytes.
    fn excess(&self) -> u64 {
        self.resident_bytes().saturating_sub(self.in_use_bytes())
    }

    /// Whether enough free memory is resident to make a trim worth its scan.
    pub fn wants_trim(&self) -> bool {
        let excess = self.excess();
        let residue = self.residue.load(Relaxed);
        if excess < residue {
            self.residue.store(excess, Relaxed);
        }
        let (keep, threshold) = (self.cfg.keep as u64, self.cfg.threshold as u64);
        excess > keep + threshold && excess > residue + threshold
    }

    /// Wakes the trim thread when a trim is due. Cheap enough for the end of
    /// every NIF call.
    pub fn poke(&self) {
        if self.wants_trim() {
            if let Some(t) = self.trimmer.get() {
                t.unpark();
            }
        }
    }

    /// Starts the thread that trims, which also looks every `idle` in case a
    /// wake-up was missed. It lives as long as the library.
    pub fn start_trimmer(&'static self, idle: Duration) -> io::Result<()> {
        let handle = thread::Builder::new()
            .name("alambique-trim".into())
            .spawn(move || {
                let mut claims = Vec::with_capacity(MAX_CLAIMS);
                loop {
                    thread::park_timeout(idle);
                    while self.wants_trim() && self.trim_cycle(&mut claims) > 0 {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            })?;
        let _ = self.trimmer.set(handle.thread().clone());
        Ok(())
    }

    /// Releases free memory, at most `max_trim` bytes, in rounds that each
    /// hold the lock for a bounded scan and ask the kernel for a bounded
    /// batch outside it. Returns the bytes released.
    pub fn trim_cycle(&self, claims: &mut Vec<Claim>) -> usize {
        let start = Instant::now();
        let mut budget = (self.excess().saturating_sub(self.cfg.keep as u64) as usize)
            .min(self.cfg.max_trim)
            / self.page;
        let (mut visited, mut freed) = (0, 0);
        while budget > 0 {
            claims.clear();
            let (got, extents, seen) = {
                let mut b = self.lock();
                let got = b.claim(claims, budget.min(ROUND_PAGES), SCAN);
                (got, b.extents(), b.scanned())
            };
            visited += seen;
            if !claims.is_empty() {
                for c in claims.iter_mut() {
                    let at = ((c.ext as usize) << self.shift) + c.first_page as usize * self.page;
                    let from = self.base.wrapping_add(at);
                    let len = (c.end_page - c.first_page) as usize * self.page;
                    // SAFETY: whole OS pages inside slots that the claim took
                    // out of `alloc`'s reach; nothing uses them until `finish`.
                    c.released = unsafe { sys::release(from, len) };
                }
                let mut b = self.lock();
                freed += b.finish(claims);
                self.publish(&b);
            }
            budget -= got.min(budget);
            if visited >= extents || (got == 0 && seen == 0) {
                break;
            }
        }
        claims.clear();
        self.residue.store(self.excess(), Relaxed);
        self.released.fetch_add((freed * self.page) as u64, Relaxed);
        self.trims.fetch_add(1, Relaxed);
        self.trim_ns_max
            .fetch_max(start.elapsed().as_nanos() as u64, Relaxed);
        freed * self.page
    }
}

#[cfg(test)]
impl Drop for Arena {
    fn drop(&mut self) {
        // SAFETY: the range `new` reserved; tests do not use it afterwards.
        unsafe { sys::unreserve(self.base, self.range) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 4096-byte page: class 3.
    const C4K: usize = 3;

    fn strides() -> [usize; CLASSES] {
        std::array::from_fn(|i| (512 << i) + 272 + 8)
    }

    fn cfg() -> Config {
        Config {
            // Miri holds the range in its own memory.
            reserved: if cfg!(miri) { 16 << 20 } else { 256 << 20 },
            extent_bytes: 64 << 10,
            keep: 0,
            threshold: 0,
            max_trim: usize::MAX / 2,
            max_regions: 64,
        }
    }

    fn arena(c: Config) -> Arena {
        Arena::new(c, strides()).unwrap()
    }

    fn fill(p: *mut u8, len: usize, v: u8) {
        // SAFETY: tests write inside slots they hold.
        unsafe { std::ptr::write_bytes(p, v, len) };
    }

    fn holds(p: *mut u8, len: usize, v: u8) -> bool {
        // SAFETY: tests read inside slots they hold.
        unsafe { std::slice::from_raw_parts(p, len) }
            .iter()
            .all(|&b| b == v)
    }

    fn trim(a: &Arena) -> usize {
        a.trim_cycle(&mut Vec::with_capacity(MAX_CLAIMS))
    }

    /// Resident bytes of the range as the kernel counts them.
    #[cfg(not(miri))]
    fn kernel_resident(a: &Arena) -> usize {
        sys::resident(a.base, a.range)
    }

    #[test]
    #[cfg(not(miri))]
    fn a_random_run_with_trims_matches_the_kernel_and_keeps_the_data() {
        use std::collections::HashMap;
        let a = arena(cfg());
        let regions: Vec<_> = (0..3).map(|_| a.open_region().unwrap().0).collect();
        let mut live: HashMap<usize, (usize, usize, u8)> = HashMap::new();
        let mut x = 99u64;
        for _ in 0..6000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            if !x.is_multiple_of(3) || live.is_empty() {
                let class = (x >> 8) as usize % 5;
                let p = a.alloc(class, regions[(x >> 20) as usize % 3]);
                assert!(!p.is_null() && a.contains(p));
                let (len, v) = (strides()[class], (x >> 32) as u8);
                fill(p, len, v);
                live.insert(p as usize, (class, len, v));
            } else {
                let key = *live.keys().nth((x >> 8) as usize % live.len()).unwrap();
                let (_, len, v) = live.remove(&key).unwrap();
                assert!(holds(key as *mut u8, len, v));
                // SAFETY: from `alloc`, out of `live`.
                unsafe { a.free(key as *mut u8) };
            }
            if x.is_multiple_of(211) {
                trim(&a);
                assert_eq!(a.resident_bytes() as usize, kernel_resident(&a));
            }
        }
        for (&p, &(_, len, v)) in &live {
            assert!(holds(p as *mut u8, len, v), "a trim damaged a live slot");
        }
        let used: usize = live.values().map(|m| m.1).sum();
        assert_eq!(a.in_use_bytes() as usize, used);
    }

    #[test]
    #[cfg(not(miri))]
    fn pages_are_released_once_and_come_back_zeroed() {
        let a = arena(cfg());
        let (r, _) = a.open_region().unwrap();
        let ps: Vec<_> = (0..200).map(|_| a.alloc(C4K, r)).collect();
        for &p in &ps {
            fill(p, strides()[C4K], 7);
        }
        assert_eq!(a.resident_bytes() as usize, kernel_resident(&a));
        for &p in &ps {
            // SAFETY: from `alloc`, not used afterwards.
            unsafe { a.free(p) };
        }
        let freed = trim(&a);
        assert!(freed > 0);
        assert_eq!(a.resident_bytes() as usize, kernel_resident(&a));
        assert_eq!(a.released_bytes() as usize, freed);
        // Nothing left to release: no second call on the same pages.
        assert_eq!(trim(&a), 0);
        let p = a.alloc(C4K, r);
        assert!(holds(p, strides()[C4K], 0));
        fill(p, strides()[C4K], 9);
        assert_eq!(a.resident_bytes() as usize, kernel_resident(&a));
    }

    #[test]
    fn the_pages_of_one_region_free_up_when_another_stays_busy() {
        let a = arena(cfg());
        let (ra, _) = a.open_region().unwrap();
        let (rb, _) = a.open_region().unwrap();
        let mut mine = Vec::new();
        let mut theirs = Vec::new();
        // Miri runs these byte by byte: fewer blocks, whole extents still.
        for _ in 0..(if cfg!(miri) { 60 } else { 400 }) {
            mine.push(a.alloc(C4K, ra));
            theirs.push(a.alloc(C4K, rb));
        }
        for &p in mine.iter().chain(&theirs) {
            fill(p, strides()[C4K], 5);
        }
        let freed_bytes = mine.len() * strides()[C4K];
        for &p in &mine {
            // SAFETY: from `alloc`, not used afterwards.
            unsafe { a.free(p) };
        }
        let released = trim(&a);
        // Interleaved slots would leave a page shared with a neighbour in
        // use almost everywhere; whole extents free up instead.
        assert!(
            released * 10 >= freed_bytes * 9,
            "released {released} of {freed_bytes}"
        );
        for &p in &theirs {
            assert!(holds(p, strides()[C4K], 5));
        }
    }

    #[test]
    fn a_slot_freed_by_another_thread_is_free_again() {
        let a = arena(cfg());
        let (r, _) = a.open_region().unwrap();
        let first = a.alloc(C4K, r) as usize;
        let second = a.alloc(C4K, r) as usize;
        thread::scope(|s| {
            // SAFETY: from `alloc`, not used afterwards.
            s.spawn(|| unsafe { a.free(first as *mut u8) });
        });
        assert_eq!(a.in_use_bytes() as usize, strides()[C4K]);
        assert_eq!(a.alloc(C4K, r) as usize, first);
        assert_ne!(first, second);
    }

    #[test]
    fn every_page_size_has_slots_and_a_dropped_class_is_not_aliased() {
        let a = arena(cfg());
        let (r, _) = a.open_region().unwrap();
        for class in 0..CLASSES {
            let p = a.alloc(class, r);
            assert!(!p.is_null() && a.contains(p), "class {class}");
            fill(p, strides()[class], class as u8 + 1);
            let q = a.alloc(class, r);
            assert!(!q.is_null());
            assert!(
                (p as usize).abs_diff(q as usize) >= strides()[class],
                "class {class} slots overlap"
            );
            assert!(holds(p, strides()[class], class as u8 + 1));
        }
    }

    #[test]
    fn a_full_range_counts_the_fallback_and_serves_again_after_a_free() {
        let mut c = cfg();
        c.reserved = 128 << 10;
        let a = arena(c);
        let n = (a.range_bytes() / strides()[C4K]).min(14);
        let ps: Vec<_> = (0..n).map(|_| a.alloc(C4K, 0)).collect();
        assert!(ps.iter().all(|p| !p.is_null()));
        assert!(a.alloc(C4K, 0).is_null());
        assert_eq!(a.fallbacks(), 1);
        // SAFETY: from `alloc`, not used afterwards.
        unsafe { a.free(ps[0]) };
        assert_eq!(a.alloc(C4K, 0), ps[0]);
    }

    #[test]
    fn a_range_the_system_refuses_is_an_error_and_a_bad_geometry_another() {
        let refused =
            Arena::with_reserve(cfg(), strides(), |_| Err(io::ErrorKind::OutOfMemory.into()));
        assert_eq!(
            refused.err().map(|e| e.kind()),
            Some(io::ErrorKind::OutOfMemory)
        );
        let mut bad = cfg();
        bad.max_regions = 0;
        let invalid = Arena::with_reserve(bad, strides(), |_| panic!("no range for no tables"));
        assert_eq!(
            invalid.err().map(|e| e.kind()),
            Some(io::ErrorKind::InvalidInput)
        );
    }

    #[test]
    fn contains_is_the_range() {
        let a = arena(cfg());
        let p = a.alloc(C4K, 0);
        assert!(a.contains(p));
        let local = 0u8;
        assert!(!a.contains(&local));
        assert!(!a.contains(std::ptr::null()));
    }

    #[test]
    fn a_trim_cycle_is_bounded_and_the_next_one_goes_on() {
        let mut c = cfg();
        c.max_trim = 64 * 4096;
        let a = arena(c);
        let (r, _) = a.open_region().unwrap();
        let ps: Vec<_> = (0..14 * if cfg!(miri) { 10 } else { 40 })
            .map(|_| a.alloc(C4K, r))
            .collect();
        for &p in &ps {
            fill(p, strides()[C4K], 1);
            // SAFETY: from `alloc`, not used afterwards.
            unsafe { a.free(p) };
        }
        let before = a.resident_bytes() as usize;
        let first = trim(&a);
        // The bound, and at most one extent over it.
        assert!(
            first > 0 && first <= 64 * 4096 + 32 * 4096,
            "released {first}"
        );
        let mut total = first;
        for _ in 0..40 {
            total += trim(&a);
        }
        assert_eq!(total, before);
        assert_eq!(a.resident_bytes(), 0);
    }

    #[test]
    fn wants_trim_waits_for_the_threshold_and_the_remainder() {
        let mut c = cfg();
        c.threshold = 256 << 10;
        let a = arena(c);
        let ps: Vec<_> = (0..200).map(|_| a.alloc(C4K, 0)).collect();
        for &p in &ps {
            fill(p, strides()[C4K], 1);
            // SAFETY: from `alloc`, not used afterwards.
            unsafe { a.free(p) };
        }
        assert!(a.wants_trim());
        trim(&a);
        assert!(!a.wants_trim());
    }

    #[test]
    #[cfg(not(miri))]
    fn the_trim_thread_releases_what_a_poke_announces() {
        let a: &'static Arena = Box::leak(Box::new(arena(cfg())));
        // The thread looks only every 30 s, so a release within the wait is
        // the wake-up's.
        a.start_trimmer(Duration::from_secs(30)).unwrap();
        let ps: Vec<_> = (0..200).map(|_| a.alloc(C4K, 0)).collect();
        for &p in &ps {
            fill(p, strides()[C4K], 1);
            // SAFETY: from `alloc`, not used afterwards.
            unsafe { a.free(p) };
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while a.released_bytes() == 0 && Instant::now() < deadline {
            a.poke();
            thread::sleep(Duration::from_millis(5));
        }
        assert!(a.released_bytes() > 0);
    }
}
