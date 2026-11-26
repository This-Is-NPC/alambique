//! Memory from the BEAM's allocators, counted.
//!
//! Rust allocations and SQLite's go through `enif_alloc`, so ERTS accounts
//! for them (`:erlang.memory/0`, `recon_alloc`) and manages and releases
//! their carriers. The counters in `stats` say which side allocated.
//!
//! The exception is the page-cache blocks of SQLite when the page arena is on
//! (`arena`): they are in a range this library reserved, which ERTS does not
//! see, and the pages of free blocks return to the kernel. Every connection
//! has a page cache of its own (SQLite is built without memory management),
//! and a thread-local names the connection whose call is running.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::{Cell, RefCell};
use std::ffi::c_void;
use std::os::raw::c_int;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering::Relaxed};
use std::sync::OnceLock;

use libsqlite3_sys as ffi;
use rustler::EnifAllocator;

use crate::arena::{self, Arena};
use crate::guard::guard;
use crate::stats::shard;

/// The BEAM's allocator, which unit tests (outside the BEAM) replace by a
/// counting one.
#[cfg(not(test))]
mod heap {
    use std::ffi::c_void;

    /// # Safety
    /// As `enif_alloc`.
    pub unsafe fn alloc(n: usize) -> *mut c_void {
        // SAFETY: `enif_alloc` may be called from any thread.
        unsafe { rustler::sys::enif_alloc(n) }
    }

    /// # Safety
    /// As `enif_free`: `p` came from `alloc` or `realloc`.
    pub unsafe fn free(p: *mut c_void) {
        // SAFETY: by the contract above.
        unsafe { rustler::sys::enif_free(p) }
    }

    /// # Safety
    /// As `enif_realloc`.
    pub unsafe fn realloc(p: *mut c_void, n: usize) -> *mut c_void {
        // SAFETY: by the contract above.
        unsafe { rustler::sys::enif_realloc(p, n) }
    }
}

#[cfg(test)]
mod heap {
    //! Counts the calls the hooks make where they would call ERTS.

    use std::alloc::Layout;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    pub static ALLOCS: AtomicUsize = AtomicUsize::new(0);
    pub static FREES: AtomicUsize = AtomicUsize::new(0);
    /// Room before the pointer for its size.
    const PREFIX: usize = 16;

    fn layout(n: usize) -> Layout {
        Layout::from_size_align(n + PREFIX, 16).unwrap()
    }

    /// # Safety
    /// None needed beyond a positive size.
    pub unsafe fn alloc(n: usize) -> *mut c_void {
        ALLOCS.fetch_add(1, Relaxed);
        // SAFETY: the layout has a positive size.
        let p = unsafe { std::alloc::alloc(layout(n)) };
        // SAFETY: `p` has room for the prefix.
        unsafe {
            *(p as *mut usize) = n;
            p.add(PREFIX) as *mut c_void
        }
    }

    /// # Safety
    /// `p` came from `alloc`.
    pub unsafe fn free(p: *mut c_void) {
        FREES.fetch_add(1, Relaxed);
        // SAFETY: by the contract above.
        unsafe {
            let q = (p as *mut u8).sub(PREFIX);
            std::alloc::dealloc(q, layout(*(q as *mut usize)));
        }
    }

    /// # Safety
    /// `p` came from `alloc`.
    pub unsafe fn realloc(p: *mut c_void, n: usize) -> *mut c_void {
        // SAFETY: by the contract above.
        unsafe {
            let q = alloc(n);
            let old = *((p as *mut u8).sub(PREFIX) as *mut usize);
            std::ptr::copy_nonoverlapping(p as *const u8, q as *mut u8, old.min(n));
            free(p);
            q
        }
    }
}

/// The page arena, set at load when it is on.
static ARENA: OnceLock<Arena> = OnceLock::new();
/// Bytes SQLite adds to the page size in a page-cache block (`PCACHE_HDRSZ`).
static PAGE_HDRSZ: AtomicUsize = AtomicUsize::new(0);
static ARENA_STATE: AtomicU8 = AtomicU8::new(ArenaState::Off as u8);

/// Why the arena is on or off, reported in the stats.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum ArenaState {
    /// Not asked for.
    Off = 0,
    On = 1,
    /// Asked for, but the address range could not be reserved.
    ReserveFailed = 2,
    /// Asked for, but the settings or the platform do not allow it.
    Unsupported = 3,
}

pub fn arena_state() -> ArenaState {
    match ARENA_STATE.load(Relaxed) {
        1 => ArenaState::On,
        2 => ArenaState::ReserveFailed,
        3 => ArenaState::Unsupported,
        _ => ArenaState::Off,
    }
}

pub fn arena() -> Option<&'static Arena> {
    ARENA.get()
}

/// Bytes SQLite adds to the page size in a page-cache block.
pub fn page_hdrsz() -> usize {
    PAGE_HDRSZ.load(Relaxed)
}

/// Wakes the trim thread when the arena has memory to release. Called where a
/// NIF call ends and where a connection closes, never inside a hook.
pub fn poke_trim() {
    if let Some(a) = ARENA.get() {
        a.poke();
    }
}

thread_local! {
    /// The region (connection) whose NIF call the thread is running; 0 for
    /// none. Page-cache blocks allocated meanwhile go to its extents.
    static REGION: Cell<u32> = const { Cell::new(0) };
}

/// Makes `slot` the region of the thread's allocations until dropped, then
/// restores the previous one.
pub struct RegionGuard(u32);

impl RegionGuard {
    pub fn enter(slot: u32) -> RegionGuard {
        RegionGuard(REGION.try_with(|r| r.replace(slot)).unwrap_or(0))
    }
}

impl Drop for RegionGuard {
    fn drop(&mut self) {
        let _ = REGION.try_with(|r| r.set(self.0));
    }
}

/// The region of one connection in the arena, closed when it drops. Region 0
/// (no arena, or the table full) is shared and never closes.
pub struct Region {
    slot: u32,
    id: u64,
}

impl Region {
    pub fn open() -> Region {
        match ARENA.get().and_then(Arena::open_region) {
            Some((slot, id)) => Region { slot, id },
            None => Region { slot: 0, id: 0 },
        }
    }

    pub fn slot(&self) -> u32 {
        self.slot
    }

    /// Closes the region; again does nothing.
    pub fn close(&self) {
        if let Some(a) = ARENA.get().filter(|_| self.slot != 0) {
            a.close_region(self.slot, self.id);
        }
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        self.close();
    }
}

/// The class of a request for `n` bytes: a page-cache block is a power-of-two
/// page and SQLite's headers.
fn arena_class(n: usize) -> Option<usize> {
    let page = n.checked_sub(PAGE_HDRSZ.load(Relaxed))?;
    if (512..=65536).contains(&page) && page.is_power_of_two() {
        Some(page.trailing_zeros() as usize - 9)
    } else {
        None
    }
}

/// The global allocator of the NIF library (not in unit tests, which run
/// outside the BEAM).
#[cfg_attr(any(test, feature = "fuzzing"), allow(dead_code))]
pub struct Counted;

// SAFETY: both methods forward to `EnifAllocator` with the caller's arguments.
unsafe impl GlobalAlloc for Counted {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        shard().rust_alloc.fetch_add(layout.size() as u64, Relaxed);
        // SAFETY: the caller upholds the `GlobalAlloc::alloc` contract.
        unsafe { EnifAllocator.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        shard().rust_free.fetch_add(layout.size() as u64, Relaxed);
        // SAFETY: the caller upholds the `GlobalAlloc::dealloc` contract.
        unsafe { EnifAllocator.dealloc(ptr, layout) }
    }
}

/// SQLite needs the size of a block (`xSize`), which `enif_alloc` does not
/// keep, so every block has an 8-byte size header (and stays 8-aligned).
const HEADER: usize = 8;

// ERTS serializes the allocations of dirty scheduler threads on one lock, and
// SQLite makes thousands of short-lived small allocations per statement. A
// thread therefore keeps the small blocks it frees, by size class, and
// reuses them. The cache is emptied when the NIF call ends: blocks that
// stay cached would keep ERTS carriers from being released.
const CLASS: usize = 16;
const CACHE_MAX: usize = 1024;
const CLASSES: usize = CACHE_MAX / CLASS;
/// Bytes kept per class; at least 4 blocks.
const CACHE_BUDGET: usize = 32 * 1024;

struct Cache {
    /// Singly linked free lists; the link is the first word of the payload.
    /// Only `push` puts blocks here, and only `pop` and `flush` take them.
    head: [*mut usize; CLASSES],
    len: [u32; CLASSES],
    kept: u32,
}

impl Cache {
    /// Keeps `block` for reuse; false when the class is full.
    ///
    /// # Safety
    /// `block` is a live `enif_alloc` block of the class, with room for the
    /// header and the link, that the caller does not use afterwards.
    unsafe fn push(&mut self, class: usize, block: *mut usize) -> bool {
        if self.len[class] as usize >= (CACHE_BUDGET / ((class + 1) * CLASS)).max(4) {
            return false;
        }
        // SAFETY: by the contract above.
        unsafe { *block.add(1) = self.head[class] as usize };
        self.head[class] = block;
        self.len[class] += 1;
        self.kept += 1;
        true
    }

    /// A kept block of the class, or null.
    fn pop(&mut self, class: usize) -> *mut usize {
        let block = self.head[class];
        if !block.is_null() {
            // SAFETY: `push` made `block` a live block whose first payload
            // word is the link.
            self.head[class] = unsafe { *block.add(1) } as *mut usize;
            self.len[class] -= 1;
            self.kept -= 1;
        }
        block
    }

    /// Hands every kept block back to ERTS.
    fn flush(&mut self) {
        if self.kept == 0 {
            return;
        }
        for class in 0..CLASSES {
            loop {
                let block = self.pop(class);
                if block.is_null() {
                    break;
                }
                // SAFETY: `pop` returned a live `enif_alloc` block that
                // nothing else holds.
                unsafe { heap::free(block as *mut c_void) };
            }
        }
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        self.flush();
    }
}

thread_local! {
    static CACHE: RefCell<Cache> = const {
        RefCell::new(Cache {
            head: [std::ptr::null_mut(); CLASSES],
            len: [0; CLASSES],
            kept: 0,
        })
    };
}

/// Returns the calling thread's cached blocks to ERTS.
pub fn flush_thread_cache() {
    let _ = CACHE.try_with(|c| c.borrow_mut().flush());
}

fn class_of(n: usize) -> usize {
    (n.max(1) - 1) / CLASS
}

/// A new block of `size` bytes from ERTS, or null.
fn enif_block(size: usize) -> *mut usize {
    // SAFETY: the BEAM's allocator may be called from any thread.
    unsafe { heap::alloc(size) as *mut usize }
}

/// A block with room for `n` bytes. Small blocks have the size of their
/// class, so any block of a class can serve any request of it.
fn raw_alloc(n: usize) -> *mut usize {
    if let Some(a) = ARENA.get() {
        if let Some(class) = arena_class(n) {
            let region = REGION.try_with(Cell::get).unwrap_or(0);
            let block = a.alloc(class, region) as *mut usize;
            if !block.is_null() {
                return block;
            }
            // Range exhausted (counted): the BEAM serves it, and `x_free`
            // tells it apart by address.
        }
    }
    if n > CACHE_MAX {
        return enif_block(n + HEADER);
    }
    let class = class_of(n);
    let cached = CACHE
        .try_with(|c| c.borrow_mut().pop(class))
        .unwrap_or(std::ptr::null_mut());
    if cached.is_null() {
        enif_block((class + 1) * CLASS + HEADER)
    } else {
        cached
    }
}

/// # Safety
/// `block` came from `raw_alloc(n)` and is not used afterwards.
unsafe fn raw_free(block: *mut usize, n: usize) {
    if n <= CACHE_MAX {
        let class = class_of(n);
        // SAFETY: by the contract above, `block` is a live block of `class`.
        let kept = CACHE
            .try_with(|c| unsafe { c.borrow_mut().push(class, block) })
            .unwrap_or(false);
        if kept {
            return;
        }
    }
    // SAFETY: `block` came from `enif_alloc` and is not cached.
    unsafe { heap::free(block as *mut c_void) };
}

// Every SQLite hook runs through `guard`: a panic returns the failure value
// (null for allocations, which SQLite reports as out of memory).

/// # Safety
/// SQLite's `xMalloc` contract: `n` is positive.
unsafe extern "C" fn x_malloc(n: c_int) -> *mut c_void {
    guard("x_malloc", std::ptr::null_mut(), || {
        let block = raw_alloc(n as usize);
        if block.is_null() {
            return block as *mut c_void;
        }
        shard().sqlite_alloc.fetch_add(n as u64, Relaxed);
        // SAFETY: the block has room for the header and `n` bytes.
        unsafe {
            *block = n as usize;
            block.add(1) as *mut c_void
        }
    })
}

/// # Safety
/// SQLite's `xFree` contract: `p` came from these hooks and is not used again.
unsafe extern "C" fn x_free(p: *mut c_void) {
    guard("x_free", (), || {
        // SAFETY: every block has its header, the size `raw_alloc` was asked
        // for, before the pointer SQLite holds.
        unsafe {
            let block = (p as *mut usize).sub(1);
            let n = *block;
            shard().sqlite_free.fetch_add(n as u64, Relaxed);
            match ARENA.get() {
                Some(a) if a.contains(block as *const u8) => a.free(block as *mut u8),
                _ => raw_free(block, n),
            }
        }
    })
}

/// # Safety
/// SQLite's `xRealloc` contract: `p` came from these hooks, `n` is positive.
unsafe extern "C" fn x_realloc(p: *mut c_void, n: c_int) -> *mut c_void {
    guard("x_realloc", std::ptr::null_mut(), || {
        // SAFETY: every block has its header before the pointer SQLite holds;
        // `x_malloc` and `x_free` have SQLite's contracts, which hold here;
        // the copy stays inside both blocks; a block of a cache-free size
        // is a live `enif_alloc` block, which `enif_realloc` may move.
        unsafe {
            let old = (p as *mut usize).sub(1);
            let old_size = *old;
            let n = n as usize;
            let in_arena = ARENA
                .get()
                .is_some_and(|a| a.contains(old as *const u8) || arena_class(n).is_some());
            if old_size <= CACHE_MAX || n <= CACHE_MAX || in_arena {
                // A cached size class or the arena is involved: move to a
                // block of the new size.
                let q = x_malloc(n as c_int);
                if !q.is_null() {
                    std::ptr::copy_nonoverlapping(p as *const u8, q as *mut u8, old_size.min(n));
                    x_free(p);
                }
                return q;
            }
            let block = heap::realloc(old as *mut c_void, n + HEADER) as *mut usize;
            if block.is_null() {
                return block as *mut c_void;
            }
            *block = n;
            shard().sqlite_free.fetch_add(old_size as u64, Relaxed);
            shard().sqlite_alloc.fetch_add(n as u64, Relaxed);
            block.add(1) as *mut c_void
        }
    })
}

/// # Safety
/// SQLite's `xSize` contract: `p` came from these hooks.
unsafe extern "C" fn x_size(p: *mut c_void) -> c_int {
    // SAFETY: every block has its header before the pointer SQLite holds.
    guard("x_size", 0, || unsafe { *(p as *mut usize).sub(1) }
        as c_int)
}

extern "C" fn x_roundup(n: c_int) -> c_int {
    guard("x_roundup", n, || (n + 7) & !7)
}

extern "C" fn x_init(_: *mut c_void) -> c_int {
    ffi::SQLITE_OK
}

extern "C" fn x_shutdown(_: *mut c_void) {}

/// Refuses a SQLite built with `SQLITE_ENABLE_MEMORY_MANAGEMENT`: that option
/// pools the page caches of all connections at run time, so `cache_kib` would
/// be a share of a pool and the shrink of an idle cell would free nothing
/// (`.cargo/config.toml` builds without it).
fn refuse_pooled_caches(memory_management: bool) -> Result<(), String> {
    if memory_management {
        return Err("SQLite is built with SQLITE_ENABLE_MEMORY_MANAGEMENT; \
                    build it with -USQLITE_ENABLE_MEMORY_MANAGEMENT (LIBSQLITE3_FLAGS) \
                    from native/alambique_nif"
            .into());
    }
    Ok(())
}

/// How long the trim thread waits for a poke before it looks anyway. Pokes
/// come with every free that leaves memory to release, so this only covers a
/// missed wake-up.
const TRIM_IDLE: std::time::Duration = std::time::Duration::from_secs(30);

/// What the load data asks of the arena.
pub struct ArenaRequest {
    pub enabled: bool,
    pub config: arena::Config,
}

/// The slot bytes of each class: the page, SQLite's headers and ours.
fn strides(hdrsz: usize) -> [usize; arena::CLASSES] {
    std::array::from_fn(|i| (512 << i) + hdrsz + HEADER)
}

/// Why the arena could not start: settings it cannot use, or a range the
/// system would not give.
fn failure_state(e: &std::io::Error) -> ArenaState {
    if e.kind() == std::io::ErrorKind::InvalidInput {
        ArenaState::Unsupported
    } else {
        ArenaState::ReserveFailed
    }
}

/// Sets up the arena if asked; on failure the library runs without it and
/// `arena_state` says why.
fn start_arena(req: &ArenaRequest, hdrsz: usize) {
    let state = if !req.enabled {
        ArenaState::Off
    } else if hdrsz == 0 || !hdrsz.is_multiple_of(8) || !cfg!(target_os = "linux") {
        ArenaState::Unsupported
    } else {
        match Arena::new(req.config, strides(hdrsz)) {
            Ok(a) => {
                let _ = ARENA.set(a);
                match ARENA.get().map(|a| a.start_trimmer(TRIM_IDLE)) {
                    Some(Ok(())) => ArenaState::On,
                    _ => ArenaState::Unsupported,
                }
            }
            Err(e) => {
                eprintln!("alambique_nif: page arena off: {e}");
                failure_state(&e)
            }
        }
    };
    ARENA_STATE.store(state as u8, Relaxed);
}

/// Makes SQLite allocate through the BEAM, with a page cache of its own for
/// each connection and, when asked, the page-cache blocks in the arena. Must
/// run before SQLite initializes; SQLite copies the table.
pub fn install_sqlite(req: &ArenaRequest) -> Result<(), String> {
    // SAFETY: a NUL-terminated name; the function only reads it.
    let pooled = unsafe { ffi::sqlite3_compileoption_used(c"ENABLE_MEMORY_MANAGEMENT".as_ptr()) };
    refuse_pooled_caches(pooled != 0)?;
    let methods = ffi::sqlite3_mem_methods {
        xMalloc: Some(x_malloc),
        xFree: Some(x_free),
        xRealloc: Some(x_realloc),
        xSize: Some(x_size),
        xRoundup: Some(x_roundup),
        xInit: Some(x_init),
        xShutdown: Some(x_shutdown),
        pAppData: std::ptr::null_mut(),
    };
    let mut hdrsz: c_int = 0;
    // SAFETY: SQLite copies the table; `sqlite3_config` runs before
    // initialization (`on_load`). `PCACHE_HDRSZ` writes one int. `PAGECACHE`
    // with no buffer keeps SQLite from carving pages out of bulk blocks,
    // which a shrink could not free.
    let ok = unsafe {
        ffi::sqlite3_config(
            ffi::SQLITE_CONFIG_MALLOC,
            &methods as *const ffi::sqlite3_mem_methods,
        ) == ffi::SQLITE_OK
            && ffi::sqlite3_config(ffi::SQLITE_CONFIG_PCACHE_HDRSZ, &mut hdrsz as *mut c_int)
                == ffi::SQLITE_OK
            && ffi::sqlite3_config(
                ffi::SQLITE_CONFIG_PAGECACHE,
                std::ptr::null_mut::<c_void>(),
                0 as c_int,
                0 as c_int,
            ) == ffi::SQLITE_OK
    };
    if !ok {
        return Err("sqlite3_config failed".into());
    }
    PAGE_HDRSZ.store(hdrsz.max(0) as usize, Relaxed);
    start_arena(req, hdrsz.max(0) as usize);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guard::probe;
    use std::sync::{Mutex, MutexGuard, Once};

    /// Counts the allocations of the calling thread, so a test can tell that
    /// a stretch of code allocates nothing.
    struct Counting;

    thread_local! {
        static ALLOCS: Cell<usize> = const { Cell::new(0) };
    }

    // SAFETY: forwards to the system allocator with the caller's arguments.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
            // SAFETY: the caller upholds the `GlobalAlloc::alloc` contract.
            unsafe { std::alloc::System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: the caller upholds the `GlobalAlloc::dealloc` contract.
            unsafe { std::alloc::System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
            // SAFETY: the caller upholds the `GlobalAlloc::realloc` contract.
            unsafe { std::alloc::System.realloc(ptr, layout, size) }
        }
    }

    #[global_allocator]
    static COUNTING: Counting = Counting;

    fn thread_allocs() -> usize {
        ALLOCS.with(Cell::get)
    }

    const HDRSZ: usize = 272;

    /// The hooks use one arena and the counters of `heap`: one test at a time.
    static HOOKS: Mutex<()> = Mutex::new(());

    fn hooks() -> (MutexGuard<'static, ()>, &'static Arena) {
        let guard = HOOKS.lock().unwrap_or_else(|e| e.into_inner());
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            PAGE_HDRSZ.store(HDRSZ, Relaxed);
            let config = arena::Config {
                // Miri holds the range in its own memory.
                reserved: if cfg!(miri) { 16 << 20 } else { 256 << 20 },
                extent_bytes: 64 << 10,
                keep: 0,
                threshold: 0,
                max_trim: usize::MAX / 2,
                max_regions: 64,
            };
            let arena = Arena::new(config, strides(HDRSZ)).unwrap();
            assert!(ARENA.set(arena).is_ok());
        });
        (guard, ARENA.get().unwrap())
    }

    /// SQLite asks for the page and its headers.
    fn page_block(class: usize) -> c_int {
        ((512 << class) + HDRSZ) as c_int
    }

    fn current() -> u32 {
        REGION.with(Cell::get)
    }

    #[test]
    fn page_blocks_cost_no_call_to_the_heap_and_no_allocation() {
        let (_g, a) = hooks();
        let (heap_allocs, heap_frees) = (heap::ALLOCS.load(Relaxed), heap::FREES.load(Relaxed));
        let mut blocks = Vec::with_capacity(8 * 40);
        let before = thread_allocs();
        let region = Region::open();
        let _in = RegionGuard::enter(region.slot());
        for class in 0..CLASSES {
            for _ in 0..40 {
                // SAFETY: SQLite's contract: a positive size.
                let p = unsafe { x_malloc(page_block(class)) };
                assert!(!p.is_null() && a.contains(p as *const u8), "class {class}");
                // SAFETY: `p` is a live block of that size.
                assert_eq!(unsafe { x_size(p) }, page_block(class));
                blocks.push(p);
            }
        }
        for p in blocks {
            // SAFETY: from `x_malloc`, not used again.
            unsafe { x_free(p) };
        }
        drop(_in);
        drop(region);
        assert_eq!(thread_allocs(), before, "a hook allocated");
        assert_eq!(heap::ALLOCS.load(Relaxed), heap_allocs);
        assert_eq!(heap::FREES.load(Relaxed), heap_frees);
    }

    #[test]
    fn blocks_that_are_not_pages_go_to_the_heap_and_come_back_by_address() {
        let (_g, a) = hooks();
        let (allocs, frees) = (heap::ALLOCS.load(Relaxed), heap::FREES.load(Relaxed));
        // Not a page size plus the headers.
        // SAFETY: SQLite's contract: a positive size.
        let p = unsafe { x_malloc(5000) };
        assert!(!p.is_null() && !a.contains(p as *const u8));
        assert_eq!(heap::ALLOCS.load(Relaxed), allocs + 1);
        // SAFETY: from `x_malloc`, not used again.
        unsafe { x_free(p) };
        assert_eq!(heap::FREES.load(Relaxed), frees + 1);
    }

    #[test]
    fn realloc_moves_a_block_between_the_arena_and_the_heap() {
        let (_g, a) = hooks();
        // SAFETY: SQLite's contract for each hook: live blocks of these
        // hooks, positive sizes; the copies stay inside the blocks.
        unsafe {
            let p = x_malloc(page_block(3)) as *mut u8;
            assert!(a.contains(p));
            std::ptr::write_bytes(p, 0xAB, page_block(3) as usize);
            let q = x_realloc(p as *mut c_void, 6000) as *mut u8;
            assert!(!q.is_null() && !a.contains(q));
            assert_eq!(x_size(q as *mut c_void), 6000);
            assert!(std::slice::from_raw_parts(q, page_block(3) as usize)
                .iter()
                .all(|&b| b == 0xAB));
            let r = x_realloc(q as *mut c_void, page_block(3)) as *mut u8;
            assert!(a.contains(r));
            assert!(std::slice::from_raw_parts(r, page_block(3) as usize)
                .iter()
                .all(|&b| b == 0xAB));
            x_free(r as *mut c_void);
        }
    }

    #[test]
    fn a_full_range_falls_back_to_the_heap_and_frees_by_address() {
        let (_g, a) = hooks();
        let before = a.fallbacks();
        let mut blocks = Vec::new();
        let mut spilled = None;
        // 64 KiB pages take one extent each: the range is soon used up.
        for _ in 0..4096 {
            // SAFETY: SQLite's contract: a positive size.
            let p = unsafe { x_malloc(page_block(7)) };
            assert!(!p.is_null(), "the heap serves what the arena cannot");
            if !a.contains(p as *const u8) {
                spilled = Some(p);
                break;
            }
            blocks.push(p);
        }
        assert!(spilled.is_some(), "the range was never used up");
        assert!(a.fallbacks() > before);
        for p in blocks.into_iter().chain(spilled) {
            // SAFETY: from `x_malloc`, not used again.
            unsafe { x_free(p) };
        }
    }

    #[test]
    fn a_failure_to_start_is_reported_by_its_cause() {
        use std::io::{Error, ErrorKind};
        assert_eq!(
            failure_state(&Error::from(ErrorKind::OutOfMemory)),
            ArenaState::ReserveFailed
        );
        assert_eq!(
            failure_state(&Error::from(ErrorKind::InvalidInput)),
            ArenaState::Unsupported
        );
    }

    #[test]
    fn the_region_guard_nests_and_restores() {
        assert_eq!(current(), 0);
        {
            let _a = RegionGuard::enter(3);
            assert_eq!(current(), 3);
            {
                let _b = RegionGuard::enter(5);
                assert_eq!(current(), 5);
            }
            assert_eq!(current(), 3);
        }
        assert_eq!(current(), 0);
    }

    #[test]
    fn blocks_come_from_the_extents_of_the_connection_that_asks() {
        let (_g, a) = hooks();
        let (ra, rb) = (Region::open(), Region::open());
        assert_ne!(ra.slot(), rb.slot());
        let (mut ea, mut eb) = (Vec::new(), Vec::new());
        let mut blocks = Vec::new();
        for _ in 0..60 {
            for (region, extents) in [(&ra, &mut ea), (&rb, &mut eb)] {
                let _in = RegionGuard::enter(region.slot());
                // SAFETY: SQLite's contract: a positive size.
                let p = unsafe { x_malloc(page_block(3)) };
                extents.push(a.extent_of(p as *const u8));
                blocks.push(p);
            }
        }
        assert!(ea.iter().all(|e| !eb.contains(e)));
        for p in blocks {
            // SAFETY: from `x_malloc`, not used again.
            unsafe { x_free(p) };
        }
    }

    #[test]
    fn a_region_closes_once_and_frees_its_slot() {
        let (_g, a) = hooks();
        let open = a.open_regions();
        let region = Region::open();
        assert_eq!(a.open_regions(), open + 1);
        region.close();
        region.close();
        assert_eq!(a.open_regions(), open);
        drop(region);
        assert_eq!(a.open_regions(), open);
    }

    // The hooks need ERTS to allocate, so these only prove what a panic does.
    #[test]
    fn a_panic_in_a_hook_returns_its_failure_value() {
        // SAFETY: the probe panics before the hooks touch their arguments.
        unsafe {
            let _p = probe::panic_at("x_malloc");
            assert!(x_malloc(16).is_null());
            let _p = probe::panic_at("x_realloc");
            assert!(x_realloc(std::ptr::null_mut(), 16).is_null());
            let _p = probe::panic_at("x_free");
            x_free(std::ptr::null_mut());
            let _p = probe::panic_at("x_size");
            assert_eq!(x_size(std::ptr::null_mut()), 0);
        }
        let _p = probe::panic_at("x_roundup");
        assert_eq!(x_roundup(13), 13);
    }

    // The crate's .cargo/config.toml sets the flag; a build started from
    // another directory would not read it.
    #[test]
    fn sqlite_is_built_without_memory_management() {
        // SAFETY: a NUL-terminated name.
        let used = unsafe { ffi::sqlite3_compileoption_used(c"ENABLE_MEMORY_MANAGEMENT".as_ptr()) };
        assert_eq!(used, 0);
    }

    #[test]
    fn a_build_with_memory_management_is_refused() {
        assert!(refuse_pooled_caches(true).is_err());
        assert!(refuse_pooled_caches(false).is_ok());
    }

    #[test]
    fn roundup_is_a_multiple_of_eight() {
        assert_eq!(x_roundup(1), 8);
        assert_eq!(x_roundup(16), 16);
        assert_eq!(x_roundup(17), 24);
    }
}
