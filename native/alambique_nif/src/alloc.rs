//! Memory from the BEAM's allocators, counted.
//!
//! Rust allocations and SQLite's go through `enif_alloc`, so ERTS accounts
//! for them (`:erlang.memory/0`, `recon_alloc`) and manages and releases
//! their carriers. The counters in `stats` say which side allocated.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::RefCell;
use std::ffi::c_void;
use std::os::raw::c_int;
use std::sync::atomic::Ordering::Relaxed;

use libsqlite3_sys as ffi;
use rustler::sys::{enif_alloc, enif_free, enif_realloc};
use rustler::EnifAllocator;

use crate::guard::guard;
use crate::stats::shard;

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
                unsafe { enif_free(block as *mut c_void) };
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
    // SAFETY: `enif_alloc` may be called from any thread.
    unsafe { enif_alloc(size) as *mut usize }
}

/// A block with room for `n` bytes. Small blocks have the size of their
/// class, so any block of a class can serve any request of it.
fn raw_alloc(n: usize) -> *mut usize {
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
    unsafe { enif_free(block as *mut c_void) };
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
            raw_free(block, n);
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
            if old_size <= CACHE_MAX || n <= CACHE_MAX {
                // A cached size class is involved: move to a block of the
                // new size.
                let q = x_malloc(n as c_int);
                if !q.is_null() {
                    std::ptr::copy_nonoverlapping(p as *const u8, q as *mut u8, old_size.min(n));
                    x_free(p);
                }
                return q;
            }
            let block = enif_realloc(old as *mut c_void, n + HEADER) as *mut usize;
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

/// Makes SQLite allocate through the BEAM. Must run before SQLite
/// initializes; SQLite copies the table.
pub fn install_sqlite() -> bool {
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
    // SAFETY: SQLite copies the table; `sqlite3_config` runs before
    // initialization (`on_load`).
    unsafe {
        ffi::sqlite3_config(
            ffi::SQLITE_CONFIG_MALLOC,
            &methods as *const ffi::sqlite3_mem_methods,
        ) == ffi::SQLITE_OK
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guard::probe;

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

    #[test]
    fn roundup_is_a_multiple_of_eight() {
        assert_eq!(x_roundup(1), 8);
        assert_eq!(x_roundup(16), 16);
        assert_eq!(x_roundup(17), 24);
    }
}
