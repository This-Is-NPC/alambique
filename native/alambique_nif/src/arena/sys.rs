//! The system calls of the arena, in one place. Under Miri the range is a
//! heap block and releasing is a no-op, so the rest can be interpreted.

use std::io;

/// The OS page size.
#[cfg(all(not(miri), target_os = "linux"))]
pub fn page_size() -> usize {
    // SAFETY: `sysconf` has no preconditions.
    (unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).max(4096) as usize
}

#[cfg(miri)]
pub fn page_size() -> usize {
    4096
}

/// Reserves `len` bytes of address space, read-write, not counted against
/// the overcommit limit and never backed by huge pages.
#[cfg(all(not(miri), target_os = "linux"))]
pub fn reserve(len: usize) -> io::Result<*mut u8> {
    // SAFETY: a fresh anonymous mapping that nothing else refers to.
    let base = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        )
    };
    if base == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `base..base + len` is the mapping made above. Failing is
    // harmless: pages are only less likely to be returned whole.
    unsafe { libc::madvise(base, len, libc::MADV_NOHUGEPAGE) };
    Ok(base as *mut u8)
}

#[cfg(miri)]
pub fn reserve(len: usize) -> io::Result<*mut u8> {
    let layout = std::alloc::Layout::from_size_align(len, 4096).map_err(io::Error::other)?;
    // SAFETY: `len` is not zero (the caller checks).
    let p = unsafe { std::alloc::alloc_zeroed(layout) };
    if p.is_null() {
        Err(io::ErrorKind::OutOfMemory.into())
    } else {
        Ok(p)
    }
}

/// Gives the pages of `ptr..ptr + len` back to the kernel; they read as zero
/// afterwards. False when the kernel refused.
///
/// # Safety
/// Whole OS pages of the reserved range that nothing uses.
#[cfg(all(not(miri), target_os = "linux"))]
pub unsafe fn release(ptr: *mut u8, len: usize) -> bool {
    // SAFETY: by the contract above.
    unsafe { libc::madvise(ptr as *mut libc::c_void, len, libc::MADV_DONTNEED) == 0 }
}

/// # Safety
/// As the real one.
#[cfg(miri)]
pub unsafe fn release(_ptr: *mut u8, _len: usize) -> bool {
    true
}

/// Bytes of `ptr..ptr + len` the kernel reports resident (tests).
#[cfg(all(test, not(miri), target_os = "linux"))]
pub fn resident(ptr: *mut u8, len: usize) -> usize {
    let page = page_size();
    let mut v = vec![0u8; len / page];
    // SAFETY: `v` has one byte per page of the mapping.
    let rc = unsafe { libc::mincore(ptr as *mut libc::c_void, len, v.as_mut_ptr() as *mut _) };
    assert_eq!(rc, 0);
    v.iter().filter(|&&b| b & 1 == 1).count() * page
}

// Elsewhere there is no arena: `reserve` fails, so the load leaves it off.

#[cfg(all(not(miri), not(target_os = "linux")))]
pub fn page_size() -> usize {
    4096
}

#[cfg(all(not(miri), not(target_os = "linux")))]
pub fn reserve(_len: usize) -> io::Result<*mut u8> {
    Err(io::ErrorKind::Unsupported.into())
}

/// # Safety
/// As the real one.
#[cfg(all(not(miri), not(target_os = "linux")))]
pub unsafe fn release(_ptr: *mut u8, _len: usize) -> bool {
    false
}

/// Returns the range to the system (tests).
///
/// # Safety
/// `ptr` and `len` are what `reserve` returned and was asked; nothing uses the
/// range afterwards.
#[cfg(all(test, not(miri), target_os = "linux"))]
pub unsafe fn unreserve(ptr: *mut u8, len: usize) {
    // SAFETY: by the contract above.
    unsafe { libc::munmap(ptr as *mut libc::c_void, len) };
}

/// # Safety
/// As the real one.
#[cfg(miri)]
pub unsafe fn unreserve(ptr: *mut u8, len: usize) {
    let layout = std::alloc::Layout::from_size_align(len, 4096).unwrap();
    // SAFETY: the block `reserve` allocated with this layout.
    unsafe { std::alloc::dealloc(ptr, layout) };
}
