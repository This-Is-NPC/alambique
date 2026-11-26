//! Bookkeeping of the page arena: which slots of which extents are free, who
//! owns each extent and which OS pages are resident. It works on offsets and
//! touches no memory, so Miri can check it.
//!
//! The range is cut into extents of one size (a power of two, `shift`). An
//! extent holds the slots of one class while it is owned by a region (one
//! connection), and goes back to a pool when all its slots are free. Every
//! table is a `Vec` with its capacity reserved at `new`; none grows past it,
//! so no operation allocates.

/// Page sizes 512, 1024, ... 65536: one class each.
pub const CLASSES: usize = 8;
/// Most claims one `claim` call takes.
pub const MAX_CLAIMS: usize = 256;
const NONE: u32 = u32::MAX;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    /// Holds slots of its class for a region.
    Owned,
    /// All slots free, some pages resident.
    Hot,
    /// All slots free, no page resident.
    Cold,
    /// All slots free, taken by a trim that is releasing its pages.
    Trimming,
}

#[derive(Clone, Copy)]
struct Ext {
    class: u8,
    state: State,
    /// The region slot and id that own it; a closed region no longer matches.
    owner: u32,
    owner_id: u64,
    free: u32,
    /// Slots a trim holds: neither free nor in use.
    claimed: u32,
    prev: u32,
    next: u32,
}

struct Region {
    id: u64,
    live: bool,
    /// Per class: extents of this region with a free slot.
    avail: [u32; CLASSES],
}

#[derive(Clone, Copy)]
enum List {
    Hot,
    Cold,
    Avail(u32, usize),
}

/// A run of free slots of an extent that a trim took, with the OS pages it
/// may release (`first..end`, relative to the extent).
#[derive(Clone, Copy, Debug)]
pub struct Claim {
    pub ext: u32,
    pub first_slot: u32,
    pub count: u32,
    pub first_page: u32,
    pub end_page: u32,
    /// The whole extent, taken from the hot pool.
    pub whole: bool,
    /// Set by the caller once the kernel took the pages back.
    pub released: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct Geometry {
    pub page: usize,
    /// Bytes of the range.
    pub reserved: usize,
    /// Target size of an extent; a class takes whole slots, at least one.
    pub extent_bytes: usize,
    /// Slot bytes (block, SQLite's headers and ours) of each class.
    pub strides: [usize; CLASSES],
    /// Most regions open at once, region 0 included.
    pub max_regions: usize,
}

pub struct Book {
    page: usize,
    shift: u32,
    strides: [usize; CLASSES],
    slots: [usize; CLASSES],
    /// Bytes of an extent a class uses (a multiple of the OS page).
    used_bytes: [usize; CLASSES],
    pages_per_ext: usize,
    free_words: usize,
    rel_words: usize,
    max_ext: usize,
    exts: Vec<Ext>,
    /// Per extent: bit set = slot free.
    free: Vec<u64>,
    /// Per extent: bit set = OS page not resident.
    rel: Vec<u64>,
    regions: Vec<Region>,
    max_regions: usize,
    spare_regions: Vec<u32>,
    next_id: u64,
    hot: u32,
    cold: u32,
    cursor: usize,
    /// Extents the last `claim` looked at.
    scanned: usize,
    in_use: u64,
    resident_pages: u64,
    open_regions: u64,
}

fn round_up(n: usize, to: usize) -> usize {
    n.div_ceil(to) * to
}

impl Book {
    /// `None` when the geometry cannot hold an extent.
    pub fn new(g: Geometry) -> Option<Book> {
        let mut slots = [0; CLASSES];
        let mut used_bytes = [0; CLASSES];
        for (c, &stride) in g.strides.iter().enumerate() {
            if stride == 0 || !stride.is_multiple_of(8) {
                return None;
            }
            slots[c] = (g.extent_bytes / stride).max(1);
            used_bytes[c] = round_up(slots[c] * stride, g.page);
        }
        let ext_bytes = used_bytes.iter().copied().max()?.next_power_of_two();
        let max_ext = (g.reserved / ext_bytes).min(NONE as usize - 1);
        if max_ext == 0 || g.max_regions == 0 || !g.page.is_power_of_two() {
            return None;
        }
        let pages_per_ext = ext_bytes / g.page;
        let free_words = slots.iter().copied().max()?.div_ceil(64);
        let rel_words = pages_per_ext.div_ceil(64);
        // Every table gets its whole capacity now; a failure is a refusal,
        // not an abort of the VM.
        let mut regions = Vec::new();
        regions.try_reserve_exact(g.max_regions).ok()?;
        regions.push(Region {
            id: 0,
            live: true,
            avail: [NONE; CLASSES],
        });
        let mut exts = Vec::new();
        exts.try_reserve_exact(max_ext).ok()?;
        let mut free = Vec::new();
        free.try_reserve_exact(max_ext.checked_mul(free_words)?)
            .ok()?;
        let mut rel = Vec::new();
        rel.try_reserve_exact(max_ext.checked_mul(rel_words)?)
            .ok()?;
        let mut spare_regions = Vec::new();
        spare_regions.try_reserve_exact(g.max_regions).ok()?;
        Some(Book {
            page: g.page,
            shift: ext_bytes.trailing_zeros(),
            strides: g.strides,
            slots,
            used_bytes,
            pages_per_ext,
            free_words,
            rel_words,
            max_ext,
            exts,
            free,
            rel,
            regions,
            max_regions: g.max_regions,
            spare_regions,
            next_id: 1,
            hot: NONE,
            cold: NONE,
            cursor: 0,
            scanned: 0,
            in_use: 0,
            resident_pages: 0,
            open_regions: 0,
        })
    }

    /// Extents the last `claim` went through whole.
    pub fn scanned(&self) -> usize {
        self.scanned
    }

    /// Bytes of the range the extents cover.
    pub fn range_bytes(&self) -> usize {
        self.max_ext << self.shift
    }

    /// Bytes of an extent.
    pub fn extent_bytes(&self) -> usize {
        1 << self.shift
    }

    #[cfg(test)]
    pub fn slots(&self, class: usize) -> usize {
        self.slots[class]
    }

    #[cfg(test)]
    pub fn stride(&self, class: usize) -> usize {
        self.strides[class]
    }

    /// Extents carved out of the range so far.
    pub fn extents(&self) -> usize {
        self.exts.len()
    }

    pub fn in_use(&self) -> u64 {
        self.in_use
    }

    pub fn resident_bytes(&self) -> u64 {
        self.resident_pages * self.page as u64
    }

    pub fn open_regions(&self) -> u64 {
        self.open_regions
    }

    /// Length of the region table (never above `max_regions`).
    #[cfg(test)]
    pub fn region_table_len(&self) -> usize {
        self.regions.len()
    }

    fn head(&mut self, l: List) -> &mut u32 {
        match l {
            List::Hot => &mut self.hot,
            List::Cold => &mut self.cold,
            List::Avail(r, c) => &mut self.regions[r as usize].avail[c],
        }
    }

    fn push(&mut self, l: List, e: u32) {
        let old = *self.head(l);
        let x = &mut self.exts[e as usize];
        x.prev = NONE;
        x.next = old;
        if old != NONE {
            self.exts[old as usize].prev = e;
        }
        *self.head(l) = e;
    }

    fn remove(&mut self, l: List, e: u32) {
        let (p, n) = (self.exts[e as usize].prev, self.exts[e as usize].next);
        if p != NONE {
            self.exts[p as usize].next = n;
        } else {
            *self.head(l) = n;
        }
        if n != NONE {
            self.exts[n as usize].prev = p;
        }
    }

    // Regions.

    /// A new region: its slot and an id that is never used again, or `None`
    /// when the table is full (the caller uses region 0).
    pub fn open_region(&mut self) -> Option<(u32, u64)> {
        let slot = match self.spare_regions.pop() {
            Some(s) => s,
            None if self.regions.len() < self.max_regions => {
                self.regions.push(Region {
                    id: 0,
                    live: false,
                    avail: [NONE; CLASSES],
                });
                (self.regions.len() - 1) as u32
            }
            None => return None,
        };
        let id = self.next_id;
        self.next_id += 1;
        let r = &mut self.regions[slot as usize];
        r.id = id;
        r.live = true;
        self.open_regions += 1;
        Some((slot, id))
    }

    /// Closes a region once. Its extents with a free slot go to region 0; the
    /// full ones follow when a slot is freed (`owner_of`).
    pub fn close_region(&mut self, slot: u32, id: u64) {
        let s = slot as usize;
        if s == 0 || s >= self.regions.len() || !self.regions[s].live || self.regions[s].id != id {
            return;
        }
        for c in 0..CLASSES {
            while self.regions[s].avail[c] != NONE {
                let e = self.regions[s].avail[c];
                self.remove(List::Avail(slot, c), e);
                self.adopt(e, c);
            }
        }
        let r = &mut self.regions[s];
        r.live = false;
        r.id = 0;
        self.spare_regions.push(slot);
        self.open_regions -= 1;
    }

    /// Region 0 takes extent `e` of class `c`, which is in no list.
    fn adopt(&mut self, e: u32, c: usize) {
        let x = &mut self.exts[e as usize];
        x.owner = 0;
        x.owner_id = 0;
        self.push(List::Avail(0, c), e);
    }

    /// The region slot that owns `e`, or 0 when its region has closed.
    fn owner_of(&self, e: u32) -> u32 {
        let x = &self.exts[e as usize];
        let r = &self.regions[x.owner as usize];
        if r.live && r.id == x.owner_id {
            x.owner
        } else {
            0
        }
    }

    // Slots.

    fn fbit(&self, e: u32, slot: usize) -> bool {
        self.free[e as usize * self.free_words + slot / 64] >> (slot % 64) & 1 == 1
    }

    fn set_fbit(&mut self, e: u32, slot: usize, on: bool) {
        let w = &mut self.free[e as usize * self.free_words + slot / 64];
        if on {
            *w |= 1 << (slot % 64);
        } else {
            *w &= !(1 << (slot % 64));
        }
    }

    fn rbit(&self, e: u32, page: usize) -> bool {
        self.rel[e as usize * self.rel_words + page / 64] >> (page % 64) & 1 == 1
    }

    fn set_rbit(&mut self, e: u32, page: usize, on: bool) {
        let w = &mut self.rel[e as usize * self.rel_words + page / 64];
        if on {
            *w |= 1 << (page % 64);
        } else {
            *w &= !(1 << (page % 64));
        }
    }

    /// The offset of a free slot of `class` for `region`, marked in use; the
    /// region is region 0 when `region` is not open. `None` when the range
    /// is exhausted.
    pub fn alloc(&mut self, class: usize, region: u32) -> Option<usize> {
        let r = match self.regions.get(region as usize) {
            Some(r) if r.live => region,
            _ => 0,
        };
        let mut e = self.regions[r as usize].avail[class];
        if e == NONE {
            e = self.take_extent(class, r)?;
        }
        let words = self.free_words;
        let base = e as usize * words;
        let slot = (0..words)
            .find(|&w| self.free[base + w] != 0)
            .map(|w| w * 64 + self.free[base + w].trailing_zeros() as usize)?;
        self.set_fbit(e, slot, false);
        self.exts[e as usize].free -= 1;
        if self.exts[e as usize].free == 0 {
            self.remove(List::Avail(r, class), e);
        }
        let stride = self.strides[class];
        let (from, to) = (slot * stride, slot * stride + stride - 1);
        for p in from / self.page..=to / self.page {
            if self.rbit(e, p) {
                self.set_rbit(e, p, false);
                self.resident_pages += 1;
            }
        }
        self.in_use += stride as u64;
        Some(((e as usize) << self.shift) | from)
    }

    /// An extent for `region`, from the hot pool, the cold pool or the range.
    fn take_extent(&mut self, class: usize, region: u32) -> Option<u32> {
        let e = if self.hot != NONE {
            let e = self.hot;
            self.remove(List::Hot, e);
            e
        } else if self.cold != NONE {
            let e = self.cold;
            self.remove(List::Cold, e);
            e
        } else {
            if self.exts.len() >= self.max_ext {
                return None;
            }
            let e = self.exts.len() as u32;
            self.exts.push(Ext {
                class: 0,
                state: State::Cold,
                owner: 0,
                owner_id: 0,
                free: 0,
                claimed: 0,
                prev: NONE,
                next: NONE,
            });
            self.free.resize(self.free.len() + self.free_words, 0);
            self.rel.resize(self.rel.len() + self.rel_words, u64::MAX);
            e
        };
        let id = self.regions[region as usize].id;
        let slots = self.slots[class];
        let x = &mut self.exts[e as usize];
        *x = Ext {
            class: class as u8,
            state: State::Owned,
            owner: region,
            owner_id: id,
            free: slots as u32,
            claimed: 0,
            prev: NONE,
            next: NONE,
        };
        for w in 0..self.free_words {
            let ones = slots.saturating_sub(w * 64).min(64);
            self.free[e as usize * self.free_words + w] = if ones == 64 {
                u64::MAX
            } else {
                (1u64 << ones) - 1
            };
        }
        self.push(List::Avail(region, class), e);
        Some(e)
    }

    /// Frees the slot at `off`, which `alloc` returned.
    pub fn free(&mut self, off: usize) {
        let e = (off >> self.shift) as u32;
        debug_assert_eq!(self.exts[e as usize].state, State::Owned);
        let class = self.exts[e as usize].class as usize;
        let slot = (off & (self.extent_bytes() - 1)) / self.strides[class];
        self.set_fbit(e, slot, true);
        self.in_use -= self.strides[class] as u64;
        let r = self.owner_of(e);
        let x = &mut self.exts[e as usize];
        x.free += 1;
        let (free, claimed) = (x.free, x.claimed);
        let was_listed = free > 1;
        if r != x.owner {
            x.owner = 0;
            x.owner_id = 0;
        }
        if free as usize == self.slots[class] && claimed == 0 {
            if was_listed {
                self.remove(List::Avail(self.exts[e as usize].owner, class), e);
            }
            self.pool(e);
        } else if !was_listed {
            self.push(List::Avail(self.exts[e as usize].owner, class), e);
        }
    }

    /// Puts a fully free extent in the pool its residency says.
    fn pool(&mut self, e: u32) {
        let hot = (0..self.pages_per_ext).any(|p| !self.rbit(e, p));
        self.exts[e as usize].state = if hot { State::Hot } else { State::Cold };
        self.push(if hot { List::Hot } else { List::Cold }, e);
    }

    // Trim.

    /// Takes runs of free slots, and fully free extents, whose OS pages are
    /// resident, from at most `scan` extents after the cursor, until
    /// `budget` pages are claimed or `out` is full. Returns the pages
    /// claimed. The claimed slots are invisible to `alloc`.
    pub fn claim(&mut self, out: &mut Vec<Claim>, budget: usize, scan: usize) -> usize {
        let mut left = budget;
        let mut seen = 0;
        self.scanned = 0;
        while seen < scan && left > 0 && out.len() < MAX_CLAIMS && !self.exts.is_empty() {
            if self.cursor >= self.exts.len() {
                self.cursor = 0;
            }
            let (pages, done) = self.claim_ext(self.cursor as u32, out, left);
            left -= pages.min(left);
            // An extent that the budget or `out` stopped half way is the next
            // start.
            if !done {
                break;
            }
            self.cursor += 1;
            seen += 1;
            self.scanned = seen;
        }
        budget - left
    }

    /// Pages claimed in extent `e`, and whether all of it was looked at.
    fn claim_ext(&mut self, e: u32, out: &mut Vec<Claim>, budget: usize) -> (usize, bool) {
        let x = self.exts[e as usize];
        match x.state {
            State::Hot => {
                let (first, end, n) = self.resident_span(e, 0, self.pages_per_ext);
                self.remove(List::Hot, e);
                if n == 0 {
                    self.exts[e as usize].state = State::Cold;
                    self.push(List::Cold, e);
                    return (0, true);
                }
                self.exts[e as usize].state = State::Trimming;
                out.push(Claim {
                    ext: e,
                    first_slot: 0,
                    count: 0,
                    first_page: first as u32,
                    end_page: end as u32,
                    whole: true,
                    released: false,
                });
                (n, true)
            }
            State::Owned if x.free > 0 => self.claim_runs(e, out, budget),
            _ => (0, true),
        }
    }

    fn claim_runs(&mut self, e: u32, out: &mut Vec<Claim>, budget: usize) -> (usize, bool) {
        let class = self.exts[e as usize].class as usize;
        let (slots, stride) = (self.slots[class], self.strides[class]);
        let mut claimed_pages = 0;
        let mut s = 0;
        while s < slots {
            if out.len() >= MAX_CLAIMS || claimed_pages >= budget {
                return (claimed_pages, false);
            }
            if !self.fbit(e, s) {
                s += 1;
                continue;
            }
            let mut n = 1;
            while s + n < slots && self.fbit(e, s + n) {
                n += 1;
            }
            // Whole OS pages inside the run; the last slot also owns the
            // tail of the extent.
            let end_byte = if s + n == slots {
                self.used_bytes[class]
            } else {
                (s + n) * stride
            };
            let (from, to) = ((s * stride).div_ceil(self.page), end_byte / self.page);
            let (first, end, pages) = self.resident_span(e, from, to);
            if pages > 0 {
                for i in s..s + n {
                    self.set_fbit(e, i, false);
                }
                let owner = self.exts[e as usize].owner;
                let x = &mut self.exts[e as usize];
                x.free -= n as u32;
                x.claimed += n as u32;
                if x.free == 0 {
                    self.remove(List::Avail(owner, class), e);
                }
                out.push(Claim {
                    ext: e,
                    first_slot: s as u32,
                    count: n as u32,
                    first_page: first as u32,
                    end_page: end as u32,
                    whole: false,
                    released: false,
                });
                claimed_pages += pages;
            }
            s += n;
        }
        (claimed_pages, true)
    }

    /// The first and end page of the resident pages in `from..to` and how
    /// many there are.
    fn resident_span(&self, e: u32, from: usize, to: usize) -> (usize, usize, usize) {
        let mut first = to;
        let mut end = from;
        let mut n = 0;
        for p in from..to.max(from) {
            if !self.rbit(e, p) {
                first = first.min(p);
                end = p + 1;
                n += 1;
            }
        }
        (first, end, n)
    }

    /// Ends a trim: the pages of the claims marked `released` are not resident
    /// any more, and every claimed slot is free again. Returns the pages
    /// released.
    pub fn finish(&mut self, claims: &[Claim]) -> usize {
        let mut total = 0;
        for c in claims {
            let e = c.ext;
            if c.released {
                for p in c.first_page..c.end_page {
                    if !self.rbit(e, p as usize) {
                        self.set_rbit(e, p as usize, true);
                        self.resident_pages -= 1;
                        total += 1;
                    }
                }
            }
            if c.whole {
                self.pool(e);
                continue;
            }
            let class = self.exts[e as usize].class as usize;
            for i in c.first_slot..c.first_slot + c.count {
                self.set_fbit(e, i as usize, true);
            }
            let r = self.owner_of(e);
            let x = &mut self.exts[e as usize];
            let was_listed = x.free > 0;
            x.free += c.count;
            x.claimed -= c.count;
            if r != x.owner {
                x.owner = 0;
                x.owner_id = 0;
            }
            let (free, claimed, owner) = (x.free, x.claimed, x.owner);
            if free as usize == self.slots[class] && claimed == 0 {
                if was_listed {
                    self.remove(List::Avail(owner, class), e);
                }
                self.pool(e);
            } else if !was_listed {
                self.push(List::Avail(owner, class), e);
            }
        }
        total
    }

    /// Whether every extent is in exactly one place and the counters match the
    /// bits. Tests call it after each step.
    #[cfg(test)]
    pub fn check(&self) {
        let mut listed = vec![0u32; self.exts.len()];
        let walk = |mut e: u32, listed: &mut Vec<u32>| {
            while e != NONE {
                listed[e as usize] += 1;
                e = self.exts[e as usize].next;
            }
        };
        walk(self.hot, &mut listed);
        walk(self.cold, &mut listed);
        for r in &self.regions {
            for c in 0..CLASSES {
                walk(r.avail[c], &mut listed);
            }
        }
        let (mut used, mut resident) = (0u64, 0u64);
        for (i, x) in self.exts.iter().enumerate() {
            let e = i as u32;
            let slots = self.slots[x.class as usize];
            let free = (0..slots).filter(|&s| self.fbit(e, s)).count() as u32;
            assert_eq!(free, x.free, "free count of extent {i}");
            let in_list = match x.state {
                State::Owned => x.free > 0,
                State::Hot | State::Cold => true,
                State::Trimming => false,
            };
            assert_eq!(listed[i], in_list as u32, "lists of extent {i}");
            if x.state != State::Owned {
                assert_eq!((x.free as usize, x.claimed), (slots, 0));
            }
            if x.state == State::Owned {
                assert!((x.free + x.claimed) as usize <= slots, "extent {i} counts");
                used += ((slots - (x.free + x.claimed) as usize) * self.strides[x.class as usize])
                    as u64;
            }
            if x.state == State::Cold {
                assert!((0..self.pages_per_ext).all(|p| self.rbit(e, p)));
            }
            resident += (0..self.pages_per_ext)
                .filter(|&p| !self.rbit(e, p))
                .count() as u64;
        }
        assert_eq!(used, self.in_use);
        assert_eq!(resident, self.resident_pages);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// The class of a 4096-byte page.
    const PAGE4K: usize = 3;
    const OS: usize = 4096;

    fn strides() -> [usize; CLASSES] {
        std::array::from_fn(|i| (512 << i) + 272 + 8)
    }

    fn book(extent_kib: usize, reserved_mib: usize, regions: usize) -> Book {
        Book::new(Geometry {
            page: OS,
            reserved: reserved_mib << 20,
            extent_bytes: extent_kib << 10,
            strides: strides(),
            max_regions: regions,
        })
        .unwrap()
    }

    fn ext_of(b: &Book, off: usize) -> usize {
        off >> b.extent_bytes().trailing_zeros()
    }

    fn rng(seed: u64) -> impl FnMut() -> u64 {
        let mut x = seed;
        move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        }
    }

    /// Claims everything claimable and finishes with `released`.
    fn trim_all(b: &mut Book, released: bool) -> usize {
        let mut total = 0;
        for _ in 0..1000 {
            let mut claims = Vec::with_capacity(MAX_CLAIMS);
            let n = b.claim(&mut claims, usize::MAX / 2, 64);
            for c in claims.iter_mut() {
                c.released = released;
            }
            total += b.finish(&claims);
            b.check();
            if n == 0 {
                break;
            }
        }
        total
    }

    #[test]
    fn every_class_holds_a_slot_inside_its_extent() {
        let mut b = book(64, 64, 8);
        let g = b.extent_bytes();
        for class in 0..CLASSES {
            assert!(b.slots(class) >= 1, "class {class}");
            assert!(b.slots(class) * b.stride(class) <= g, "class {class}");
            let off = b.alloc(class, 0).expect("a slot");
            assert!((off & (g - 1)) + b.stride(class) <= g, "class {class}");
        }
        // A 64 KiB page takes more than a 64 KiB extent target: one slot.
        assert_eq!(b.slots(7), 1);
        assert_eq!(b.stride(7), 65536 + 272 + 8);
        b.check();
    }

    #[test]
    fn a_random_run_keeps_the_books_and_never_overlaps_slots() {
        let mut b = book(64, if cfg!(miri) { 64 } else { 512 }, 16);
        let mut next = rng(7);
        let regions: Vec<_> = (0..4).map(|_| b.open_region().unwrap()).collect();
        let mut live: HashMap<usize, usize> = HashMap::new();
        let steps: usize = if cfg!(miri) { 600 } else { 20_000 };
        for i in 0..steps {
            let r = next();
            if !r.is_multiple_of(3) || live.is_empty() {
                let class = (r >> 8) as usize % CLASSES;
                let region = regions[(r >> 16) as usize % regions.len()].0;
                let off = b.alloc(class, region).expect("room");
                assert!(live.insert(off, class).is_none(), "slot handed out twice");
            } else {
                let key = *live.keys().nth((r >> 8) as usize % live.len()).unwrap();
                b.free(key);
                live.remove(&key);
            }
            if r.is_multiple_of(11) {
                let mut claims = Vec::with_capacity(MAX_CLAIMS);
                b.claim(&mut claims, 64, 8);
                // A claim never covers a slot in use.
                for c in &claims {
                    let g = b.extent_bytes();
                    let (lo, hi) = (
                        ((c.ext as usize) << g.trailing_zeros()) + c.first_page as usize * OS,
                        ((c.ext as usize) << g.trailing_zeros()) + c.end_page as usize * OS,
                    );
                    for (&off, &class) in &live {
                        assert!(
                            off + b.stride(class) <= lo || off >= hi,
                            "a claim covers a live slot"
                        );
                    }
                }
                for c in claims.iter_mut() {
                    c.released = r.is_multiple_of(2);
                }
                b.finish(&claims);
            }
            if i.is_multiple_of(97) {
                b.check();
            }
        }
        let used: usize = live.values().map(|&c| b.stride(c)).sum();
        assert_eq!(b.in_use() as usize, used);
        b.check();
        let mut spans: Vec<_> = live.iter().map(|(&o, &c)| (o, o + b.stride(c))).collect();
        spans.sort();
        assert!(spans.windows(2).all(|w| w[0].1 <= w[1].0));
    }

    #[test]
    fn a_region_fills_extents_of_its_own() {
        let mut b = book(64, 64, 8);
        let (a, _) = b.open_region().unwrap();
        let (c, _) = b.open_region().unwrap();
        let (mut ea, mut ec) = (Vec::new(), Vec::new());
        for _ in 0..100 {
            ea.push(b.alloc(PAGE4K, a).unwrap());
            ec.push(b.alloc(PAGE4K, c).unwrap());
        }
        let set = |v: &[usize]| {
            v.iter()
                .map(|&o| ext_of(&b, o))
                .collect::<std::collections::BTreeSet<_>>()
        };
        assert!(set(&ea).is_disjoint(&set(&ec)));
        let freed = set(&ea);
        for &o in &ea {
            b.free(o);
        }
        b.check();
        // The extents of the region that freed everything are in the pool:
        // a new region takes one of them and carves nothing.
        let before = b.extents();
        let (d, _) = b.open_region().unwrap();
        let o = b.alloc(PAGE4K, d).unwrap();
        assert!(freed.contains(&ext_of(&b, o)));
        assert_eq!(b.extents(), before);
    }

    #[test]
    fn the_region_table_does_not_grow_over_many_open_and_close() {
        let mut b = book(64, 64, 64);
        let mut last = 0;
        let rounds = if cfg!(miri) { 300 } else { 100_000 };
        for _ in 0..rounds {
            let (slot, id) = b.open_region().expect("a free region slot");
            assert!(id > last, "ids are never used again");
            last = id;
            let offs: Vec<_> = (0..3).map(|_| b.alloc(PAGE4K, slot).unwrap()).collect();
            for o in offs {
                b.free(o);
            }
            b.close_region(slot, id);
            b.close_region(slot, id);
        }
        // Region 0 and the one slot in use over and over.
        assert_eq!(b.region_table_len(), 2);
        assert_eq!(b.open_regions(), 0);
        assert!(b.extents() <= 2);
        b.check();
    }

    #[test]
    fn a_full_region_table_gives_region_zero() {
        let mut b = book(64, 64, 3);
        assert!(b.open_region().is_some());
        let (slot, id) = b.open_region().unwrap();
        assert!(b.open_region().is_none());
        // A region that is closed, or out of the table, is region 0.
        b.close_region(slot, id);
        let o = b.alloc(PAGE4K, slot).unwrap();
        let o2 = b.alloc(PAGE4K, 999).unwrap();
        assert_eq!(ext_of(&b, o), ext_of(&b, o2));
        b.check();
    }

    #[test]
    fn a_closed_regions_extents_pass_to_region_zero() {
        let mut b = book(64, 64, 8);
        let (r, id) = b.open_region().unwrap();
        // One extent full (14 slots), one partly used.
        let offs: Vec<_> = (0..16).map(|_| b.alloc(PAGE4K, r).unwrap()).collect();
        let (full, partial) = (ext_of(&b, offs[0]), ext_of(&b, offs[15]));
        assert_ne!(full, partial);
        b.close_region(r, id);
        b.check();
        // The partial extent is region 0's now.
        let o = b.alloc(PAGE4K, 0).unwrap();
        assert_eq!(ext_of(&b, o), partial);
        // The slot is reused with a new id; the full extent, freed later,
        // must not go to the new region.
        let (r2, id2) = b.open_region().unwrap();
        assert_eq!(r2, r);
        assert_ne!(id2, id);
        b.free(offs[0]);
        b.check();
        let o2 = b.alloc(PAGE4K, r2).unwrap();
        assert_ne!(
            ext_of(&b, o2),
            full,
            "a closed region's extent went to its successor"
        );
        // It is region 0's.
        let mut got = Vec::new();
        for _ in 0..20 {
            let o = b.alloc(PAGE4K, 0).unwrap();
            got.push(ext_of(&b, o));
        }
        assert!(got.contains(&full));
    }

    #[test]
    fn a_claim_takes_whole_pages_of_free_runs_only() {
        let mut b = book(64, 64, 8);
        let (r, _) = b.open_region().unwrap();
        let offs: Vec<_> = (0..14).map(|_| b.alloc(PAGE4K, r).unwrap()).collect();
        // Free slots 2..10, keep 0, 1 and 10..14.
        for &o in &offs[2..10] {
            b.free(o);
        }
        let mut claims = Vec::with_capacity(MAX_CLAIMS);
        let pages = b.claim(&mut claims, 1000, 64);
        assert_eq!(claims.len(), 1);
        let c = claims[0];
        assert_eq!((c.first_slot, c.count), (2, 8));
        let stride = b.stride(PAGE4K);
        let g = b.extent_bytes();
        let base = ext_of(&b, offs[0]) * g;
        // Inside the run, whole pages only.
        assert!(base + c.first_page as usize * OS >= offs[2]);
        assert!(base + c.end_page as usize * OS <= offs[9] + stride);
        assert_eq!(pages, (c.end_page - c.first_page) as usize);
        b.check();
        // The claimed slots are not handed out.
        let o = b.alloc(PAGE4K, r).unwrap();
        assert!(!offs[2..10].contains(&o));
        let mid = b.resident_bytes();
        claims[0].released = true;
        assert_eq!(b.finish(&claims), pages);
        assert_eq!(b.resident_bytes(), mid - (pages * OS) as u64);
        b.check();
        // The slots are free again; allocating makes their pages resident.
        let back = b.alloc(PAGE4K, r).unwrap();
        assert!(offs[2..10].contains(&back));
        assert!(b.resident_bytes() > mid - (pages * OS) as u64);
        b.check();
    }

    #[test]
    fn an_extent_with_a_claim_stays_out_of_the_pool_until_it_ends() {
        let mut b = book(64, 64, 8);
        let (r, _) = b.open_region().unwrap();
        let offs: Vec<_> = (0..14).map(|_| b.alloc(PAGE4K, r).unwrap()).collect();
        for &o in &offs[..5] {
            b.free(o);
        }
        let mut claims = Vec::with_capacity(MAX_CLAIMS);
        b.claim(&mut claims, 1000, 64);
        assert_eq!(claims.len(), 1);
        // The rest frees while the trim holds its run: not all free yet.
        for &o in &offs[5..] {
            b.free(o);
        }
        b.check();
        assert_eq!(b.hot, NONE, "an extent with a claim went to the pool");
        claims[0].released = true;
        b.finish(&claims);
        b.check();
        assert_ne!(
            b.hot, NONE,
            "the extent did not return when the claim ended"
        );
        assert_eq!(b.exts[b.hot as usize].next, NONE);
    }

    #[test]
    fn free_extents_are_released_whole_and_end_cold() {
        let mut b = book(64, 64, 8);
        let (r, _) = b.open_region().unwrap();
        let offs: Vec<_> = (0..56).map(|_| b.alloc(PAGE4K, r).unwrap()).collect();
        assert!(b.resident_bytes() > 0);
        for &o in &offs {
            b.free(o);
        }
        assert!(trim_all(&mut b, true) > 0);
        assert_eq!(b.resident_bytes(), 0);
        assert_eq!(b.hot, NONE);
        assert_ne!(b.cold, NONE);
        // A failed release leaves the pages marked resident, and the extent
        // in the pool.
        let o = b.alloc(PAGE4K, r).unwrap();
        b.free(o);
        let kept = b.resident_bytes();
        assert_eq!(trim_all(&mut b, false), 0);
        assert_eq!(b.resident_bytes(), kept);
        b.check();
    }

    #[test]
    fn a_claim_stops_at_its_budget_and_the_next_one_resumes() {
        let mut b = book(64, 64, 8);
        let (r, _) = b.open_region().unwrap();
        let offs: Vec<_> = (0..14 * 6).map(|_| b.alloc(PAGE4K, r).unwrap()).collect();
        for &o in &offs {
            b.free(o);
        }
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..6 {
            let mut claims = Vec::with_capacity(MAX_CLAIMS);
            b.claim(&mut claims, 1, 64);
            assert_eq!(claims.len(), 1, "a budget of one page takes one extent");
            assert!(seen.insert(claims[0].ext), "an extent was claimed twice");
            claims[0].released = true;
            b.finish(&claims);
        }
        assert_eq!(b.resident_bytes(), 0);
    }

    #[test]
    fn the_range_runs_out_and_a_freed_slot_serves_again() {
        let mut b = Book::new(Geometry {
            page: OS,
            reserved: 128 << 10,
            extent_bytes: 64 << 10,
            strides: strides(),
            max_regions: 4,
        })
        .unwrap();
        // 64 KiB pages need 128 KiB extents, so the range is one extent.
        assert_eq!(b.range_bytes(), b.extent_bytes());
        let n = b.slots(PAGE4K);
        let offs: Vec<_> = (0..n).map(|_| b.alloc(PAGE4K, 0).unwrap()).collect();
        assert!(b.alloc(PAGE4K, 0).is_none());
        assert!(b.alloc(0, 0).is_none(), "the one extent is a 4 KiB one");
        b.free(offs[3]);
        assert_eq!(b.alloc(PAGE4K, 0), Some(offs[3]));
        b.check();
    }
}
