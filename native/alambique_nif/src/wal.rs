//! Reading committed frames from a SQLite WAL file.
//!
//! A WAL starts with a 32-byte header followed by frames of a 24-byte header
//! and one page. Frame checksums chain from the WAL header, and a frame only
//! counts when its salts equal the header's. A transaction is committed by a
//! frame whose "database size" field is non-zero. See
//! https://www.sqlite.org/fileformat2.html#walformat.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::ErrorKind;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::bytes::Reader;
use crate::error::{invalid, Error, Result};

const WAL_HEADER_LEN: usize = 32;
pub const WAL_HEADER_SIZE: u64 = WAL_HEADER_LEN as u64;
const FRAME_HEADER_SIZE: usize = 24;
const MAGIC_LE: u32 = 0x377f0682;
const MAGIC_BE: u32 = 0x377f0683;

/// Where the next unread frame is. `offset == 0` means "not positioned yet":
/// the next scan starts at the first frame of whatever WAL it finds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalPos {
    pub offset: u64,
    pub salt1: u32,
    pub salt2: u32,
    pub cksum1: u32,
    pub cksum2: u32,
}

#[derive(Debug)]
pub struct Scan {
    /// Position after the last committed frame (or the start, if none).
    pub pos: WalPos,
    /// Offset of the first frame examined.
    pub first_offset: u64,
    /// Database size in pages after the last committed transaction.
    pub commit: Option<u32>,
    /// Latest committed version of every page written, pages past `commit`
    /// removed.
    pub pages: BTreeMap<u32, Vec<u8>>,
}

struct WalHeader {
    big_endian: bool,
    page_size: u32,
    salt1: u32,
    salt2: u32,
    cksum1: u32,
    cksum2: u32,
}

/// SQLite's WAL checksum over `data` (a multiple of 8 bytes).
fn checksum(big_endian: bool, data: &[u8], mut s0: u32, mut s1: u32) -> (u32, u32) {
    for &[a, b] in data.as_chunks::<4>().0.as_chunks::<2>().0 {
        let (a, b) = if big_endian {
            (u32::from_be_bytes(a), u32::from_be_bytes(b))
        } else {
            (u32::from_le_bytes(a), u32::from_le_bytes(b))
        };
        s0 = s0.wrapping_add(a).wrapping_add(s1);
        s1 = s1.wrapping_add(b).wrapping_add(s0);
    }
    (s0, s1)
}

fn read_header(file: &File) -> Result<Option<WalHeader>> {
    let mut b = [0u8; WAL_HEADER_LEN];
    match file.read_exact_at(&mut b, 0) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let Some((front, _)) = b.split_first_chunk::<24>() else {
        return Ok(None);
    };
    let mut r = Reader::new(&b);
    let (Some(magic), Some(_version), Some(page_size), Some(_seq)) =
        (r.u32(), r.u32(), r.u32(), r.u32())
    else {
        return Ok(None);
    };
    let (Some(salt1), Some(salt2), Some(sum1), Some(sum2)) = (r.u32(), r.u32(), r.u32(), r.u32())
    else {
        return Ok(None);
    };
    if magic != MAGIC_LE && magic != MAGIC_BE {
        return Ok(None);
    }
    let big_endian = magic == MAGIC_BE;
    let (c1, c2) = checksum(big_endian, front, 0, 0);
    if (c1, c2) != (sum1, sum2) {
        return Ok(None);
    }
    Ok(Some(WalHeader {
        big_endian,
        page_size,
        salt1,
        salt2,
        cksum1: c1,
        cksum2: c2,
    }))
}

/// The page size and the two salts of the WAL header at `path`, `None` when
/// there is no valid WAL. The WAL file carries no POSIX lock of SQLite's, so
/// opening it by path is safe.
pub fn read_salts(path: &Path) -> Result<Option<(u32, u32, u32)>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    Ok(read_header(&file)?.map(|h| (h.page_size, h.salt1, h.salt2)))
}

/// How many frames of the WAL at `path` are committed, `0` without a valid WAL.
pub fn committed_frames(path: &Path) -> Result<u64> {
    let Some((page_size, ..)) = read_salts(path)? else {
        return Ok(0);
    };
    let end = scan(path, page_size, None, None)?.pos.offset;
    Ok(end.saturating_sub(WAL_HEADER_SIZE) / (FRAME_HEADER_SIZE as u64 + u64::from(page_size)))
}

/// One frame whose salts and checksum chain are right.
struct Frame<'a> {
    pgno: u32,
    db_size: u32,
    /// The checksum chain after the frame.
    sums: (u32, u32),
    page: &'a [u8],
}

impl<'a> Frame<'a> {
    /// The frame in `buf` (header and page), `None` when it is not the next
    /// frame of the WAL of `hdr` after the checksums `prev`.
    fn parse(buf: &'a [u8], hdr: &WalHeader, prev: (u32, u32)) -> Option<Frame<'a>> {
        let (head, page) = buf.split_first_chunk::<FRAME_HEADER_SIZE>()?;
        let mut r = Reader::new(head);
        let front = r.array::<8>()?;
        let (salt1, salt2, sum1, sum2) = (r.u32()?, r.u32()?, r.u32()?, r.u32()?);
        let mut f = Reader::new(&front);
        let (pgno, db_size) = (f.u32()?, f.u32()?);
        if salt1 != hdr.salt1 || salt2 != hdr.salt2 {
            return None;
        }
        let (c0, c1) = checksum(hdr.big_endian, &front, prev.0, prev.1);
        let sums = checksum(hdr.big_endian, page, c0, c1);
        (sums == (sum1, sum2)).then_some(Frame {
            pgno,
            db_size,
            sums,
            page,
        })
    }
}

/// Where scanning starts: `from` when it is in this WAL generation, the first
/// frame otherwise.
fn resume_pos(hdr: &WalHeader, from: Option<WalPos>) -> WalPos {
    from.filter(|p| p.offset >= WAL_HEADER_SIZE && p.salt1 == hdr.salt1 && p.salt2 == hdr.salt2)
        .unwrap_or(WalPos {
            offset: WAL_HEADER_SIZE,
            salt1: hdr.salt1,
            salt2: hdr.salt2,
            cksum1: hdr.cksum1,
            cksum2: hdr.cksum2,
        })
}

/// The offset a limit allows; a limit from another WAL generation (different
/// salts) allows nothing.
fn limit_offset(hdr: &WalHeader, limit: Option<WalPos>) -> Option<u64> {
    limit.map(|l| {
        if l.salt1 == hdr.salt1 && l.salt2 == hdr.salt2 {
            l.offset
        } else {
            0
        }
    })
}

/// The committed frames from `start` on, of frames of `frame_len` bytes,
/// stopping at the first that is not the next of the WAL or at `limit`.
fn read_frames(
    file: &File,
    hdr: &WalHeader,
    start: WalPos,
    limit: Option<u64>,
    frame_len: usize,
) -> Result<Scan> {
    let mut buf = vec![0u8; frame_len];
    let mut offset = start.offset;
    let (mut s0, mut s1) = (start.cksum1, start.cksum2);
    let mut pending: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    let mut committed: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    let mut commit = None;
    let mut pos = start;

    loop {
        if limit.is_some_and(|l| offset >= l) {
            break;
        }
        match file.read_exact_at(&mut buf, offset) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let Some(frame) = Frame::parse(&buf, hdr, (s0, s1)) else {
            break;
        };
        (s0, s1) = frame.sums;
        let (pgno, db_size, page) = (frame.pgno, frame.db_size, frame.page);
        crate::stats::WAL_FRAMES_READ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        offset = offset
            .checked_add(frame_len as u64)
            .ok_or_else(|| Error::Invalid("wal offset overflows".into()))?;
        if pgno == 0 {
            break;
        }
        pending.insert(pgno, page.to_vec());
        if db_size != 0 {
            committed.append(&mut pending);
            commit = Some(db_size);
            pos = WalPos {
                offset,
                salt1: hdr.salt1,
                salt2: hdr.salt2,
                cksum1: s0,
                cksum2: s1,
            };
        }
    }

    if let Some(c) = commit {
        committed.retain(|pgno, _| *pgno <= c);
    }
    Ok(Scan {
        pos,
        first_offset: start.offset,
        commit,
        pages: committed,
    })
}

/// Scans committed frames.
///
/// With `from` set and its salts equal to the WAL header's, scanning resumes
/// at `from.offset`; otherwise (no position, or the WAL was restarted) it
/// begins at the first frame. `limit` is a position in the same WAL
/// generation: frames that start at or after its offset are ignored. A missing, empty or invalid WAL has
/// no frames.
pub fn scan(
    path: &Path,
    page_size: u32,
    from: Option<WalPos>,
    limit: Option<WalPos>,
) -> Result<Scan> {
    let none = |pos: WalPos| Scan {
        pos,
        first_offset: pos.offset,
        commit: None,
        pages: BTreeMap::new(),
    };
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(none(from.unwrap_or_default())),
        Err(e) => return Err(e.into()),
    };
    let Some(hdr) = read_header(&file)? else {
        return Ok(none(WalPos::default()));
    };
    if hdr.page_size != page_size {
        return invalid(format!(
            "wal page size {} does not match {page_size}",
            hdr.page_size
        ));
    }

    let start = resume_pos(&hdr, from);
    let limit = limit_offset(&hdr, limit);
    let frame_len = usize::try_from(page_size)
        .ok()
        .and_then(|p| p.checked_add(FRAME_HEADER_SIZE))
        .ok_or_else(|| Error::Invalid(format!("wal page size {page_size} too large")))?;
    read_frames(&file, &hdr, start, limit, frame_len)
}

/// Makes arbitrary bytes a valid WAL as far as the layout allows: magic,
/// page size, salts and the checksum chain, so that a fuzzer reaches the
/// frames instead of stopping at the first checksum.
#[cfg(any(test, feature = "fuzzing"))]
pub fn repair(b: &mut [u8]) {
    if b.len() < WAL_HEADER_SIZE as usize {
        return;
    }
    let big_endian = b[0] & 1 == 1;
    let magic = if big_endian { MAGIC_BE } else { MAGIC_LE };
    b[0..4].copy_from_slice(&magic.to_be_bytes());
    let mut page_size = crate::bytes::u32_at(b, 8).unwrap_or(0);
    if !crate::ltx::valid_page_size(page_size) {
        page_size = 512;
        b[8..12].copy_from_slice(&page_size.to_be_bytes());
    }
    let (c0, c1) = checksum(big_endian, &b[..24], 0, 0);
    b[24..28].copy_from_slice(&c0.to_be_bytes());
    b[28..32].copy_from_slice(&c1.to_be_bytes());
    let (mut s0, mut s1) = (c0, c1);
    let frame = FRAME_HEADER_SIZE + page_size as usize;
    let mut at = WAL_HEADER_SIZE as usize;
    while at + frame <= b.len() {
        b.copy_within(16..24, at + 8);
        let (x0, x1) = checksum(big_endian, &b[at..at + 8], s0, s1);
        (s0, s1) = checksum(big_endian, &b[at + FRAME_HEADER_SIZE..at + frame], x0, x1);
        b[at + 16..at + 20].copy_from_slice(&s0.to_be_bytes());
        b[at + 20..at + 24].copy_from_slice(&s1.to_be_bytes());
        at += frame;
    }
}

#[cfg(any(test, feature = "fuzzing"))]
pub mod testing {
    //! Builds WAL files the way SQLite does, for tests and fuzz harnesses.
    use super::*;
    use std::io::Write;

    pub struct WalBuilder {
        pub bytes: Vec<u8>,
        page_size: usize,
        big_endian: bool,
        salt: (u32, u32),
        s: (u32, u32),
    }

    impl WalBuilder {
        pub fn new(page_size: u32, salt: (u32, u32), big_endian: bool) -> Self {
            let mut h = Vec::new();
            h.extend_from_slice(&(if big_endian { MAGIC_BE } else { MAGIC_LE }).to_be_bytes());
            h.extend_from_slice(&3007000u32.to_be_bytes());
            h.extend_from_slice(&page_size.to_be_bytes());
            h.extend_from_slice(&0u32.to_be_bytes());
            h.extend_from_slice(&salt.0.to_be_bytes());
            h.extend_from_slice(&salt.1.to_be_bytes());
            let s = checksum(big_endian, &h, 0, 0);
            h.extend_from_slice(&s.0.to_be_bytes());
            h.extend_from_slice(&s.1.to_be_bytes());
            WalBuilder {
                bytes: h,
                page_size: page_size as usize,
                big_endian,
                salt,
                s,
            }
        }

        /// Appends a frame; `commit` is the db size in pages for a commit frame.
        pub fn frame(&mut self, pgno: u32, commit: u32, data: &[u8]) -> &mut Self {
            let mut h = Vec::new();
            h.extend_from_slice(&pgno.to_be_bytes());
            h.extend_from_slice(&commit.to_be_bytes());
            h.extend_from_slice(&self.salt.0.to_be_bytes());
            h.extend_from_slice(&self.salt.1.to_be_bytes());
            let s = checksum(self.big_endian, &h[..8], self.s.0, self.s.1);
            self.s = checksum(self.big_endian, data, s.0, s.1);
            h.extend_from_slice(&self.s.0.to_be_bytes());
            h.extend_from_slice(&self.s.1.to_be_bytes());
            self.bytes.extend_from_slice(&h);
            self.bytes.extend_from_slice(data);
            self
        }

        /// Appends one transaction as SQLite would log it: the database ends
        /// with `size` pages (at least 1), the pages `writes` names (`(page,
        /// fill)`, pages wrapped into range) are rewritten, and every page
        /// the database grows by is written. `pages` is the database after
        /// the commit.
        pub fn transaction(
            &mut self,
            pages: &mut Vec<Vec<u8>>,
            size: u32,
            writes: &[(u32, u8)],
        ) -> &mut Self {
            let size = size.max(1) as usize;
            let old = pages.len();
            pages.resize(size, vec![0; self.page_size]);
            let mut touched = std::collections::BTreeMap::new();
            for pgno in old + 1..=size {
                touched.insert(pgno, 0);
            }
            for &(pgno, fill) in writes {
                touched.insert(pgno as usize % size + 1, fill);
            }
            if touched.is_empty() {
                // A commit needs a frame: log page 1 again, unchanged.
                touched.insert(1, pages[0][0]);
            }
            let last = *touched.keys().next_back().expect("not empty");
            for (&pgno, &fill) in &touched {
                pages[pgno - 1] = vec![fill; self.page_size];
                let commit = if pgno == last { size as u32 } else { 0 };
                self.frame(pgno as u32, commit, &pages[pgno - 1].clone());
            }
            self
        }

        /// Writes the file followed by the first `tail` bytes of a frame that
        /// has no commit, like a write SQLite has not finished. The builder
        /// does not keep the frame.
        pub fn write_with_tail(&mut self, path: &Path, tail: usize) {
            let (len, s) = (self.bytes.len(), self.s);
            self.frame(1, 0, &vec![0xee; self.page_size]);
            let end = len + tail.min(self.bytes.len() - len);
            File::create(path)
                .and_then(|mut f| f.write_all(&self.bytes[..end]))
                .unwrap();
            self.bytes.truncate(len);
            self.s = s;
        }

        pub fn write(&self, path: &Path) {
            let mut f = File::create(path).unwrap();
            f.write_all(&self.bytes).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::WalBuilder;
    use super::*;

    const PS: u32 = 512;

    fn pg(fill: u8) -> Vec<u8> {
        vec![fill; PS as usize]
    }

    fn wal(dir: &tempfile::TempDir, b: &WalBuilder) -> std::path::PathBuf {
        let p = dir.path().join("db-wal");
        b.write(&p);
        p
    }

    #[test]
    fn reads_committed_transactions_and_keeps_the_latest_page() {
        for be in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut b = WalBuilder::new(PS, (7, 9), be);
            b.frame(1, 0, &pg(1))
                .frame(2, 2, &pg(2))
                .frame(2, 0, &pg(3))
                .frame(3, 3, &pg(4));
            let s = scan(&wal(&dir, &b), PS, None, None).unwrap();
            assert_eq!(s.commit, Some(3));
            assert_eq!(s.pages.len(), 3);
            assert_eq!(s.pages[&2], pg(3));
            assert_eq!(s.pos.offset, b.bytes.len() as u64);
            assert_eq!(s.first_offset, 32);
        }
    }

    #[test]
    fn uncommitted_tail_frames_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 1, &pg(1));
        let committed_end = b.bytes.len() as u64;
        b.frame(1, 0, &pg(9)).frame(2, 0, &pg(9));
        let s = scan(&wal(&dir, &b), PS, None, None).unwrap();
        assert_eq!(s.commit, Some(1));
        assert_eq!(s.pages[&1], pg(1));
        assert_eq!(s.pos.offset, committed_end);
    }

    #[test]
    fn torn_last_frame_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 1, &pg(1));
        let end = b.bytes.len() as u64;
        b.frame(1, 1, &pg(2));
        let cut = b.bytes.len() - 100;
        b.bytes.truncate(cut);
        let s = scan(&wal(&dir, &b), PS, None, None).unwrap();
        assert_eq!(s.pages[&1], pg(1));
        assert_eq!(s.pos.offset, end);
    }

    #[test]
    fn corrupted_frame_stops_the_scan() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 1, &pg(1)).frame(2, 2, &pg(2));
        let n = b.bytes.len();
        b.bytes[n - 10] ^= 1;
        let s = scan(&wal(&dir, &b), PS, None, None).unwrap();
        assert_eq!(s.commit, Some(1));
        assert!(!s.pages.contains_key(&2));
    }

    #[test]
    fn stale_frames_with_old_salts_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let mut old = WalBuilder::new(PS, (1, 2), false);
        old.frame(1, 1, &pg(1))
            .frame(2, 2, &pg(2))
            .frame(3, 3, &pg(3));
        // A restarted WAL overwrites the front of the file and leaves old frames behind.
        let mut new = WalBuilder::new(PS, (2, 5), false);
        new.frame(1, 1, &pg(8));
        let mut bytes = old.bytes.clone();
        bytes[..new.bytes.len()].copy_from_slice(&new.bytes);
        let p = dir.path().join("db-wal");
        std::fs::write(&p, &bytes).unwrap();
        let s = scan(&p, PS, None, None).unwrap();
        assert_eq!(s.pages.len(), 1);
        assert_eq!(s.pages[&1], pg(8));
    }

    #[test]
    fn resumes_from_a_position_and_restarts_when_salts_change() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 1, &pg(1));
        let p = wal(&dir, &b);
        let first = scan(&p, PS, None, None).unwrap();
        b.frame(2, 2, &pg(2));
        b.write(&p);
        let second = scan(&p, PS, Some(first.pos), None).unwrap();
        assert_eq!(second.first_offset, first.pos.offset);
        assert_eq!(second.pages.keys().copied().collect::<Vec<_>>(), vec![2]);
        assert_eq!(second.commit, Some(2));

        let mut restarted = WalBuilder::new(PS, (2, 77), false);
        restarted.frame(1, 2, &pg(5));
        restarted.write(&p);
        let third = scan(&p, PS, Some(second.pos), None).unwrap();
        assert_eq!(third.first_offset, 32);
        assert_eq!(third.pages[&1], pg(5));
    }

    #[test]
    fn nothing_new_keeps_the_position() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 1, &pg(1));
        let p = wal(&dir, &b);
        let first = scan(&p, PS, None, None).unwrap();
        let again = scan(&p, PS, Some(first.pos), None).unwrap();
        assert_eq!(again.commit, None);
        assert_eq!(again.pos, first.pos);
    }

    #[test]
    fn pages_past_the_final_size_are_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 0, &pg(1))
            .frame(5, 5, &pg(5))
            .frame(1, 2, &pg(2));
        let s = scan(&wal(&dir, &b), PS, None, None).unwrap();
        assert_eq!(s.commit, Some(2));
        assert_eq!(s.pages.keys().copied().collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn limit_stops_before_later_frames() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 1, &pg(1));
        let limit = WalPos {
            offset: b.bytes.len() as u64,
            salt1: 1,
            salt2: 2,
            ..Default::default()
        };
        b.frame(1, 1, &pg(2));
        let p = wal(&dir, &b);
        let s = scan(&p, PS, None, Some(limit)).unwrap();
        assert_eq!(s.pages[&1], pg(1));
        let other = WalPos { salt1: 9, ..limit };
        assert_eq!(scan(&p, PS, None, Some(other)).unwrap().commit, None);
    }

    #[test]
    fn missing_empty_or_foreign_wal_has_no_frames() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("db-wal");
        assert_eq!(scan(&p, PS, None, None).unwrap().commit, None);
        std::fs::write(&p, b"").unwrap();
        assert_eq!(scan(&p, PS, None, None).unwrap().commit, None);
        std::fs::write(&p, vec![7u8; 100]).unwrap();
        assert_eq!(scan(&p, PS, None, None).unwrap().commit, None);
    }

    #[test]
    fn page_size_mismatch_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = WalBuilder::new(1024, (1, 2), false);
        b.frame(1, 1, &vec![0; 1024]);
        assert!(scan(&wal(&dir, &b), PS, None, None).is_err());
    }
}

#[cfg(test)]
mod props {
    use super::testing::WalBuilder;
    use super::*;
    use proptest::prelude::*;

    const PS: u32 = 512;

    type Tx = (u32, Vec<(u32, u8)>);

    fn txs() -> impl Strategy<Value = Vec<Tx>> {
        prop::collection::vec(
            (
                1u32..8,
                prop::collection::vec((0u32..16, any::<u8>()), 0..5),
            ),
            1..8,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// The scan returns the committed pages and nothing else, whatever
        /// follows the last commit: frames without a commit, or a frame cut
        /// short. A second scan from the first one's position sees the rest.
        #[test]
        #[cfg_attr(miri, ignore = "proptest is too slow under Miri")]
        fn scan_returns_exactly_the_committed_frames(
            txs in txs(),
            uncommitted in prop::collection::vec((1u32..9, any::<u8>()), 0..3),
            tail in 0usize..600,
            big_endian in any::<bool>(),
            split in any::<prop::sample::Index>(),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db-wal");
            let k = split.index(txs.len() + 1);
            let mut b = WalBuilder::new(PS, (3, 4), big_endian);
            let mut db = Vec::new();
            let mut at_split = None;
            for (i, (size, writes)) in txs.iter().enumerate() {
                if i == k {
                    b.write(&path);
                    at_split = Some(scan(&path, PS, None, None).unwrap());
                }
                b.transaction(&mut db, *size, writes);
            }
            if k == txs.len() {
                b.write(&path);
                at_split = Some(scan(&path, PS, None, None).unwrap());
            }
            let committed_end = b.bytes.len() as u64;
            for (pgno, fill) in &uncommitted {
                b.frame(*pgno, 0, &vec![*fill; PS as usize]);
            }
            b.write_with_tail(&path, tail);

            let full = scan(&path, PS, None, None).unwrap();
            let expected: BTreeMap<u32, Vec<u8>> = db
                .iter()
                .enumerate()
                .map(|(i, p)| (i as u32 + 1, p.clone()))
                .collect();
            prop_assert_eq!(&full.pages, &expected);
            prop_assert_eq!(full.commit, Some(db.len() as u32));
            prop_assert_eq!(full.pos.offset, committed_end);

            let first = at_split.unwrap();
            let rest = scan(&path, PS, Some(first.pos), None).unwrap();
            let mut overlay = first.pages;
            overlay.extend(rest.pages);
            if let Some(c) = rest.commit {
                overlay.retain(|p, _| *p <= c);
            }
            prop_assert_eq!(overlay, expected);
            prop_assert_eq!(rest.pos, full.pos);
        }
    }
}
