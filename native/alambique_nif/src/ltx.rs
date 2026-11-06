//! LTX version 3 encoder and decoder (github.com/superfly/ltx, see
//! docs/segment-format.md for the exact upstream commit).
//!
//! All fixed-width integers are big-endian. A file is: 100-byte header, page
//! frames (6-byte page header, 4-byte compressed size, LZ4 block), a zero page
//! header, the page index (varint triples, zero terminator, 8-byte size), and
//! a 16-byte trailer.
//!
//! The specification makes the header and trailer checksums zero in files
//! with the `NoChecksum` flag, so the files this crate writes set the flag and
//! keep the pre- and post-apply checksums in the reserved header bytes 80..96.
//! Files without the flag carry them in the header and trailer, as upstream
//! does for checksummed files.

use crate::crc::Crc64;
use crate::error::{invalid, Error, Result};

pub const MAGIC: &[u8; 4] = b"LTX1";
pub const HEADER_SIZE: usize = 100;
pub const PAGE_HEADER_SIZE: usize = 6;
pub const TRAILER_SIZE: usize = 16;
pub const CHECKSUM_FLAG: u64 = 1 << 63;
pub const HEADER_FLAG_NO_CHECKSUM: u32 = 1 << 1;
/// Offsets of the checksums in the reserved header bytes of `NoChecksum` files.
const RESERVED_PRE: usize = 80;
const RESERVED_POST: usize = 88;
const PAGE_FLAG_SIZE: u16 = 1;
const PENDING_BYTE: u64 = 0x4000_0000;

pub fn lock_pgno(page_size: u32) -> u32 {
    (PENDING_BYTE / page_size as u64) as u32 + 1
}

pub fn valid_page_size(ps: u32) -> bool {
    (512..=65536).contains(&ps) && ps.is_power_of_two()
}

/// Raw CRC-64/ISO of `be32(pgno) || data`, without the checksum flag.
pub fn page_crc(pgno: u32, data: &[u8]) -> u64 {
    let mut h = Crc64::default();
    h.update(&pgno.to_be_bytes());
    h.update(data);
    h.sum()
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Header {
    pub flags: u32,
    pub page_size: u32,
    pub commit: u32,
    pub min_txid: u64,
    pub max_txid: u64,
    pub timestamp: i64,
    pub pre_apply: u64,
    /// Database checksum after applying the file.
    pub post_apply: u64,
    pub wal_offset: i64,
    pub wal_size: i64,
    pub wal_salt1: u32,
    pub wal_salt2: u32,
    pub node_id: u64,
}

impl Header {
    pub fn is_snapshot(&self) -> bool {
        self.min_txid == 1
    }

    pub fn no_checksum(&self) -> bool {
        self.flags & HEADER_FLAG_NO_CHECKSUM != 0
    }

    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut b = [0u8; HEADER_SIZE];
        b[0..4].copy_from_slice(MAGIC);
        b[4..8].copy_from_slice(&self.flags.to_be_bytes());
        b[8..12].copy_from_slice(&self.page_size.to_be_bytes());
        b[12..16].copy_from_slice(&self.commit.to_be_bytes());
        b[16..24].copy_from_slice(&self.min_txid.to_be_bytes());
        b[24..32].copy_from_slice(&self.max_txid.to_be_bytes());
        b[32..40].copy_from_slice(&self.timestamp.to_be_bytes());
        if self.no_checksum() {
            b[RESERVED_PRE..RESERVED_PRE + 8].copy_from_slice(&self.pre_apply.to_be_bytes());
            b[RESERVED_POST..RESERVED_POST + 8].copy_from_slice(&self.post_apply.to_be_bytes());
        } else {
            b[40..48].copy_from_slice(&self.pre_apply.to_be_bytes());
        }
        b[48..56].copy_from_slice(&self.wal_offset.to_be_bytes());
        b[56..64].copy_from_slice(&self.wal_size.to_be_bytes());
        b[64..68].copy_from_slice(&self.wal_salt1.to_be_bytes());
        b[68..72].copy_from_slice(&self.wal_salt2.to_be_bytes());
        b[72..80].copy_from_slice(&self.node_id.to_be_bytes());
        b
    }

    pub fn from_bytes(b: &[u8]) -> Result<Header> {
        if b.len() < HEADER_SIZE {
            return invalid("short header");
        }
        if &b[0..4] != MAGIC {
            return invalid("bad magic");
        }
        let u32_at = |o: usize| u32::from_be_bytes(b[o..o + 4].try_into().unwrap());
        let u64_at = |o: usize| u64::from_be_bytes(b[o..o + 8].try_into().unwrap());
        let flags = u32_at(4);
        let (pre_apply, post_apply) = if flags & HEADER_FLAG_NO_CHECKSUM != 0 {
            (u64_at(RESERVED_PRE), u64_at(RESERVED_POST))
        } else {
            (u64_at(40), 0)
        };
        Ok(Header {
            flags,
            page_size: u32_at(8),
            commit: u32_at(12),
            min_txid: u64_at(16),
            max_txid: u64_at(24),
            timestamp: u64_at(32) as i64,
            pre_apply,
            post_apply,
            wal_offset: u64_at(48) as i64,
            wal_size: u64_at(56) as i64,
            wal_salt1: u32_at(64),
            wal_salt2: u32_at(68),
            node_id: u64_at(72),
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.flags & !HEADER_FLAG_NO_CHECKSUM != 0 {
            return invalid(format!("invalid flags: {:#010x}", self.flags));
        }
        if !valid_page_size(self.page_size) {
            return invalid(format!("invalid page size: {}", self.page_size));
        }
        if self.min_txid == 0 || self.max_txid == 0 {
            return invalid("transaction ids required");
        }
        if self.min_txid > self.max_txid {
            return invalid("transaction ids out of order");
        }
        if self.wal_offset < 0 || self.wal_size < 0 {
            return invalid("negative wal offset or size");
        }
        if (self.wal_salt1 != 0 || self.wal_salt2 != 0) && self.wal_offset == 0 {
            return invalid("wal offset required if salt exists");
        }
        if self.wal_offset == 0 && self.wal_size != 0 {
            return invalid("wal offset required if wal size exists");
        }
        if self.is_snapshot() {
            if self.pre_apply != 0 {
                return invalid("pre-apply checksum must be zero on snapshots");
            }
        } else if self.pre_apply == 0 || self.pre_apply & CHECKSUM_FLAG == 0 {
            return invalid("pre-apply checksum required on non-snapshot files");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trailer {
    pub post_apply: u64,
    pub file_checksum: u64,
}

fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub(crate) fn get_uvarint(data: &[u8], pos: &mut usize) -> Result<u64> {
    let mut x = 0u64;
    let mut shift = 0u32;
    loop {
        let b = *data
            .get(*pos)
            .ok_or_else(|| Error::Invalid("truncated varint".into()))?;
        *pos += 1;
        if shift >= 64 || (shift == 63 && b > 1) {
            return invalid("varint overflow");
        }
        x |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return Ok(x);
        }
        shift += 7;
    }
}

/// The page a snapshot has after `prev` (0 before the first): consecutive,
/// without the lock page.
fn next_snapshot_page(prev: u32, lock: u32) -> u32 {
    if prev == 0 {
        1
    } else if prev == lock - 1 {
        prev + 2
    } else {
        prev + 1
    }
}

/// How many pages a file must carry for the database to grow from `old` to
/// `new` pages: each new page, except the lock page.
pub fn pages_added(old: u32, new: u32, lock: u32) -> u64 {
    let added = u64::from(new.saturating_sub(old));
    added - u64::from(old < lock && lock <= new)
}

/// The last page of a complete snapshot of `commit` pages (0 for none).
fn last_snapshot_page(commit: u32, lock: u32) -> u32 {
    if commit == lock {
        lock - 1
    } else {
        commit
    }
}

/// Streaming encoder into memory. Pages must be added in ascending order.
pub struct Encoder {
    header: Header,
    out: Vec<u8>,
    hash: Crc64,
    index: Vec<(u32, u64, u64)>,
    prev_pgno: u32,
    lock: u32,
}

impl Encoder {
    pub fn new(header: Header) -> Result<Encoder> {
        header.validate()?;
        let mut out = Vec::with_capacity(HEADER_SIZE + 1024);
        let hb = header.to_bytes();
        out.extend_from_slice(&hb);
        let mut hash = Crc64::default();
        hash.update(&hb);
        let lock = lock_pgno(header.page_size);
        Ok(Encoder {
            header,
            out,
            hash,
            index: Vec::new(),
            prev_pgno: 0,
            lock,
        })
    }

    pub fn add_page(&mut self, pgno: u32, data: &[u8]) -> Result<()> {
        self.check_page(pgno, data.len())?;
        let compressed = lz4_flex::block::compress(data);
        self.add_frame(pgno, data, &compressed);
        Ok(())
    }

    /// Adds a page whose LZ4 block `compressed` is already known to decompress
    /// to `data`, so merging does not compress again.
    pub fn add_compressed(&mut self, pgno: u32, data: &[u8], compressed: &[u8]) -> Result<()> {
        self.check_page(pgno, data.len())?;
        self.add_frame(pgno, data, compressed);
        Ok(())
    }

    fn check_page(&self, pgno: u32, len: usize) -> Result<()> {
        let h = &self.header;
        if pgno == 0 || pgno > h.commit {
            return invalid(format!("page {pgno} out of bounds for commit {}", h.commit));
        }
        if len != h.page_size as usize {
            return invalid("bad page buffer size");
        }
        if pgno == self.lock {
            return invalid("cannot encode lock page");
        }
        if h.is_snapshot() {
            let expect = next_snapshot_page(self.prev_pgno, self.lock);
            if pgno != expect {
                return invalid(format!(
                    "snapshot pages must be sequential: got {pgno}, expected {expect}"
                ));
            }
        } else if pgno <= self.prev_pgno {
            return invalid("out-of-order page numbers");
        }
        Ok(())
    }

    /// Writes a frame with an already compressed payload. The file checksum
    /// covers the payload length but not its bytes.
    fn add_frame(&mut self, pgno: u32, data: &[u8], compressed: &[u8]) {
        let offset = self.out.len() as u64;
        let mut head = [0u8; PAGE_HEADER_SIZE + 4];
        head[0..4].copy_from_slice(&pgno.to_be_bytes());
        head[4..6].copy_from_slice(&PAGE_FLAG_SIZE.to_be_bytes());
        head[6..10].copy_from_slice(&(compressed.len() as u32).to_be_bytes());
        self.out.extend_from_slice(&head);
        self.out.extend_from_slice(compressed);
        self.hash.update(&head);
        self.hash.update(data);
        self.index
            .push((pgno, offset, self.out.len() as u64 - offset));
        self.prev_pgno = pgno;
    }

    pub fn finish(mut self) -> Result<Vec<u8>> {
        let post_apply = self.header.post_apply;
        if post_apply & CHECKSUM_FLAG == 0 {
            return invalid("post-apply checksum required");
        }
        if self.header.commit == 0 && post_apply != CHECKSUM_FLAG {
            return invalid("post-apply checksum must be empty for zero-length database");
        }
        let zero = [0u8; PAGE_HEADER_SIZE];
        self.out.extend_from_slice(&zero);
        self.hash.update(&zero);

        let start = self.out.len();
        let mut idx = Vec::with_capacity(self.index.len() * 6 + 9);
        for (pgno, offset, size) in &self.index {
            put_uvarint(&mut idx, *pgno as u64);
            put_uvarint(&mut idx, *offset);
            put_uvarint(&mut idx, *size);
        }
        put_uvarint(&mut idx, 0);
        let size = idx.len() as u64;
        idx.extend_from_slice(&size.to_be_bytes());
        self.out.extend_from_slice(&idx);
        self.hash.update(&idx);
        debug_assert_eq!(self.out.len() - start, idx.len());

        // The wire trailer is zero in files that keep checksums in the header.
        let wire_post = if self.header.no_checksum() {
            0
        } else {
            post_apply
        };
        let post = wire_post.to_be_bytes();
        self.hash.update(&post);
        let file_checksum = CHECKSUM_FLAG | self.hash.sum();
        self.out.extend_from_slice(&post);
        self.out.extend_from_slice(&file_checksum.to_be_bytes());
        crate::stats::add(&crate::stats::LTX_BYTES_ENCODED, self.out.len());
        Ok(self.out)
    }
}

/// Decoder over an in-memory file.
pub struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
    pub header: Header,
    hash: Crc64,
    frames: Vec<(u32, u64, u64)>,
    at_end_of_pages: bool,
    lock: u32,
    /// The last page read, for the order of snapshot pages.
    prev_pgno: u32,
}

impl<'a> Decoder<'a> {
    pub fn new(data: &'a [u8]) -> Result<Decoder<'a>> {
        let header = Header::from_bytes(data)?;
        header.validate()?;
        crate::stats::add(&crate::stats::LTX_BYTES_DECODED, data.len());
        let mut hash = Crc64::default();
        hash.update(&data[..HEADER_SIZE]);
        let lock = lock_pgno(header.page_size);
        Ok(Decoder {
            data,
            pos: HEADER_SIZE,
            header,
            hash,
            frames: Vec::new(),
            at_end_of_pages: false,
            lock,
            prev_pgno: 0,
        })
    }

    /// Reads the next page into `buf` (page-size bytes) and returns its number,
    /// or `None` at the end of the page block.
    pub fn next_page(&mut self, buf: &mut [u8]) -> Result<Option<u32>> {
        if self.at_end_of_pages {
            return Ok(None);
        }
        let start = self.pos;
        let head = self
            .data
            .get(start..start + PAGE_HEADER_SIZE)
            .ok_or_else(|| Error::Invalid("truncated page header".into()))?;
        self.hash.update(head);
        let pgno = u32::from_be_bytes(head[0..4].try_into().unwrap());
        let flags = u16::from_be_bytes(head[4..6].try_into().unwrap());
        self.pos += PAGE_HEADER_SIZE;
        if pgno == 0 && flags == 0 {
            self.at_end_of_pages = true;
            return Ok(None);
        }
        if pgno == 0 {
            return invalid("page number required");
        }
        if flags & !PAGE_FLAG_SIZE != 0 {
            return invalid("invalid page header flags");
        }
        if flags & PAGE_FLAG_SIZE == 0 {
            return invalid("legacy page frames (LZ4 frame format) are not supported");
        }
        if pgno > self.header.commit || pgno == self.lock {
            return invalid(format!("page {pgno} not allowed here"));
        }
        // Pages ascend, so none repeats; a snapshot replaces the database and
        // has every page.
        if pgno <= self.prev_pgno {
            return invalid(format!("page {pgno} out of order"));
        }
        if self.header.is_snapshot() && pgno != next_snapshot_page(self.prev_pgno, self.lock) {
            return invalid(format!("snapshot page {pgno} out of sequence"));
        }
        self.prev_pgno = pgno;
        let size_bytes = self
            .data
            .get(self.pos..self.pos + 4)
            .ok_or_else(|| Error::Invalid("truncated page size".into()))?;
        self.hash.update(size_bytes);
        let n = u32::from_be_bytes(size_bytes.try_into().unwrap()) as usize;
        self.pos += 4;
        let payload = self
            .data
            .get(self.pos..self.pos + n)
            .ok_or_else(|| Error::Invalid("truncated page data".into()))?;
        let got = lz4_flex::block::decompress_into(payload, buf)
            .map_err(|e| Error::Invalid(format!("decompress page {pgno}: {e}")))?;
        if got != buf.len() {
            return invalid(format!("page {pgno} decompressed to {got} bytes"));
        }
        self.pos += n;
        self.hash.update(buf);
        self.frames
            .push((pgno, start as u64, (self.pos - start) as u64));
        Ok(Some(pgno))
    }

    /// Verifies the page index, trailer and file checksum. Call after
    /// `next_page` returned `None`.
    pub fn finish(mut self) -> Result<Trailer> {
        if !self.at_end_of_pages {
            return invalid("page block not fully read");
        }
        if self.header.is_snapshot()
            && self.prev_pgno != last_snapshot_page(self.header.commit, self.lock)
        {
            return invalid("snapshot is missing pages");
        }
        let index_start = self.pos;
        let mut p = self.pos;
        let mut entries = 0usize;
        loop {
            let pgno = get_uvarint(self.data, &mut p)?;
            if pgno == 0 {
                break;
            }
            let offset = get_uvarint(self.data, &mut p)?;
            let size = get_uvarint(self.data, &mut p)?;
            match self.frames.get(entries) {
                Some(&(fp, fo, fs)) if fp as u64 == pgno && fo == offset && fs == size => {}
                _ => return invalid("page index does not match the decoded pages"),
            }
            entries += 1;
        }
        if entries != self.frames.len() {
            return invalid("page index has fewer entries than pages");
        }
        let entries_len = (p - index_start) as u64;
        let size_field = self
            .data
            .get(p..p + 8)
            .ok_or_else(|| Error::Invalid("truncated page index size".into()))?;
        if u64::from_be_bytes(size_field.try_into().unwrap()) != entries_len {
            return invalid("page index size mismatch");
        }
        p += 8;
        self.hash.update(&self.data[index_start..p]);

        let trailer = self
            .data
            .get(p..)
            .ok_or_else(|| Error::Invalid("truncated trailer".into()))?;
        if trailer.len() != TRAILER_SIZE {
            return invalid("unexpected data after trailer or truncated trailer");
        }
        self.hash.update(&trailer[..8]);
        let wire_post = u64::from_be_bytes(trailer[..8].try_into().unwrap());
        let file_checksum = u64::from_be_bytes(trailer[8..].try_into().unwrap());
        // The fuzzers cannot guess a CRC, so they skip this one check.
        if !cfg!(fuzzing) && CHECKSUM_FLAG | self.hash.sum() != file_checksum {
            return invalid("file checksum mismatch");
        }
        let post_apply = trailer_post(&self.header, wire_post)?;
        Ok(Trailer {
            post_apply,
            file_checksum,
        })
    }
}

/// The post-apply checksum of a file, from the header or the trailer
/// depending on the flag.
pub(crate) fn trailer_post(h: &Header, wire: u64) -> Result<u64> {
    let post = if h.no_checksum() {
        if wire != 0 {
            return invalid("post-apply checksum not allowed in the trailer");
        }
        h.post_apply
    } else {
        wire
    };
    if post == 0 || post & CHECKSUM_FLAG == 0 {
        return invalid("post-apply checksum required");
    }
    Ok(post)
}

/// Header and trailer without verifying the page block.
pub fn peek(data: &[u8]) -> Result<(Header, Trailer)> {
    let header = Header::from_bytes(data)?;
    header.validate()?;
    if data.len() < HEADER_SIZE + TRAILER_SIZE {
        return invalid("short file");
    }
    let t = &data[data.len() - TRAILER_SIZE..];
    let post_apply = trailer_post(&header, u64::from_be_bytes(t[..8].try_into().unwrap()))?;
    Ok((
        header,
        Trailer {
            post_apply,
            file_checksum: u64::from_be_bytes(t[8..].try_into().unwrap()),
        },
    ))
}

/// Location of one page frame inside a file: page number, offset of the
/// frame header and frame length.
pub type Frame = (u32, usize, usize);

/// Fully verifies a file page by page, keeping one page in memory, and
/// returns its header, trailer and frame locations.
pub fn verify(data: &[u8]) -> Result<(Header, Trailer, Vec<Frame>)> {
    let mut dec = Decoder::new(data)?;
    let header = dec.header.clone();
    let mut buf = vec![0u8; header.page_size as usize];
    let mut frames = Vec::new();
    while dec.next_page(&mut buf)?.is_some() {
        let &(pgno, start, len) = dec.frames.last().expect("frame recorded");
        frames.push((pgno, start as usize, len as usize));
    }
    let trailer = dec.finish()?;
    Ok((header, trailer, frames))
}

/// The LZ4 block of a frame returned by `verify`.
pub fn frame_payload<'a>(data: &'a [u8], frame: &Frame) -> &'a [u8] {
    &data[frame.1 + PAGE_HEADER_SIZE + 4..frame.1 + frame.2]
}

/// Fully decodes and verifies a file, returning its pages.
#[cfg(any(test, feature = "fuzzing"))]
pub type Pages = Vec<(u32, Vec<u8>)>;

#[cfg(any(test, feature = "fuzzing"))]
pub fn decode_all(data: &[u8]) -> Result<(Header, Pages, Trailer)> {
    let mut dec = Decoder::new(data)?;
    let header = dec.header.clone();
    let mut pages = Vec::new();
    loop {
        let mut buf = vec![0u8; header.page_size as usize];
        match dec.next_page(&mut buf)? {
            Some(pgno) => pages.push((pgno, buf)),
            None => break,
        }
    }
    let trailer = dec.finish()?;
    Ok((header, pages, trailer))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(seed: u8) -> Vec<u8> {
        (0..512).map(|i| seed.wrapping_add((i % 7) as u8)).collect()
    }

    #[test]
    fn duplicate_pages_in_an_increment_are_rejected() {
        let h = Header {
            page_size: 512,
            commit: 3,
            min_txid: 2,
            max_txid: 2,
            timestamp: 1,
            pre_apply: CHECKSUM_FLAG | 1,
            post_apply: CHECKSUM_FLAG | 2,
            ..Default::default()
        };
        let mut e = Encoder::new(h).unwrap();
        for seed in [1, 2] {
            let data = page(seed);
            e.add_frame(2, &data, &lz4_flex::block::compress(&data));
        }
        let out = e.finish().unwrap();
        assert!(verify(&out).is_err(), "page 2 twice, page 3 never");
    }

    // Golden values from superfly/ltx encoder_golden_test.go. The file checksum
    // covers each payload's length, and upstream's LZ4 compressor (pierrec/lz4)
    // writes 31 bytes for these pages, so the frames are built with a payload
    // of that length instead of this crate's compressor output.
    const UPSTREAM_PAYLOAD_LEN: usize = 31;

    #[test]
    fn matches_upstream_golden_sparse_non_snapshot() {
        let h = Header {
            page_size: 512,
            commit: 16,
            min_txid: 2,
            max_txid: 3,
            timestamp: 1000,
            pre_apply: CHECKSUM_FLAG | 1,
            post_apply: CHECKSUM_FLAG | 2,
            ..Default::default()
        };
        let mut e = Encoder::new(h).unwrap();
        for pgno in [2u32, 5, 9, 16] {
            e.add_frame(pgno, &page(pgno as u8), &[0; UPSTREAM_PAYLOAD_LEN]);
        }
        let out = e.finish().unwrap();
        let (_, t) = peek(&out).unwrap();
        assert_eq!(t.file_checksum, 0xe27f4c18f0da2c10);
    }

    #[test]
    fn matches_upstream_golden_snapshot() {
        let h = Header {
            page_size: 512,
            commit: 6,
            min_txid: 1,
            max_txid: 4,
            timestamp: 1000,
            post_apply: CHECKSUM_FLAG | 3,
            ..Default::default()
        };
        let mut e = Encoder::new(h).unwrap();
        for pgno in 1..=6u32 {
            e.add_frame(pgno, &page(pgno as u8), &[0; UPSTREAM_PAYLOAD_LEN]);
        }
        let out = e.finish().unwrap();
        let (_, t) = peek(&out).unwrap();
        assert_eq!(t.file_checksum, 0xd926967672f7ca53);
    }

    #[test]
    fn round_trip_preserves_header_and_pages() {
        let h = Header {
            flags: HEADER_FLAG_NO_CHECKSUM,
            post_apply: CHECKSUM_FLAG | 5,
            page_size: 1024,
            commit: 9,
            min_txid: 7,
            max_txid: 7,
            timestamp: 123456,
            pre_apply: CHECKSUM_FLAG | 99,
            wal_offset: 32,
            wal_size: 2000,
            wal_salt1: 5,
            wal_salt2: 6,
            ..Default::default()
        };
        let mut e = Encoder::new(h.clone()).unwrap();
        let a = vec![1u8; 1024];
        let b: Vec<u8> = (0..1024).map(|i| (i * 31 % 251) as u8).collect();
        e.add_page(3, &a).unwrap();
        e.add_page(9, &b).unwrap();
        let out = e.finish().unwrap();
        // The specified fields are zero; ours are in the reserved bytes.
        assert_eq!(&out[40..48], &[0; 8]);
        assert_eq!(&out[out.len() - 16..out.len() - 8], &[0; 8]);
        let (hdr, pages, trailer) = decode_all(&out).unwrap();
        assert_eq!(hdr, h);
        assert_eq!(pages, vec![(3, a), (9, b)]);
        assert_eq!(trailer.post_apply, CHECKSUM_FLAG | 5);
    }

    #[test]
    fn empty_segment_round_trips() {
        let h = Header {
            page_size: 512,
            commit: 3,
            min_txid: 4,
            max_txid: 4,
            pre_apply: CHECKSUM_FLAG | 8,
            post_apply: CHECKSUM_FLAG | 8,
            ..Default::default()
        };
        let out = Encoder::new(h).unwrap().finish().unwrap();
        let (_, pages, _) = decode_all(&out).unwrap();
        assert!(pages.is_empty());
    }

    #[test]
    fn detects_corruption_anywhere() {
        let h = Header {
            page_size: 512,
            commit: 2,
            min_txid: 1,
            max_txid: 1,
            flags: HEADER_FLAG_NO_CHECKSUM,
            post_apply: CHECKSUM_FLAG | 1,
            ..Default::default()
        };
        let mut e = Encoder::new(h).unwrap();
        e.add_page(1, &page(1)).unwrap();
        e.add_page(2, &page(2)).unwrap();
        let good = e.finish().unwrap();
        decode_all(&good).unwrap();
        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0x40;
            assert!(decode_all(&bad).is_err(), "flip at byte {i} went unnoticed");
        }
        assert!(decode_all(&good[..good.len() - 1]).is_err());
        let mut extra = good.clone();
        extra.push(0);
        assert!(decode_all(&extra).is_err());
    }

    #[test]
    fn rejects_bad_page_order_and_lock_page() {
        let h = Header {
            page_size: 512,
            commit: 10,
            min_txid: 2,
            max_txid: 2,
            pre_apply: CHECKSUM_FLAG | 1,
            ..Default::default()
        };
        let mut e = Encoder::new(h).unwrap();
        e.add_page(4, &page(0)).unwrap();
        assert!(e.add_page(4, &page(0)).is_err());
        assert!(e.add_page(11, &page(0)).is_err());
        let snap = Header {
            page_size: 512,
            commit: 3,
            min_txid: 1,
            max_txid: 1,
            ..Default::default()
        };
        let mut e = Encoder::new(snap).unwrap();
        assert!(e.add_page(2, &page(0)).is_err());
    }

    #[test]
    fn a_snapshot_must_have_every_page() {
        let snapshot = |pages: &[u32]| {
            let h = Header {
                page_size: 512,
                commit: 6,
                min_txid: 1,
                max_txid: 1,
                flags: HEADER_FLAG_NO_CHECKSUM,
                post_apply: CHECKSUM_FLAG | 1,
                ..Default::default()
            };
            let mut e = Encoder::new(h).unwrap();
            for &p in pages {
                let data = page(p as u8);
                // Frames are written directly: `add_page` refuses the gaps.
                e.add_frame(p, &data, &lz4_flex::block::compress(&data));
            }
            e.finish().unwrap()
        };
        decode_all(&snapshot(&[1, 2, 3, 4, 5, 6])).unwrap();
        assert!(decode_all(&snapshot(&[1, 2, 3])).is_err());
        assert!(decode_all(&snapshot(&[2, 3, 4, 5, 6])).is_err());
        assert!(decode_all(&snapshot(&[1, 3, 2, 4, 5, 6])).is_err());
        assert!(decode_all(&snapshot(&[])).is_err());
    }

    #[test]
    fn lock_page_for_4k_pages() {
        assert_eq!(lock_pgno(4096), 262145);
    }
}

#[cfg(test)]
mod props {
    use super::*;
    use proptest::prelude::*;

    /// A file with random header fields and pages; a snapshot has every page.
    #[derive(Debug)]
    struct File {
        header: Header,
        pages: Vec<(u32, Vec<u8>)>,
    }

    fn page(size: usize) -> impl Strategy<Value = Vec<u8>> {
        // A filled page with a few random bytes: compressible, never uniform.
        (
            any::<u8>(),
            prop::collection::vec((0..size, any::<u8>()), 0..8),
        )
            .prop_map(move |(fill, edits)| {
                let mut p = vec![fill; size];
                for (i, b) in edits {
                    p[i] = b;
                }
                p
            })
    }

    type Shape = (u32, u32, bool, bool, (u64, i64, u64, u64), u64);

    fn file() -> impl Strategy<Value = File> {
        let shape = (
            prop::sample::select(vec![512u32, 1024]),
            1u32..8,
            any::<bool>(),
            any::<bool>(),
            (any::<u64>(), any::<i64>(), any::<u64>(), 0u64..4),
            any::<u64>(),
        );
        shape
            .prop_flat_map(|s: Shape| {
                let kept = prop::collection::vec(any::<bool>(), s.1 as usize);
                let pages = prop::collection::vec(page(s.0 as usize), s.1 as usize);
                (Just(s), kept, pages)
            })
            .prop_map(|(s, kept, data)| {
                let (ps, commit, snapshot, nocksum, ids, post) = s;
                let (txid, timestamp, node_id, span) = ids;
                let min_txid = if snapshot { 1 } else { txid % (1 << 40) + 2 };
                let header = Header {
                    flags: if nocksum { HEADER_FLAG_NO_CHECKSUM } else { 0 },
                    page_size: ps,
                    commit,
                    min_txid,
                    max_txid: min_txid + span,
                    timestamp,
                    pre_apply: if snapshot {
                        0
                    } else {
                        CHECKSUM_FLAG | post >> 1
                    },
                    post_apply: CHECKSUM_FLAG | post >> 1,
                    node_id,
                    ..Default::default()
                };
                let pages = data
                    .into_iter()
                    .zip(kept)
                    .enumerate()
                    .filter(|(_, (_, keep))| snapshot || *keep)
                    .map(|(i, (d, _))| (i as u32 + 1, d))
                    .collect();
                File { header, pages }
            })
    }

    fn encode(f: &File) -> Vec<u8> {
        let mut e = Encoder::new(f.header.clone()).unwrap();
        for (pgno, data) in &f.pages {
            e.add_page(*pgno, data).unwrap();
        }
        e.finish().unwrap()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Decoding gives back the header and the pages that were encoded.
        #[test]
        #[cfg_attr(miri, ignore = "proptest is too slow under Miri")]
        fn encode_then_decode_returns_the_input(f in file()) {
            let (header, pages, trailer) = decode_all(&encode(&f)).unwrap();
            let mut expected = f.header.clone();
            if !expected.no_checksum() {
                // Such files keep the post-apply checksum in the trailer only.
                expected.post_apply = 0;
            }
            prop_assert_eq!(&header, &expected);
            prop_assert_eq!(&pages, &f.pages);
            prop_assert_eq!(trailer.post_apply, f.header.post_apply);
        }

        /// An altered byte is rejected or changes nothing: the file checksum
        /// covers the decompressed pages, so a different LZ4 block of the
        /// same page may pass. A cut file never decodes.
        #[test]
        #[cfg_attr(miri, ignore = "proptest is too slow under Miri")]
        fn a_flipped_byte_never_changes_what_decodes_and_a_cut_or_extended_file_is_rejected(
            f in file(),
            at in any::<prop::sample::Index>(),
            mask in 1u8..,
            cut in any::<prop::sample::Index>(),
        ) {
            let good = encode(&f);
            let mut bad = good.clone();
            bad[at.index(good.len())] ^= mask;
            if let Ok(decoded) = decode_all(&bad) {
                prop_assert_eq!(decoded, decode_all(&good).unwrap());
            }
            prop_assert!(decode_all(&good[..cut.index(good.len())]).is_err());
            let mut longer = good;
            longer.push(mask);
            prop_assert!(decode_all(&longer).is_err());
        }
    }
}
