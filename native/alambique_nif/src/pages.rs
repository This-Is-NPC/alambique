//! Page-hash sidecar of an LTX file (docs/segment-format.md): one BLAKE3-128
//! hash per page, and one of every byte that is not a page frame, so that a
//! lazy restore can trust the pages it reads by range without reading the
//! whole object (design §16).
//!
//! Layout, big endian:
//!
//! ```text
//! 0   "ALPH"  version u8  hash id u8  level u8  0 u8
//! 8   min TXID u64  max TXID u64  page size u32  count u32
//! 32  file checksum u64  object size u64  meta hash [16]
//! 64  count x (page number u32, page hash [16])   ascending
//!     seal [16]   hash of everything before it
//! ```
//!
//! A page hash covers a domain tag, the page number, the file checksum of the
//! object and the page bytes. The hash is not linear, unlike the CRCs of the
//! file, so errors cannot cancel in it. The meta hash covers the header of
//! the LTX file and everything after its last frame (page index, index size,
//! trailer).

use crate::error::{invalid, Result};
use crate::ltx::{peek, Decoder, HEADER_SIZE, TRAILER_SIZE};

const MAGIC: &[u8; 4] = b"ALPH";
const VERSION: u8 = 1;
const HASH_BLAKE3_128: u8 = 1;
const HEAD_LEN: usize = 64;
const ENTRY_LEN: usize = 20;
const HASH_LEN: usize = 16;

pub type Hash = [u8; HASH_LEN];

/// Bytes of the sidecar of an object with `count` pages in its index.
pub fn len_for(count: usize) -> usize {
    HEAD_LEN + count * ENTRY_LEN + HASH_LEN
}

fn digest(parts: &[&[u8]]) -> Hash {
    let mut h = blake3::Hasher::new();
    for p in parts {
        h.update(p);
    }
    let mut out = [0u8; HASH_LEN];
    h.finalize_xof().fill(&mut out);
    out
}

pub fn page_hash(file_checksum: u64, pgno: u32, page: &[u8]) -> Hash {
    digest(&[
        b"alambique-page-v1",
        &pgno.to_be_bytes(),
        &file_checksum.to_be_bytes(),
        page,
    ])
}

/// `head` is the LTX header, `rest` the bytes from the page index to the end
/// of the file.
pub fn meta_hash(head: &[u8], rest: &[u8]) -> Hash {
    digest(&[b"alambique-meta-v1", head, rest])
}

fn seal(body: &[u8]) -> Hash {
    digest(&[b"alambique-sidecar-v1", body])
}

/// The page hashes of one object, as a lazy restore keeps them.
pub struct Hashes {
    pub file_checksum: u64,
    pgnos: Vec<u32>,
    hashes: Vec<Hash>,
}

impl Hashes {
    /// Whether `page` is the page `pgno` of the object.
    pub fn matches(&self, pgno: u32, page: &[u8]) -> bool {
        self.pgnos
            .binary_search(&pgno)
            .is_ok_and(|i| self.hashes[i] == page_hash(self.file_checksum, pgno, page))
    }

    pub fn pgnos(&self) -> &[u32] {
        &self.pgnos
    }
}

pub struct Parsed {
    pub level: u8,
    pub min_txid: u64,
    pub max_txid: u64,
    pub page_size: u32,
    pub object_size: u64,
    pub meta: Hash,
    pub hashes: Hashes,
}

/// Reads a sidecar. Anything but a sidecar of this version, intact, is an
/// error: the caller then reads the object whole.
pub fn parse(b: &[u8]) -> Result<Parsed> {
    if b.len() < HEAD_LEN + HASH_LEN {
        return invalid("short page sidecar");
    }
    let (body, tail) = b.split_at(b.len() - HASH_LEN);
    if seal(body) != tail {
        return invalid("page sidecar seal mismatch");
    }
    if &body[..4] != MAGIC || body[4] != VERSION || body[5] != HASH_BLAKE3_128 || body[7] != 0 {
        return invalid("unknown page sidecar version");
    }
    let be64 = |at: usize| u64::from_be_bytes(body[at..at + 8].try_into().unwrap());
    let be32 = |at: usize| u32::from_be_bytes(body[at..at + 4].try_into().unwrap());
    let count = be32(28) as usize;
    if body.len() - HEAD_LEN != count * ENTRY_LEN {
        return invalid("page sidecar count differs from its size");
    }
    let mut pgnos = Vec::with_capacity(count);
    let mut hashes = Vec::with_capacity(count);
    for e in body[HEAD_LEN..].chunks_exact(ENTRY_LEN) {
        pgnos.push(u32::from_be_bytes(e[..4].try_into().unwrap()));
        hashes.push(e[4..].try_into().unwrap());
    }
    Ok(Parsed {
        level: body[6],
        min_txid: be64(8),
        max_txid: be64(16),
        page_size: be32(24),
        object_size: be64(40),
        meta: body[48..64].try_into().unwrap(),
        hashes: Hashes {
            file_checksum: be64(32),
            pgnos,
            hashes,
        },
    })
}

/// The sidecar of the whole LTX file `object`, which lives at `level`. The
/// object is decoded and checked on the way, so a sidecar is never built
/// from damaged bytes.
pub fn build(level: u8, object: &[u8]) -> Result<Vec<u8>> {
    let (header, trailer) = peek(object)?;
    let mut dec = Decoder::new(object)?;
    let mut page = vec![0u8; header.page_size as usize];
    let mut entries = Vec::new();
    while let Some(pgno) = dec.next_page(&mut page)? {
        entries.push((pgno, page_hash(trailer.file_checksum, pgno, &page)));
    }
    dec.finish()?;
    let rest = &object[rest_start(object)?..];

    let mut out = Vec::with_capacity(len_for(entries.len()));
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&[VERSION, HASH_BLAKE3_128, level, 0]);
    out.extend_from_slice(&header.min_txid.to_be_bytes());
    out.extend_from_slice(&header.max_txid.to_be_bytes());
    out.extend_from_slice(&header.page_size.to_be_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    out.extend_from_slice(&trailer.file_checksum.to_be_bytes());
    out.extend_from_slice(&(object.len() as u64).to_be_bytes());
    out.extend_from_slice(&meta_hash(&object[..HEADER_SIZE], rest));
    for (pgno, hash) in &entries {
        out.extend_from_slice(&pgno.to_be_bytes());
        out.extend_from_slice(hash);
    }
    let sealed = seal(&out);
    out.extend_from_slice(&sealed);
    Ok(out)
}

/// A copy of the sidecar `side` whose first page hash is wrong and whose seal
/// is right: intact, and wrong (for tests of what a restore does with one).
#[cfg(any(test, feature = "test-panic"))]
pub fn forge(side: &[u8]) -> Vec<u8> {
    let mut body = side[..side.len() - HASH_LEN].to_vec();
    body[HEAD_LEN + 4] ^= 1;
    let sealed = seal(&body);
    body.extend_from_slice(&sealed);
    body
}

/// Where the page index starts in a verified object.
fn rest_start(object: &[u8]) -> Result<usize> {
    let at = object.len() - TRAILER_SIZE - 8;
    let index_len = u64::from_be_bytes(object[at..at + 8].try_into().unwrap());
    usize::try_from(index_len)
        .ok()
        .and_then(|n| at.checked_sub(n))
        .map_or_else(|| invalid("page index size out of range"), Ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ltx::{Encoder, Header, CHECKSUM_FLAG, HEADER_FLAG_NO_CHECKSUM};

    fn page(pgno: u32, fill: u8) -> Vec<u8> {
        (0..512)
            .map(|i| fill.wrapping_add((i % 13) as u8) ^ pgno as u8)
            .collect()
    }

    fn object() -> Vec<u8> {
        let h = Header {
            page_size: 512,
            commit: 3,
            min_txid: 1,
            max_txid: 1,
            timestamp: 1,
            post_apply: CHECKSUM_FLAG | 1,
            flags: HEADER_FLAG_NO_CHECKSUM,
            ..Default::default()
        };
        let mut enc = Encoder::new(h).unwrap();
        for p in 1..=3 {
            enc.add_page(p, &page(p, p as u8)).unwrap();
        }
        enc.finish().unwrap()
    }

    #[test]
    fn a_sidecar_names_the_object_it_describes() {
        let o = object();
        let p = parse(&build(9, &o).unwrap()).unwrap();
        assert_eq!((p.level, p.min_txid, p.max_txid), (9, 1, 1));
        assert_eq!((p.object_size, p.page_size), (o.len() as u64, 512));
        assert_eq!(p.hashes.pgnos(), &[1, 2, 3]);
        let two = page(2, 2);
        assert!(p.hashes.matches(2, &two));
        assert!(!p.hashes.matches(3, &two), "the page number is hashed");
        assert!(!p.hashes.matches(2, &page(2, 3)));
        assert!(!p.hashes.matches(4, &two), "a page the sidecar lacks");
        assert_eq!(build(9, &o).unwrap().len(), len_for(3));
    }

    #[test]
    fn the_file_checksum_is_part_of_every_page_hash() {
        let one = page(1, 1);
        assert_ne!(page_hash(1, 1, &one), page_hash(2, 1, &one));
    }

    #[test]
    fn a_damaged_sidecar_is_refused() {
        let s = build(0, &object()).unwrap();
        for i in [0, 5, 6, 12, 30, 45, 55, 70, s.len() - 1] {
            let mut bad = s.clone();
            bad[i] ^= 1;
            assert!(parse(&bad).is_err(), "byte {i}");
        }
        assert!(parse(&s[..s.len() - 1]).is_err());
        assert!(parse(&s[..HEAD_LEN]).is_err());
        assert!(parse(&[]).is_err());
    }

    #[test]
    fn an_unknown_version_is_refused_even_with_a_valid_seal() {
        let s = build(0, &object()).unwrap();
        let mut body = s[..s.len() - HASH_LEN].to_vec();
        body[4] = 2;
        let sealed = seal(&body);
        body.extend_from_slice(&sealed);
        assert!(parse(&body).is_err());
    }

    #[test]
    fn a_damaged_object_has_no_sidecar() {
        let mut o = object();
        o[HEADER_SIZE + 12] ^= 1;
        assert!(build(0, &o).is_err());
    }

    #[test]
    fn the_meta_hash_covers_the_header_and_the_index() {
        let o = object();
        let rest = &o[rest_start(&o).unwrap()..];
        let h = meta_hash(&o[..HEADER_SIZE], rest);
        let mut head = o[..HEADER_SIZE].to_vec();
        head[40] ^= 1;
        assert_ne!(h, meta_hash(&head, rest));
        let mut r = rest.to_vec();
        r[0] ^= 1;
        assert_ne!(h, meta_hash(&o[..HEADER_SIZE], &r));
    }
}
