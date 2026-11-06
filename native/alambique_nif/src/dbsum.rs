//! Rolling database checksum and application of LTX files to a database file.
//!
//! The database checksum is `CHECKSUM_FLAG | xor(page_crc(pgno, page))` over
//! every page except the lock page, so replacing a page costs two page
//! checksums. `sums` below holds the raw per-page CRCs (zero for the lock page).

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::error::{invalid, Error, Result};
use crate::ltx::{self, lock_pgno, page_crc, Decoder, CHECKSUM_FLAG};

/// Database checksum from per-page CRCs.
pub fn fold(sums: &[u64]) -> u64 {
    CHECKSUM_FLAG | sums.iter().fold(0, |acc, s| acc ^ s)
}

/// Per-page CRCs of a database file. Trailing bytes that do not fill a page
/// are ignored.
pub fn file_sums(path: &Path, page_size: u32) -> Result<Vec<u64>> {
    if !ltx::valid_page_size(page_size) {
        return invalid("invalid page size");
    }
    let file = File::open(path)?;
    let pages = file.metadata()?.len() / page_size as u64;
    let lock = lock_pgno(page_size) as u64;
    let mut buf = vec![0u8; page_size as usize];
    let mut sums = Vec::with_capacity(pages as usize);
    for pgno in 1..=pages {
        if pgno == lock {
            sums.push(0);
            continue;
        }
        file.read_exact_at(&mut buf, (pgno - 1) * page_size as u64)?;
        sums.push(page_crc(pgno as u32, &buf));
    }
    Ok(sums)
}

#[derive(Debug)]
pub struct Applied {
    pub header: ltx::Header,
    pub post_apply: u64,
}

/// Applies an LTX file to the database at `path`.
///
/// The file is verified before the database is touched. `current` is the
/// database checksum before applying (ignored for snapshots, which replace the
/// database). `page_size` of 0 accepts any page size.
pub fn apply(path: &Path, data: &[u8], current: u64, page_size: u32) -> Result<Applied> {
    let (header, trailer, frames) = ltx::verify(data)?;
    if page_size != 0 && header.page_size != page_size {
        return invalid(format!(
            "page size {} does not match database page size {page_size}",
            header.page_size
        ));
    }
    let ps = header.page_size as u64;
    let lock = lock_pgno(header.page_size);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let snapshot = header.is_snapshot();
    let old_commit = if snapshot {
        0
    } else {
        (file.metadata()?.len() / ps) as u32
    };
    if !snapshot && header.pre_apply != current {
        return Err(Error::PreApplyMismatch {
            expected: header.pre_apply,
            got: current,
        });
    }
    // Pages the file leaves out would stay holes that no checksum accounts for.
    let added = frames.iter().filter(|f| f.0 > old_commit).count() as u64;
    if added != ltx::pages_added(old_commit, header.commit, lock) {
        return invalid("file lacks pages the database grows by");
    }
    if snapshot {
        file.set_len(0)?;
    }
    let mut acc = if snapshot {
        0
    } else {
        current & !CHECKSUM_FLAG
    };

    let mut dec = Decoder::new(data)?;
    let mut buf = vec![0u8; ps as usize];
    let mut old = vec![0u8; ps as usize];
    while let Some(pgno) = dec.next_page(&mut buf)? {
        if pgno <= old_commit {
            file.read_exact_at(&mut old, (pgno as u64 - 1) * ps)?;
            acc ^= page_crc(pgno, &old);
        }
        acc ^= page_crc(pgno, &buf);
        file.write_all_at(&buf, (pgno as u64 - 1) * ps)?;
    }
    for pgno in header.commit.saturating_add(1)..=old_commit {
        if pgno == lock {
            continue;
        }
        file.read_exact_at(&mut old, (pgno as u64 - 1) * ps)?;
        acc ^= page_crc(pgno, &old);
    }
    file.set_len(header.commit as u64 * ps)?;

    let post_apply = CHECKSUM_FLAG | acc;
    if post_apply != trailer.post_apply {
        return Err(Error::PostApplyMismatch {
            expected: trailer.post_apply,
            got: post_apply,
        });
    }
    Ok(Applied { header, post_apply })
}

/// Fully decodes `data` without applying it.
pub fn verify(data: &[u8]) -> Result<(ltx::Header, ltx::Trailer)> {
    let mut dec = Decoder::new(data)?;
    let mut buf = vec![0u8; dec.header.page_size as usize];
    while dec.next_page(&mut buf)?.is_some() {}
    let header = dec.header.clone();
    let trailer = dec.finish()?;
    Ok((header, trailer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ltx::{Encoder, Header};

    pub fn page(pgno: u32, fill: u8, ps: usize) -> Vec<u8> {
        (0..ps)
            .map(|i| fill.wrapping_add(pgno as u8).wrapping_add((i % 13) as u8))
            .collect()
    }

    fn snapshot(pages: &[Vec<u8>], txid: u64) -> (Vec<u8>, u64) {
        let ps = pages[0].len();
        let acc = pages
            .iter()
            .enumerate()
            .fold(0, |a, (i, p)| a ^ page_crc(i as u32 + 1, p));
        let h = Header {
            page_size: ps as u32,
            commit: pages.len() as u32,
            min_txid: 1,
            max_txid: txid,
            post_apply: CHECKSUM_FLAG | acc,
            ..Default::default()
        };
        let mut e = Encoder::new(h).unwrap();
        for (i, p) in pages.iter().enumerate() {
            e.add_page(i as u32 + 1, p).unwrap();
        }
        (e.finish().unwrap(), CHECKSUM_FLAG | acc)
    }

    #[test]
    fn snapshot_then_incremental_apply_tracks_the_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut pages: Vec<Vec<u8>> = (1..=5).map(|n| page(n, 1, 512)).collect();
        let (snap, c1) = snapshot(&pages, 1);
        let applied = apply(&path, &snap, 0, 0).unwrap();
        assert_eq!(applied.post_apply, c1);
        assert_eq!(fold(&file_sums(&path, 512).unwrap()), c1);

        // Change page 2, add page 6, drop nothing.
        pages[1] = page(2, 9, 512);
        pages.push(page(6, 4, 512));
        let acc: u64 = pages
            .iter()
            .enumerate()
            .fold(0, |a, (i, p)| a ^ page_crc(i as u32 + 1, p));
        let h = Header {
            page_size: 512,
            commit: 6,
            min_txid: 2,
            max_txid: 2,
            pre_apply: c1,
            post_apply: CHECKSUM_FLAG | acc,
            ..Default::default()
        };
        let mut e = Encoder::new(h).unwrap();
        e.add_page(2, &pages[1]).unwrap();
        e.add_page(6, &pages[5]).unwrap();
        let seg = e.finish().unwrap();
        let applied = apply(&path, &seg, c1, 512).unwrap();
        assert_eq!(applied.post_apply, CHECKSUM_FLAG | acc);
        assert_eq!(fold(&file_sums(&path, 512).unwrap()), CHECKSUM_FLAG | acc);
        let content = std::fs::read(&path).unwrap();
        assert_eq!(content, pages.concat());
    }

    #[test]
    fn growing_without_the_new_pages_is_refused_and_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let pages: Vec<Vec<u8>> = (1..=2).map(|n| page(n, 3, 512)).collect();
        let (snap, c1) = snapshot(&pages, 1);
        apply(&path, &snap, 0, 0).unwrap();
        // Commit 4, but page 3 is missing.
        let h = Header {
            page_size: 512,
            commit: 4,
            min_txid: 2,
            max_txid: 2,
            pre_apply: c1,
            post_apply: CHECKSUM_FLAG | 7,
            ..Default::default()
        };
        let mut e = Encoder::new(h).unwrap();
        e.add_page(4, &page(4, 1, 512)).unwrap();
        let seg = e.finish().unwrap();
        assert!(matches!(
            apply(&path, &seg, c1, 512),
            Err(Error::Invalid(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), pages.concat());
    }

    #[test]
    fn shrinking_commit_truncates_and_updates_the_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let pages: Vec<Vec<u8>> = (1..=4).map(|n| page(n, 3, 512)).collect();
        let (snap, c1) = snapshot(&pages, 1);
        apply(&path, &snap, 0, 0).unwrap();
        let kept: u64 = (1..=2).fold(0, |a, n| a ^ page_crc(n, &pages[n as usize - 1]));
        let h = Header {
            page_size: 512,
            commit: 2,
            min_txid: 2,
            max_txid: 2,
            pre_apply: c1,
            post_apply: CHECKSUM_FLAG | kept,
            ..Default::default()
        };
        let seg = Encoder::new(h).unwrap().finish().unwrap();
        apply(&path, &seg, c1, 512).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 1024);
    }

    #[test]
    fn wrong_pre_apply_checksum_is_rejected_without_touching_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let pages: Vec<Vec<u8>> = (1..=2).map(|n| page(n, 3, 512)).collect();
        let (snap, c1) = snapshot(&pages, 1);
        apply(&path, &snap, 0, 0).unwrap();
        let before = std::fs::read(&path).unwrap();
        let h = Header {
            page_size: 512,
            commit: 2,
            min_txid: 2,
            max_txid: 2,
            pre_apply: c1,
            post_apply: CHECKSUM_FLAG | 5,
            ..Default::default()
        };
        let mut e = Encoder::new(h).unwrap();
        e.add_page(1, &page(1, 77, 512)).unwrap();
        let seg = e.finish().unwrap();
        let err = apply(&path, &seg, c1 ^ 1, 512).unwrap_err();
        assert!(matches!(err, Error::PreApplyMismatch { .. }));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn lying_trailer_is_caught_as_post_apply_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let pages: Vec<Vec<u8>> = (1..=2).map(|n| page(n, 3, 512)).collect();
        let (snap, c1) = snapshot(&pages, 1);
        apply(&path, &snap, 0, 0).unwrap();
        let h = Header {
            page_size: 512,
            commit: 2,
            min_txid: 2,
            max_txid: 2,
            pre_apply: c1,
            post_apply: CHECKSUM_FLAG | 5,
            ..Default::default()
        };
        let mut e = Encoder::new(h).unwrap();
        e.add_page(1, &page(1, 77, 512)).unwrap();
        let seg = e.finish().unwrap();
        assert!(matches!(
            apply(&path, &seg, c1, 512),
            Err(Error::PostApplyMismatch { .. })
        ));
    }

    #[test]
    fn empty_database_has_the_flag_only_checksum() {
        assert_eq!(fold(&[]), CHECKSUM_FLAG);
    }
}
