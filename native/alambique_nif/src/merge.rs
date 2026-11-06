//! Merging consecutive LTX files into one (compaction, design section 9).

use std::collections::BTreeMap;

use crate::error::{invalid, Result};
use crate::ltx::{self, Encoder, Frame, Header, HEADER_FLAG_NO_CHECKSUM};

/// Merges consecutive files (ascending TXID order) into one file covering the
/// whole range. Each page comes from the latest input that has it; pages past
/// the final database size are dropped.
///
/// The inputs must follow each other exactly: each starts at the TXID after
/// the previous one and its pre-apply checksum equals the previous post-apply
/// checksum. Every input is verified. Memory use is the inputs, the output and
/// one page per frame index entry, never the decompressed database: pages are
/// decompressed one at a time to verify and to checksum them, and the output
/// reuses the inputs' compressed blocks.
pub fn merge(inputs: &[&[u8]]) -> Result<Vec<u8>> {
    if inputs.is_empty() {
        return invalid("at least one input required");
    }
    let mut latest: BTreeMap<u32, (usize, Frame)> = BTreeMap::new();
    let mut headers: Vec<Header> = Vec::with_capacity(inputs.len());
    let mut last_post = 0;
    for (i, data) in inputs.iter().enumerate() {
        let (header, trailer, frames) = ltx::verify(data)?;
        if let Some(prev) = headers.last() {
            if prev.page_size != header.page_size {
                return invalid("mismatched page sizes");
            }
            if prev.max_txid.checked_add(1) != Some(header.min_txid) {
                return invalid("non-contiguous transaction ids");
            }
            if header.pre_apply != last_post {
                return invalid("pre-apply checksum does not follow the previous input");
            }
        }
        for frame in frames {
            latest.insert(frame.0, (i, frame));
        }
        last_post = trailer.post_apply;
        headers.push(header);
    }
    let (first, last) = (&headers[0], &headers[headers.len() - 1]);
    // A page that no input has would be a hole in a merged snapshot.
    let lock = ltx::lock_pgno(first.page_size);
    if first.is_snapshot()
        && latest.range(..=last.commit).count() as u64 != ltx::pages_added(0, last.commit, lock)
    {
        return invalid("inputs leave pages of the final database out");
    }
    let out = Header {
        page_size: first.page_size,
        commit: last.commit,
        min_txid: first.min_txid,
        max_txid: last.max_txid,
        timestamp: last.timestamp,
        pre_apply: first.pre_apply,
        post_apply: last_post,
        flags: HEADER_FLAG_NO_CHECKSUM,
        ..Default::default()
    };
    let mut enc = Encoder::new(out)?;
    let mut page = vec![0u8; first.page_size as usize];
    for (pgno, (i, frame)) in latest.range(..=last.commit) {
        let payload = ltx::frame_payload(inputs[*i], frame);
        let got = lz4_flex::block::decompress_into(payload, &mut page)
            .map_err(|e| crate::error::Error::Invalid(format!("decompress page {pgno}: {e}")))?;
        if got != page.len() {
            return invalid(format!("page {pgno} decompressed to {got} bytes"));
        }
        enc.add_compressed(*pgno, &page, payload)?;
    }
    enc.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbsum;
    use crate::ltx::{page_crc, CHECKSUM_FLAG};

    fn pg(n: u32, fill: u8) -> Vec<u8> {
        vec![fill.wrapping_add(n as u8); 512]
    }

    fn seg(
        min: u64,
        pre: u64,
        commit: u32,
        pages: &[(u32, Vec<u8>)],
        post: u64,
        ts: i64,
    ) -> Vec<u8> {
        let h = Header {
            page_size: 512,
            commit,
            min_txid: min,
            max_txid: min,
            timestamp: ts,
            pre_apply: pre,
            post_apply: post,
            ..Default::default()
        };
        let mut e = Encoder::new(h).unwrap();
        for (n, d) in pages {
            e.add_page(*n, d).unwrap();
        }
        e.finish().unwrap()
    }

    #[test]
    fn merged_file_applies_like_the_chain() {
        let dir = tempfile::tempdir().unwrap();
        let chain = dir.path().join("chain");
        let merged = dir.path().join("merged");
        let sum = |pages: &[Vec<u8>]| -> u64 {
            CHECKSUM_FLAG
                | pages
                    .iter()
                    .enumerate()
                    .fold(0, |a, (i, p)| a ^ page_crc(i as u32 + 1, p))
        };

        let mut db: Vec<Vec<u8>> = (1..=3).map(|n| pg(n, 1)).collect();
        let c1 = sum(&db);
        let s1 = seg(
            1,
            0,
            3,
            &[(1, db[0].clone()), (2, db[1].clone()), (3, db[2].clone())],
            c1,
            10,
        );
        db[1] = pg(2, 50);
        db.push(pg(4, 7));
        let c2 = sum(&db);
        let s2 = seg(2, c1, 4, &[(2, db[1].clone()), (4, db[3].clone())], c2, 20);
        db[1] = pg(2, 90);
        db.truncate(2);
        let c3 = sum(&db);
        let s3 = seg(3, c2, 2, &[(2, db[1].clone())], c3, 30);

        let m = merge(&[&s1, &s2, &s3]).unwrap();
        let (h, t) = ltx::peek(&m).unwrap();
        assert_eq!(
            (h.min_txid, h.max_txid, h.commit, h.timestamp),
            (1, 3, 2, 30)
        );
        assert_eq!(t.post_apply, c3);
        assert!(h.is_snapshot());

        let mut cur = 0;
        for s in [&s1, &s2, &s3] {
            cur = dbsum::apply(&chain, s, cur, 512).unwrap().post_apply;
        }
        dbsum::apply(&merged, &m, 0, 512).unwrap();
        assert_eq!(
            std::fs::read(&chain).unwrap(),
            std::fs::read(&merged).unwrap()
        );
        assert_eq!(cur, c3);
    }

    #[test]
    fn merge_of_incrementals_keeps_pre_apply_of_the_first() {
        let a = seg(
            5,
            CHECKSUM_FLAG | 1,
            2,
            &[(1, pg(1, 1))],
            CHECKSUM_FLAG | 2,
            1,
        );
        let b = seg(
            6,
            CHECKSUM_FLAG | 2,
            2,
            &[(2, pg(2, 1))],
            CHECKSUM_FLAG | 3,
            2,
        );
        let m = merge(&[&a, &b]).unwrap();
        let (h, _) = ltx::peek(&m).unwrap();
        assert_eq!(
            (h.min_txid, h.max_txid, h.pre_apply),
            (5, 6, CHECKSUM_FLAG | 1)
        );
        ltx::decode_all(&m).unwrap();
    }

    #[test]
    fn rejects_gaps_and_empty_input() {
        let a = seg(
            5,
            CHECKSUM_FLAG | 1,
            2,
            &[(1, pg(1, 1))],
            CHECKSUM_FLAG | 2,
            1,
        );
        let c = seg(
            7,
            CHECKSUM_FLAG | 3,
            2,
            &[(1, pg(1, 2))],
            CHECKSUM_FLAG | 4,
            2,
        );
        assert!(merge(&[&a, &c]).is_err());
        assert!(merge(&[]).is_err());
    }

    #[test]
    fn rejects_inputs_that_grow_the_database_without_its_pages() {
        let a = seg(
            1,
            0,
            2,
            &[(1, pg(1, 1)), (2, pg(2, 1))],
            CHECKSUM_FLAG | 1,
            1,
        );
        let b = seg(
            2,
            CHECKSUM_FLAG | 1,
            4,
            &[(4, pg(4, 1))],
            CHECKSUM_FLAG | 2,
            2,
        );
        assert!(merge(&[&a, &b]).is_err());
    }

    #[test]
    fn rejects_a_file_after_the_last_possible_transaction() {
        let a = seg(u64::MAX, CHECKSUM_FLAG | 1, 1, &[], CHECKSUM_FLAG | 2, 1);
        let b = seg(2, CHECKSUM_FLAG | 2, 1, &[], CHECKSUM_FLAG | 3, 2);
        assert!(merge(&[&a, &b]).is_err());
    }

    #[test]
    fn rejects_inputs_that_do_not_follow_each_other() {
        let a = seg(
            5,
            CHECKSUM_FLAG | 1,
            2,
            &[(1, pg(1, 1))],
            CHECKSUM_FLAG | 2,
            1,
        );
        let wrong_pre = seg(
            6,
            CHECKSUM_FLAG | 9,
            2,
            &[(2, pg(2, 1))],
            CHECKSUM_FLAG | 3,
            2,
        );
        let overlap = seg(
            5,
            CHECKSUM_FLAG | 2,
            2,
            &[(2, pg(2, 1))],
            CHECKSUM_FLAG | 3,
            2,
        );
        assert!(merge(&[&a, &wrong_pre]).is_err());
        assert!(merge(&[&a, &overlap]).is_err());
    }

    #[test]
    fn rejects_a_corrupt_input() {
        let a = seg(
            5,
            CHECKSUM_FLAG | 1,
            2,
            &[(1, pg(1, 1))],
            CHECKSUM_FLAG | 2,
            1,
        );
        let mut bad = a.clone();
        let mid = bad.len() / 2;
        bad[mid] ^= 1;
        assert!(merge(&[&bad]).is_err());
    }
}

#[cfg(test)]
mod props {
    use super::*;
    use crate::dbsum;
    use crate::ltx::{page_crc, CHECKSUM_FLAG};
    use proptest::prelude::*;

    const PS: usize = 512;

    /// One transaction: the database size afterwards and `(page, fill)` writes.
    type Tx = (u32, Vec<(u32, u8)>);

    fn chain_spec() -> impl Strategy<Value = Vec<Tx>> {
        prop::collection::vec(
            (1u32..7, prop::collection::vec((0u32..8, any::<u8>()), 0..4)),
            1..7,
        )
    }

    fn sum(pages: &[Vec<u8>]) -> u64 {
        CHECKSUM_FLAG
            | pages
                .iter()
                .enumerate()
                .fold(0, |a, (i, p)| a ^ page_crc(i as u32 + 1, p))
    }

    /// Consecutive LTX files for the transactions (the first is a snapshot),
    /// the database after each, and its checksums.
    fn build(spec: &[Tx]) -> (Vec<Vec<u8>>, Vec<Vec<Vec<u8>>>) {
        let mut db: Vec<Vec<u8>> = Vec::new();
        let (mut files, mut states) = (Vec::new(), Vec::new());
        let mut pre = 0;
        for (n, (size, writes)) in spec.iter().enumerate() {
            let old = db.len();
            db.resize(*size as usize, vec![0; PS]);
            let mut touched = std::collections::BTreeSet::new();
            if n == 0 {
                touched.extend(1..=*size);
            }
            touched.extend(old as u32 + 1..=*size);
            for &(pgno, fill) in writes {
                let pgno = pgno % size + 1;
                db[pgno as usize - 1] = vec![fill; PS];
                touched.insert(pgno);
            }
            let post = sum(&db);
            let h = Header {
                page_size: PS as u32,
                commit: *size,
                min_txid: n as u64 + 1,
                max_txid: n as u64 + 1,
                timestamp: n as i64,
                pre_apply: pre,
                post_apply: post,
                ..Default::default()
            };
            let mut e = Encoder::new(h).unwrap();
            for p in touched {
                e.add_page(p, &db[p as usize - 1]).unwrap();
            }
            files.push(e.finish().unwrap());
            states.push(db.clone());
            pre = post;
        }
        (files, states)
    }

    fn refs(files: &[Vec<u8>]) -> Vec<&[u8]> {
        files.iter().map(|f| &f[..]).collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// A merged file applies to the same database as the chain.
        #[test]
        #[cfg_attr(miri, ignore = "proptest is too slow under Miri")]
        fn merging_a_chain_equals_applying_it_in_order(spec in chain_spec()) {
            let (files, states) = build(&spec);
            let dir = tempfile::tempdir().unwrap();
            let (chain, merged) = (dir.path().join("chain"), dir.path().join("merged"));
            let mut cur = 0;
            for f in &files {
                cur = dbsum::apply(&chain, f, cur, PS as u32).unwrap().post_apply;
            }
            let m = merge(&refs(&files)).unwrap();
            let applied = dbsum::apply(&merged, &m, 0, PS as u32).unwrap();
            let last = states.last().unwrap();
            prop_assert_eq!(std::fs::read(&merged).unwrap(), last.concat());
            prop_assert_eq!(std::fs::read(&chain).unwrap(), last.concat());
            prop_assert_eq!((applied.post_apply, cur), (sum(last), sum(last)));
        }

        /// Merging adjacent groups first gives the same file.
        #[test]
        #[cfg_attr(miri, ignore = "proptest is too slow under Miri")]
        fn merge_is_associative_over_adjacent_segments(
            spec in chain_spec(),
            cuts in (any::<prop::sample::Index>(), any::<prop::sample::Index>()),
        ) {
            let (files, _) = build(&spec);
            let n = files.len();
            let (mut i, mut j) = (cuts.0.index(n + 1), cuts.1.index(n + 1));
            if i > j {
                std::mem::swap(&mut i, &mut j);
            }
            let groups: Vec<Vec<u8>> = [&files[..i], &files[i..j], &files[j..]]
                .iter()
                .filter(|g| !g.is_empty())
                .map(|g| merge(&refs(g)).unwrap())
                .collect();
            prop_assert_eq!(merge(&refs(&groups)).unwrap(), merge(&refs(&files)).unwrap());
        }
    }
}
