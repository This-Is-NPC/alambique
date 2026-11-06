//! Building LTX segments from the WAL and the database file.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::dbsum::{self, fold};
use crate::error::{invalid, Result};
use crate::ltx::{lock_pgno, page_crc, Encoder, Header, CHECKSUM_FLAG, HEADER_FLAG_NO_CHECKSUM};
use crate::sums::Sums;
use crate::wal::{self, WalPos};

pub struct Output {
    pub segment: Vec<u8>,
    /// WAL position after the captured frames.
    pub pos: WalPos,
    /// Per-page CRCs of the database after the segment.
    pub sums: Sums,
    pub checksum: u64,
    pub commit: u32,
}

pub struct Args<'a> {
    pub db_path: &'a Path,
    pub wal_path: &'a Path,
    pub page_size: u32,
    pub pos: WalPos,
    /// Per-page CRCs of the database before this segment.
    pub sums: &'a Sums,
    /// Database checksum before this segment. It is not folded from `sums`,
    /// because the sums of pages that were not fetched yet (lazy restore) are
    /// not known.
    pub pre: u64,
    pub txid: u64,
    pub timestamp: i64,
}

/// Captures the committed WAL frames after `args.pos` as one segment with the
/// latest version of each changed page. `None` when there is nothing new.
pub fn incremental(args: &Args) -> Result<Option<Output>> {
    if args.txid < 2 {
        return invalid("incremental segments start at txid 2; use a full segment for txid 1");
    }
    let scan = wal::scan(args.wal_path, args.page_size, Some(args.pos), None)?;
    let Some(new_commit) = scan.commit else {
        return Ok(None);
    };
    let pre = args.pre;
    let old_commit = args.sums.len() as u32;
    let lock = lock_pgno(args.page_size);

    // A copy-on-write clone: only the chunks of the changed pages are copied.
    let mut sums = args.sums.clone();
    sums.resize(new_commit as usize);
    let mut acc = pre & !CHECKSUM_FLAG;
    // Pages dropped by a shrinking commit.
    for pgno in new_commit + 1..=old_commit {
        acc ^= args.sums.get(pgno as usize - 1);
    }
    for (&pgno, data) in &scan.pages {
        let new = page_crc(pgno, data);
        acc ^= sums.get(pgno as usize - 1) ^ new;
        sums.set(pgno as usize - 1, new);
    }
    for pgno in old_commit + 1..=new_commit {
        if pgno != lock && !scan.pages.contains_key(&pgno) {
            return invalid(format!("page {pgno} is new but missing from the wal"));
        }
    }
    let checksum = CHECKSUM_FLAG | acc;

    let header = Header {
        page_size: args.page_size,
        commit: new_commit,
        min_txid: args.txid,
        max_txid: args.txid,
        timestamp: args.timestamp,
        pre_apply: pre,
        post_apply: checksum,
        flags: HEADER_FLAG_NO_CHECKSUM,
        wal_offset: scan.first_offset as i64,
        wal_size: (scan.pos.offset - scan.first_offset) as i64,
        wal_salt1: scan.pos.salt1,
        wal_salt2: scan.pos.salt2,
        ..Default::default()
    };
    let mut enc = Encoder::new(header)?;
    for (pgno, data) in &scan.pages {
        enc.add_page(*pgno, data)?;
    }
    let segment = enc.finish()?;
    Ok(Some(Output {
        segment,
        pos: scan.pos,
        sums,
        checksum,
        commit: new_commit,
    }))
}

/// A segment with no pages: the fence, and any marker that only claims a TXID.
pub fn empty(args: &Args) -> Result<Output> {
    if args.txid < 2 {
        return invalid("an empty segment cannot be txid 1; use a full segment");
    }
    let checksum = args.pre;
    let commit = args.sums.len() as u32;
    let header = Header {
        page_size: args.page_size,
        commit,
        min_txid: args.txid,
        max_txid: args.txid,
        timestamp: args.timestamp,
        pre_apply: checksum,
        post_apply: checksum,
        flags: HEADER_FLAG_NO_CHECKSUM,
        ..Default::default()
    };
    let segment = Encoder::new(header)?.finish()?;
    Ok(Output {
        segment,
        pos: args.pos,
        sums: args.sums.clone(),
        checksum,
        commit,
    })
}

/// Page `pgno` of a snapshot: the WAL version, or the database file's.
fn snapshot_page<'a>(
    scan: &'a wal::Scan,
    db: Option<&File>,
    db_pages: u32,
    pgno: u32,
    buf: &'a mut [u8],
) -> Result<&'a [u8]> {
    if let Some(d) = scan.pages.get(&pgno) {
        return Ok(d);
    }
    match db {
        Some(f) if pgno <= db_pages => {
            let ps = buf.len() as u64;
            f.read_exact_at(buf, (pgno as u64 - 1) * ps)?;
            Ok(buf)
        }
        _ => invalid(format!(
            "page {pgno} is in neither the wal nor the database file"
        )),
    }
}

/// A full snapshot as of `args.txid`: the database file overlaid with the
/// committed WAL frames before the position `wal_limit` (all of them when
/// `None`). `args.pos` and `args.sums` are ignored.
pub fn full(args: &Args, wal_limit: Option<WalPos>) -> Result<Output> {
    whole(args, wal_limit, 1)
}

/// The whole database as the segment for `args.txid`, which replaces the state
/// whose checksum is `args.pre`: applied to it, the segment gives exactly the
/// file captured here, pages beyond the new size dropped. Like a snapshot it
/// lists every page, so it is as large as the database.
pub fn replace(args: &Args) -> Result<Output> {
    if args.txid < 2 {
        return invalid("a replacing segment starts at txid 2; use a full segment for txid 1");
    }
    whole(args, None, args.txid)
}

fn whole(args: &Args, wal_limit: Option<WalPos>, min_txid: u64) -> Result<Output> {
    let scan = wal::scan(args.wal_path, args.page_size, None, wal_limit)?;
    let ps = args.page_size as u64;
    let db = match File::open(args.db_path) {
        Ok(f) => Some(f),
        // A snapshot of a cell with no file yet is empty; a replacing segment
        // of a file that is gone must not turn into an empty database.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && min_txid == 1 => None,
        Err(e) => return Err(e.into()),
    };
    let db_pages = match &db {
        Some(f) => (f.metadata()?.len() / ps) as u32,
        None => 0,
    };
    let commit = scan.commit.unwrap_or(db_pages);
    let lock = lock_pgno(args.page_size);

    // The checksum goes into the header, so the pages are read twice: once to
    // compute it and once to encode them.
    let mut buf = vec![0u8; ps as usize];
    let mut sums = Vec::with_capacity(commit as usize);
    for pgno in 1..=commit {
        sums.push(if pgno == lock {
            0
        } else {
            page_crc(
                pgno,
                snapshot_page(&scan, db.as_ref(), db_pages, pgno, &mut buf)?,
            )
        });
    }
    let checksum = fold(&sums);
    let sums = Sums::from_slice(&sums);

    let header = Header {
        page_size: args.page_size,
        commit,
        min_txid,
        max_txid: args.txid,
        timestamp: args.timestamp,
        pre_apply: if min_txid > 1 { args.pre } else { 0 },
        post_apply: checksum,
        flags: HEADER_FLAG_NO_CHECKSUM,
        ..Default::default()
    };
    let mut enc = Encoder::new(header)?;
    for pgno in (1..=commit).filter(|&p| p != lock) {
        enc.add_page(
            pgno,
            snapshot_page(&scan, db.as_ref(), db_pages, pgno, &mut buf)?,
        )?;
    }
    let segment = enc.finish()?;
    Ok(Output {
        segment,
        pos: scan.pos,
        sums,
        checksum,
        commit,
    })
}

/// True if committed frames exist after `pos`.
pub fn pending(wal_path: &Path, page_size: u32, pos: WalPos) -> Result<bool> {
    Ok(wal::scan(wal_path, page_size, Some(pos), None)?
        .commit
        .is_some())
}

/// Reads the database checksum state of a file, for starting a replicator on
/// an existing database.
pub fn file_state(path: &Path, page_size: u32) -> Result<(Vec<u64>, u64)> {
    let sums = dbsum::file_sums(path, page_size)?;
    let checksum = fold(&sums);
    Ok((sums, checksum))
}

/// Chained captures over generated SQLite-style transactions, shared by the
/// property tests and the `capture_chain` fuzz target.
#[cfg(any(test, feature = "fuzzing"))]
pub mod testing {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::error::Error;
    use crate::ltx;
    use crate::merge::merge;
    use crate::wal::testing::WalBuilder;

    pub const PS: u32 = 512;

    /// A transaction: the database size after it and the pages it rewrites
    /// (see `WalBuilder::transaction`).
    #[derive(Clone, Debug)]
    pub struct Tx {
        pub size: u32,
        pub writes: Vec<(u32, u8)>,
    }

    /// One captured segment: the transactions that arrived since the previous
    /// capture, or `None` for a fence (an empty segment that claims a TXID).
    pub type Step = Option<Vec<Tx>>;

    /// Segments captured back to back, each fed the previous output and none
    /// uploaded or applied before the next is encoded.
    pub struct Chain {
        dir: PathBuf,
        start: PathBuf,
        /// Checksum of the database the chain starts from (TXID 1).
        pub start_checksum: u64,
        /// Segment `i` has TXID `i + 2`.
        pub segments: Vec<Vec<u8>>,
        pub fence: Vec<bool>,
        /// The database after the last transaction.
        pub pages: Vec<Vec<u8>>,
        pub checksum: u64,
        pub sums: Sums,
    }

    /// How `apply` refuses a segment: the pre-apply checksum does not follow,
    /// or (checked after it) the database lacks the pages the segment grows
    /// from, or (checked last) a skipped, checksum-neutral segment left pages
    /// that the post-apply checksum exposes.
    fn refused(e: &Error) -> bool {
        matches!(
            e,
            Error::PreApplyMismatch { .. } | Error::PostApplyMismatch { .. } | Error::Invalid(_)
        )
    }

    impl Chain {
        /// Logs `initial`, takes the snapshot (TXID 1), then captures one
        /// segment per step. `tail` bytes of an uncommitted frame trail the
        /// WAL after every step, like a write in progress. `dir` must exist
        /// and be empty.
        pub fn build(dir: &Path, initial: &[Tx], steps: &[Step], tail: usize) -> Chain {
            let wal_path = dir.join("db-wal");
            let mut wal = WalBuilder::new(PS, (1, 2), false);
            let mut pages = Vec::new();
            for tx in initial {
                wal.transaction(&mut pages, tx.size, &tx.writes);
            }
            wal.write(&wal_path);
            let db_path = dir.join("db");
            fn args<'a>(
                (db_path, wal_path): (&'a Path, &'a Path),
                pos: WalPos,
                sums: &'a Sums,
                pre: u64,
                txid: u64,
            ) -> Args<'a> {
                Args {
                    db_path,
                    wal_path,
                    page_size: PS,
                    pos,
                    sums,
                    pre,
                    txid,
                    timestamp: 1000 + txid as i64,
                }
            }
            let files = (db_path.as_path(), wal_path.as_path());
            let snap = full(
                &args(files, WalPos::default(), &Sums::default(), 0, 1),
                None,
            )
            .expect("snapshot");
            let start = dir.join("start");
            dbsum::apply(&start, &snap.segment, 0, 0).expect("apply snapshot");

            let (mut pos, mut sums, mut pre) = (snap.pos, snap.sums, snap.checksum);
            let (mut segments, mut fence) = (Vec::new(), Vec::new());
            for (i, step) in steps.iter().enumerate() {
                let args = args(files, pos, &sums, pre, i as u64 + 2);
                let out = match step {
                    None => empty(&args).expect("fence"),
                    Some(txs) => {
                        for tx in txs {
                            wal.transaction(&mut pages, tx.size, &tx.writes);
                        }
                        wal.write_with_tail(&wal_path, tail);
                        incremental(&args).expect("capture").expect("commits")
                    }
                };
                assert_eq!(out.checksum, fold(&out.sums.to_vec()));
                segments.push(out.segment);
                fence.push(step.is_none());
                (pos, sums, pre) = (out.pos, out.sums, out.checksum);
            }
            Chain {
                dir: dir.to_path_buf(),
                start,
                start_checksum: snap.checksum,
                segments,
                fence,
                pages,
                checksum: pre,
                sums,
            }
        }

        /// A copy of the starting database.
        fn fresh(&self, name: &str) -> PathBuf {
            let path = self.dir.join(name);
            // Not `fs::copy`: Miri has no copy_file_range.
            std::fs::write(&path, std::fs::read(&self.start).expect("read")).expect("copy");
            path
        }

        /// Applies segments in `order` to a copy of the starting database.
        /// The error carries the position in `order` that failed.
        pub fn apply_order(
            &self,
            name: &str,
            order: &[usize],
        ) -> std::result::Result<PathBuf, (usize, Error)> {
            let path = self.fresh(name);
            let mut current = self.start_checksum;
            for (at, &i) in order.iter().enumerate() {
                current = dbsum::apply(&path, &self.segments[i], current, PS)
                    .map_err(|e| (at, e))?
                    .post_apply;
            }
            Ok(path)
        }

        fn header(&self, i: usize) -> (ltx::Header, ltx::Trailer, bool) {
            let (h, pages, t) = ltx::decode_all(&self.segments[i]).expect("decode");
            (h, t, pages.is_empty())
        }

        /// Whether segment `i` leaves the checksum as it was.
        fn neutral(&self, i: usize) -> bool {
            let (h, t, _) = self.header(i);
            h.pre_apply == t.post_apply
        }

        /// TXIDs follow each other, checksums chain, and applying the
        /// segments in order gives the database after the last transaction.
        pub fn check_chain(&self) {
            let mut prev = self.start_checksum;
            for i in 0..self.segments.len() {
                let (h, t, no_pages) = self.header(i);
                assert_eq!((h.min_txid, h.max_txid), (i as u64 + 2, i as u64 + 2));
                assert_eq!(h.pre_apply, prev, "segment {i} pre-apply");
                if self.fence[i] {
                    assert!(no_pages && h.pre_apply == t.post_apply);
                }
                prev = t.post_apply;
            }
            assert_eq!(prev, self.checksum);

            let order: Vec<usize> = (0..self.segments.len()).collect();
            let applied = self.apply_order("applied", &order).expect("chain applies");
            let bytes = std::fs::read(&applied).unwrap();
            assert_eq!(bytes, self.pages.concat());
            let sums = dbsum::file_sums(&applied, PS).unwrap();
            assert_eq!(sums, self.sums.to_vec());
            assert_eq!(fold(&sums), self.checksum);

            // One capture of the whole WAL gives the same database.
            let args = Args {
                db_path: &self.dir.join("db"),
                wal_path: &self.dir.join("db-wal"),
                page_size: PS,
                pos: WalPos::default(),
                sums: &Sums::default(),
                pre: 0,
                txid: 1,
                timestamp: 1,
            };
            let whole = full(&args, None).unwrap();
            assert_eq!(whole.checksum, self.checksum);

            // The merged chain is the same step.
            if !self.segments.is_empty() {
                let all: Vec<&[u8]> = self.segments.iter().map(|s| &s[..]).collect();
                let merged = merge(&all).unwrap();
                let path = self.fresh("merged");
                dbsum::apply(&path, &merged, self.start_checksum, PS).unwrap();
                assert_eq!(std::fs::read(&path).unwrap(), bytes);
            }
        }

        /// Segments applied out of order, twice or with one missing are
        /// refused where the checksums can tell, and `merge` refuses all of
        /// them. A segment that leaves the checksum as it was (a fence, or
        /// a rewrite of identical bytes) is invisible to the checksums:
        /// `dbsum::apply` does not look at TXIDs, so only `merge` (and the
        /// restore that checks `min_txid == txid + 1` in Elixir) notices it
        /// missing.
        pub fn check_misuse(&self) {
            let n = self.segments.len();
            let all: Vec<usize> = (0..n).collect();
            let merged = |order: &[usize]| {
                let inputs: Vec<&[u8]> = order.iter().map(|&i| &self.segments[i][..]).collect();
                merge(&inputs)
            };
            for i in 0..n {
                // Segment i missing.
                let order: Vec<usize> = all.iter().copied().filter(|&j| j != i).collect();
                match self.apply_order("missing", &order) {
                    Err((at, e)) => {
                        assert!(refused(&e), "missing {i}: {e:?}");
                        assert!(self.neutral(i) || at == i, "missing {i}: {e:?} at {at}");
                    }
                    Ok(path) => {
                        assert!(self.neutral(i) || i + 1 == n, "missing {i} went through");
                        if self.fence[i] {
                            assert_eq!(std::fs::read(path).unwrap(), self.pages.concat());
                        }
                    }
                }
                if i > 0 && i + 1 < n {
                    assert!(merged(&order).is_err(), "merge skipped {i}");
                }

                // Segment i twice in a row.
                let mut order = all.clone();
                order.insert(i, i);
                match self.apply_order("twice", &order) {
                    Err((at, e)) => {
                        assert!(refused(&e), "twice {i}: {e:?}");
                        assert!(!self.neutral(i) && at == i + 1, "twice {i}: {e:?} at {at}");
                    }
                    Ok(_) => assert!(self.neutral(i)),
                }
                assert!(merged(&order).is_err());

                // Segments i and i + 1 swapped.
                if i + 1 < n {
                    let mut order = all.clone();
                    order.swap(i, i + 1);
                    match self.apply_order("swapped", &order) {
                        Err((at, e)) => {
                            assert!(refused(&e), "swapped {i}: {e:?}");
                            assert!(self.neutral(i) || at == i, "swapped {i}: {e:?} at {at}");
                        }
                        Ok(_) => assert!(self.neutral(i) && self.neutral(i + 1)),
                    }
                    assert!(merged(&order).is_err());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ltx;
    use crate::wal::testing::WalBuilder;

    const PS: u32 = 512;

    fn pg(fill: u8) -> Vec<u8> {
        vec![fill; PS as usize]
    }

    /// The database and WAL files of a test directory.
    struct Paths {
        db: std::path::PathBuf,
        wal: std::path::PathBuf,
    }

    impl Paths {
        fn new(dir: &Path) -> Paths {
            Paths {
                db: dir.join("db"),
                wal: dir.join("db-wal"),
            }
        }

        fn args<'a>(&'a self, pos: WalPos, sums: &'a Sums, txid: u64) -> Args<'a> {
            Args {
                db_path: &self.db,
                wal_path: &self.wal,
                page_size: PS,
                pos,
                sums,
                pre: fold(&sums.to_vec()),
                txid,
                timestamp: 1000 + txid as i64,
            }
        }
    }

    #[test]
    fn full_then_incremental_segments_rebuild_the_wal_state() {
        let dir = tempfile::tempdir().unwrap();
        let p = Paths::new(dir.path());
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 0, &pg(1)).frame(2, 2, &pg(2));
        b.write(&dir.path().join("db-wal"));

        let snap = full(&p.args(WalPos::default(), &Sums::default(), 1), None).unwrap();
        assert_eq!(snap.commit, 2);
        assert_eq!(snap.checksum, fold(&snap.sums.to_vec()));

        b.frame(2, 0, &pg(3)).frame(3, 3, &pg(4));
        b.write(&dir.path().join("db-wal"));
        let inc = incremental(&p.args(snap.pos, &snap.sums, 2))
            .unwrap()
            .unwrap();
        assert_eq!(inc.commit, 3);
        let (h, pages, t) = ltx::decode_all(&inc.segment).unwrap();
        assert_eq!((h.min_txid, h.max_txid, h.pre_apply), (2, 2, snap.checksum));
        assert_eq!(pages.iter().map(|p| p.0).collect::<Vec<_>>(), vec![2, 3]);
        assert_eq!(t.post_apply, inc.checksum);

        // Apply the chain to a file and compare with a direct checksum.
        let out = dir.path().join("restored");
        let a = dbsum::apply(&out, &snap.segment, 0, 0).unwrap();
        let b2 = dbsum::apply(&out, &inc.segment, a.post_apply, PS).unwrap();
        assert_eq!(b2.post_apply, inc.checksum);
        assert_eq!(fold(&dbsum::file_sums(&out, PS).unwrap()), inc.checksum);
        assert_eq!(std::fs::read(&out).unwrap(), [pg(1), pg(3), pg(4)].concat());

        assert!(incremental(&p.args(inc.pos, &inc.sums, 3))
            .unwrap()
            .is_none());
    }

    #[test]
    fn replace_turns_one_state_into_another_and_drops_extra_pages() {
        let dir = tempfile::tempdir().unwrap();
        let p = Paths::new(dir.path());
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 0, &pg(1))
            .frame(2, 0, &pg(2))
            .frame(3, 3, &pg(3));
        b.write(&dir.path().join("db-wal"));
        let old = full(&p.args(WalPos::default(), &Sums::default(), 1), None).unwrap();

        // The new state is smaller and shares no page with the old one.
        std::fs::remove_file(dir.path().join("db-wal")).unwrap();
        std::fs::write(dir.path().join("db"), [pg(7), pg(8)].concat()).unwrap();
        let none = Sums::default();
        let mut args = p.args(WalPos::default(), &none, 2);
        args.pre = old.checksum;
        let new = replace(&args).unwrap();
        assert_eq!(
            (new.commit, new.checksum),
            (2, fold(&[page_crc(1, &pg(7)), page_crc(2, &pg(8))]))
        );
        let (h, pages, t) = ltx::decode_all(&new.segment).unwrap();
        assert_eq!(
            (h.min_txid, h.max_txid, h.pre_apply, h.commit),
            (2, 2, old.checksum, 2)
        );
        assert_eq!(pages.len(), 2);
        assert_eq!(t.post_apply, new.checksum);

        let out = dir.path().join("restored");
        let a = dbsum::apply(&out, &old.segment, 0, 0).unwrap();
        let b2 = dbsum::apply(&out, &new.segment, a.post_apply, PS).unwrap();
        assert_eq!(b2.post_apply, new.checksum);
        assert_eq!(std::fs::read(&out).unwrap(), [pg(7), pg(8)].concat());

        // Applied to any other state it is refused.
        let other = dir.path().join("other");
        let wrong = dbsum::apply(&other, &old.segment, 0, 0).unwrap().post_apply ^ 1;
        assert!(dbsum::apply(&other, &new.segment, wrong, PS).is_err());
        assert!(replace(&p.args(WalPos::default(), &Sums::default(), 1)).is_err());
    }

    #[test]
    fn replace_refuses_a_missing_database_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = Paths::new(dir.path());
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 1, &pg(1));
        b.write(&dir.path().join("db-wal"));

        let none = Sums::default();
        let mut args = p.args(WalPos::default(), &none, 2);
        args.pre = CHECKSUM_FLAG;
        assert!(replace(&args).is_err());
        // A snapshot still reads a cell whose pages are only in the WAL.
        assert_eq!(
            full(&p.args(WalPos::default(), &Sums::default(), 1), None)
                .unwrap()
                .commit,
            1
        );
    }

    #[test]
    fn shrinking_the_database_is_captured() {
        let dir = tempfile::tempdir().unwrap();
        let p = Paths::new(dir.path());
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 0, &pg(1))
            .frame(2, 0, &pg(2))
            .frame(3, 3, &pg(3));
        b.write(&dir.path().join("db-wal"));
        let snap = full(&p.args(WalPos::default(), &Sums::default(), 1), None).unwrap();
        b.frame(1, 1, &pg(9));
        b.write(&dir.path().join("db-wal"));
        let inc = incremental(&p.args(snap.pos, &snap.sums, 2))
            .unwrap()
            .unwrap();
        assert_eq!(inc.commit, 1);
        assert_eq!(inc.sums.len(), 1);
        assert_eq!(inc.checksum, fold(&[page_crc(1, &pg(9))]));
        let out = dir.path().join("restored");
        let a = dbsum::apply(&out, &snap.segment, 0, 0).unwrap();
        dbsum::apply(&out, &inc.segment, a.post_apply, PS).unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), pg(9));
    }

    #[test]
    fn after_a_checkpoint_the_database_file_supplies_the_old_pages() {
        let dir = tempfile::tempdir().unwrap();
        let p = Paths::new(dir.path());
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 0, &pg(1)).frame(2, 2, &pg(2));
        b.write(&dir.path().join("db-wal"));
        let snap = full(&p.args(WalPos::default(), &Sums::default(), 1), None).unwrap();

        // Checkpoint: pages move to the db file, the WAL restarts with new salts.
        std::fs::write(dir.path().join("db"), [pg(1), pg(2)].concat()).unwrap();
        let mut b2 = WalBuilder::new(PS, (2, 99), false);
        b2.frame(2, 2, &pg(7));
        b2.write(&dir.path().join("db-wal"));
        let inc = incremental(&p.args(snap.pos, &snap.sums, 2))
            .unwrap()
            .unwrap();
        let (h, _, _) = ltx::decode_all(&inc.segment).unwrap();
        assert_eq!(h.wal_offset, 32);

        // A snapshot at txid 2 includes db file pages plus the WAL.
        let s2 = full(&p.args(WalPos::default(), &Sums::default(), 2), None).unwrap();
        assert_eq!(s2.checksum, inc.checksum);
    }

    #[test]
    fn full_snapshot_limited_to_the_replicated_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let p = Paths::new(dir.path());
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 1, &pg(1));
        let limit = WalPos {
            offset: b.bytes.len() as u64,
            salt1: 1,
            salt2: 2,
            ..Default::default()
        };
        b.frame(1, 1, &pg(2));
        b.write(&dir.path().join("db-wal"));
        let s = full(&p.args(WalPos::default(), &Sums::default(), 1), Some(limit)).unwrap();
        assert_eq!(s.checksum, fold(&[page_crc(1, &pg(1))]));
    }

    #[test]
    fn full_with_no_wal_reads_the_database_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = Paths::new(dir.path());
        std::fs::write(dir.path().join("db"), [pg(4), pg(5)].concat()).unwrap();
        let s = full(&p.args(WalPos::default(), &Sums::default(), 1), None).unwrap();
        assert_eq!(s.commit, 2);
        let empty = tempfile::tempdir().unwrap();
        let q = Paths::new(empty.path());
        let z = full(&q.args(WalPos::default(), &Sums::default(), 1), None).unwrap();
        assert_eq!((z.commit, z.checksum), (0, CHECKSUM_FLAG));
        ltx::decode_all(&z.segment).unwrap();
    }

    #[test]
    fn an_incremental_capture_copies_only_the_changed_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let p = Paths::new(dir.path());
        let pages: u32 = 100_000;
        let old = Sums::from_slice(&(1..=pages as u64).collect::<Vec<_>>());
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(5, pages, &pg(5));
        b.write(&dir.path().join("db-wal"));
        let inc = incremental(&p.args(WalPos::default(), &old, 2))
            .unwrap()
            .unwrap();
        let chunks = (pages as usize).div_ceil(256);
        assert_eq!(inc.sums.shared_chunks(&old), chunks - 1);
        assert_eq!(old.get(4), 5);
        assert_eq!(inc.sums.get(4), page_crc(5, &pg(5)));
        assert_eq!(inc.checksum, fold(&old.to_vec()) ^ 5 ^ page_crc(5, &pg(5)));
    }

    #[test]
    fn empty_segment_keeps_the_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let p = Paths::new(dir.path());
        let sums = Sums::from_slice(&[page_crc(1, &pg(1))]);
        let e = empty(&p.args(WalPos::default(), &sums, 5)).unwrap();
        let (h, pages, t) = ltx::decode_all(&e.segment).unwrap();
        assert!(pages.is_empty());
        assert_eq!(
            (h.pre_apply, t.post_apply, h.commit),
            (fold(&sums.to_vec()), fold(&sums.to_vec()), 1)
        );
    }

    use super::testing::{Chain, Step, Tx};
    use proptest::prelude::*;

    fn tx() -> impl Strategy<Value = Tx> {
        (
            1u32..10,
            prop::collection::vec((0u32..16, any::<u8>()), 0..5),
        )
            .prop_map(|(size, writes)| Tx { size, writes })
    }

    /// A fence now and then, otherwise a batch of one to three transactions.
    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            1 => Just(None),
            4 => prop::collection::vec(tx(), 1..4).prop_map(Some),
        ]
    }

    fn build(dir: &Path, initial: &[Tx], steps: &[Step], tail: usize) -> Chain {
        Chain::build(dir, initial, steps, tail)
    }

    fn fixed() -> (Vec<Tx>, Vec<Step>) {
        let tx = |size, writes: &[(u32, u8)]| Tx {
            size,
            writes: writes.to_vec(),
        };
        (
            vec![tx(3, &[(0, 1), (1, 2)])],
            vec![
                Some(vec![tx(5, &[(4, 9)]), tx(5, &[(0, 7)])]),
                None,
                Some(vec![tx(2, &[])]),
                Some(vec![tx(4, &[(2, 5)])]),
                None,
                None,
                Some(vec![tx(4, &[])]),
            ],
        )
    }

    #[test]
    fn chained_captures_apply_in_order_and_refuse_misuse() {
        let dir = tempfile::tempdir().unwrap();
        let (initial, steps) = fixed();
        let chain = build(dir.path(), &initial, &steps, 100);
        chain.check_chain();
        chain.check_misuse();
    }

    #[test]
    fn a_missing_fence_is_invisible_to_apply_and_caught_by_merge() {
        let dir = tempfile::tempdir().unwrap();
        let (initial, steps) = fixed();
        let chain = build(dir.path(), &initial, &steps, 0);
        // Segment 1 is a fence: the checksums cannot see it missing.
        assert!(chain.fence[1]);
        let order = [0, 2, 3, 4, 5, 6];
        let out = chain.apply_order("skipped", &order).unwrap();
        assert_eq!(std::fs::read(out).unwrap(), chain.pages.concat());
        let inputs: Vec<&[u8]> = order.iter().map(|&i| &chain.segments[i][..]).collect();
        assert!(crate::merge::merge(&inputs).is_err());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Chained captures of random transaction sequences: contiguous
        /// TXIDs, a checksum chain, and the last transaction's database.
        #[test]
        #[cfg_attr(miri, ignore = "proptest is too slow under Miri")]
        fn chained_captures_reproduce_the_database(
            initial in prop::collection::vec(tx(), 1..3),
            steps in prop::collection::vec(step(), 1..8),
            tail in 0usize..600,
        ) {
            let dir = tempfile::tempdir().unwrap();
            build(dir.path(), &initial, &steps, tail).check_chain();
        }

        /// A segment that is missing, repeated or out of order is refused.
        #[test]
        #[cfg_attr(miri, ignore = "proptest is too slow under Miri")]
        fn misordered_chains_are_refused(
            initial in prop::collection::vec(tx(), 1..3),
            steps in prop::collection::vec(step(), 2..8),
        ) {
            let dir = tempfile::tempdir().unwrap();
            build(dir.path(), &initial, &steps, 0).check_misuse();
        }

        /// A segment is a function of its inputs: the same bytes come out
        /// when the chain is captured again, whatever an unfinished write
        /// leaves after the committed frames.
        #[test]
        #[cfg_attr(miri, ignore = "proptest is too slow under Miri")]
        fn segments_are_deterministic_and_ignore_a_torn_tail(
            initial in prop::collection::vec(tx(), 1..3),
            steps in prop::collection::vec(step(), 1..6),
            tail in 1usize..600,
        ) {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let first = build(a.path(), &initial, &steps, 0);
            let second = build(b.path(), &initial, &steps, tail);
            prop_assert_eq!(&first.segments, &second.segments);
            prop_assert_eq!(first.checksum, second.checksum);
        }
    }

    #[test]
    fn new_page_missing_from_the_wal_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let p = Paths::new(dir.path());
        let mut b = WalBuilder::new(PS, (1, 2), false);
        b.frame(1, 3, &pg(1)); // claims 3 pages, writes only page 1
        b.write(&dir.path().join("db-wal"));
        assert!(incremental(&p.args(WalPos::default(), &Sums::default(), 2)).is_err());
    }
}
