//! Entry points of the fuzz targets (`fuzz/`), kept in the crate to reach its
//! private modules. Each takes arbitrary bytes, must never panic, and checks
//! the properties that hold for every input. The unit tests replay the seed
//! corpus through them, so they also run under Miri.

use std::path::PathBuf;

use crate::capture::testing::{Chain, Step, Tx};
use crate::dbsum::{self, fold};
use crate::ltx::{
    self, page_crc, Encoder, Header, CHECKSUM_FLAG, HEADER_FLAG_NO_CHECKSUM, HEADER_SIZE,
};
use crate::{lazy, merge, wal};

/// Largest database a harness applies a file to; bigger commits would make
/// huge sparse files.
const MAX_DB_BYTES: u64 = 1 << 20;

/// A scratch file of this thread (`TMPDIR` is the caller's to clean up).
fn scratch(name: &str) -> PathBuf {
    let thread = format!("{:?}", std::thread::current().id()).replace(['(', ')'], "");
    std::env::temp_dir().join(format!(
        "alambique-fuzz-{}-{thread}-{name}",
        std::process::id()
    ))
}

/// Reads bytes from the input; zeros once it runs out.
struct Input<'a>(&'a [u8]);

impl Input<'_> {
    fn u8(&mut self) -> u8 {
        let (b, rest) = self.0.split_first().map_or((0, self.0), |(b, r)| (*b, r));
        self.0 = rest;
        b
    }

    fn u16(&mut self) -> u16 {
        u16::from_be_bytes([self.u8(), self.u8()])
    }

    fn take(&mut self, n: usize) -> &[u8] {
        let (a, b) = self.0.split_at(n.min(self.0.len()));
        self.0 = b;
        a
    }
}

/// Decoder, verifier and the lazy-restore parser over arbitrary bytes.
pub fn ltx_decode(data: &[u8]) {
    let _ = ltx::peek(data);
    let _ = Header::from_bytes(data);
    let verified = ltx::verify(data);
    let applied = dbsum::verify(data);
    assert_eq!(verified.is_ok(), applied.is_ok());
    if let (Ok((header, trailer, frames)), Ok((h2, t2))) = (&verified, &applied) {
        assert_eq!((header, trailer), (h2, t2));
        for frame in frames {
            assert!(frame.1 + frame.2 <= data.len());
            let _ = ltx::frame_payload(data, frame);
        }
        // What the decoder accepts, the lazy-restore parser accepts too.
        let staged = lazy::stage_file(&data[..HEADER_SIZE], data, data.len() as u64);
        let staged = staged
            .expect("staging a verified file")
            .expect("whole tail");
        assert_eq!(&staged.header, header);
        assert_eq!(staged.post_apply, trailer.post_apply);
    }
    // Pieces as the hydrator sees them: the start, a tail, a claimed size.
    let mut input = Input(data);
    let (head, tail) = (input.u8() as usize, input.u8() as usize);
    let total = u64::from(input.u16()) * 64;
    let _ = lazy::stage_file(
        &data[..head.min(data.len())],
        &data[data.len().saturating_sub(tail * 4)..],
        total,
    );
    let ps = 512 << (input.u8() % 8);
    let _ = lazy::decode_frame(input.0, u32::from(input.u8()), ps);
}

/// The lazy-restore plan over a chain of arbitrary files (a list of `u16`
/// length and bytes): staging, applying, `ready` and the fetch of every page.
/// A chain that `dbsum::apply` restores must give the same plan and bytes.
pub fn lazy_plan(data: &[u8]) {
    let mut input = Input(data);
    let mut files = Vec::new();
    while files.len() < 4 && !input.0.is_empty() {
        let n = usize::from(input.u16());
        files.push(input.take(n).to_vec());
    }
    let Some(page_size) = files
        .first()
        .and_then(|f| Header::from_bytes(f).ok())
        .map(|h| h.page_size)
    else {
        return;
    };
    let reference = scratch("plan-ref.db");
    let (mut full, mut current) = (true, 0);
    for file in &files {
        match Header::from_bytes(file) {
            Ok(h) if u64::from(h.commit) * u64::from(h.page_size) <= MAX_DB_BYTES => {}
            _ => full = false,
        }
        full = full
            && match dbsum::apply(&reference, file, current, page_size) {
                Ok(applied) => {
                    current = applied.post_apply;
                    true
                }
                Err(_) => false,
            };
    }

    let path = scratch("plan.db");
    let planned = lazy::Lazy::create(&path, page_size, std::time::Duration::from_secs(1))
        .ok()
        .filter(|plan| {
            files.iter().enumerate().all(|(i, file)| {
                let head = &file[..HEADER_SIZE.min(file.len())];
                match lazy::stage_file(head, file, file.len() as u64) {
                    Ok(Ok(staged)) => {
                        plan.stage(i as u32, staged);
                        plan.apply(i as u32).is_ok()
                    }
                    _ => false,
                }
            }) && plan.ready().is_ok()
        });
    assert!(planned.is_some() || !full, "a restorable chain gets a plan");
    if let Some(plan) = planned {
        let mut fetched = true;
        'runs: while let Some(run) = plan.next_batch(1 << 16) {
            for &(pgno, rel, len) in &run.pages {
                let start = (run.offset + u64::from(rel)) as usize;
                let frame = &files[run.file as usize][start..start + len as usize];
                match lazy::decode_frame(frame, pgno, page_size) {
                    Ok(page) => {
                        plan.install(pgno, &page).expect("a page of the plan");
                    }
                    Err(_) => {
                        fetched = false;
                        break 'runs;
                    }
                }
            }
        }
        if full && fetched {
            assert!(plan.is_complete());
            assert_eq!(
                std::fs::read(&path).expect("plan file"),
                std::fs::read(&reference).expect("reference file")
            );
        }
    }
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&reference);
}

/// What the generator below decided to encode.
struct Plan {
    ps: usize,
    checksummed: bool,
    /// Pages of the database before the second file.
    base: Vec<Vec<u8>>,
    /// Pages the second file replaces or appends, ascending.
    delta: Vec<(u32, Vec<u8>)>,
    /// Size of the database after the second file.
    commit: u32,
}

impl Plan {
    fn new(input: &mut Input) -> Plan {
        let flags = input.u8();
        let ps = 512usize << (flags % 3);
        let base: Vec<_> = (0..1 + input.u8() % 6).map(|_| page(input, ps)).collect();
        let mut commit = base.len() as u32 + u32::from(input.u8() % 4);
        if input.u8() % 2 == 1 && base.len() > 1 {
            commit = base.len() as u32 - 1;
        }
        let (mask, grown) = (input.u16(), base.len() as u32);
        let delta = (1..=commit)
            .filter(|p| *p > grown || mask & (1 << (p % 16)) != 0)
            .map(|p| (p, page(input, ps)))
            .collect();
        Plan {
            ps,
            checksummed: flags & 0x80 != 0,
            base,
            delta,
            commit,
        }
    }

    fn final_pages(&self) -> Vec<Vec<u8>> {
        let mut pages = self.base.clone();
        pages.truncate(self.commit as usize);
        pages.resize(self.commit as usize, Vec::new());
        for (p, data) in &self.delta {
            pages[*p as usize - 1] = data.clone();
        }
        pages
    }
}

/// A page that is constant, repeating or noise, so LZ4 takes all its paths.
fn page(input: &mut Input, ps: usize) -> Vec<u8> {
    match input.u8() % 3 {
        0 => vec![input.u8(); ps],
        1 => {
            let n = 1 + usize::from(input.u8() % 16);
            let mut unit = input.take(n).to_vec();
            // An exhausted input gives no bytes to repeat.
            unit.push(0);
            unit.iter().cycle().take(ps).copied().collect()
        }
        _ => {
            let mut x = u64::from(input.u16()) | 1;
            (0..ps)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect()
        }
    }
}

fn checksum_of(pages: &[Vec<u8>]) -> u64 {
    CHECKSUM_FLAG
        | pages
            .iter()
            .enumerate()
            .fold(0, |a, (i, p)| a ^ page_crc(i as u32 + 1, p))
}

fn encode(plan: &Plan, delta: bool, pre: u64, post: u64) -> Vec<u8> {
    let header = Header {
        flags: if plan.checksummed {
            0
        } else {
            HEADER_FLAG_NO_CHECKSUM
        },
        page_size: plan.ps as u32,
        commit: if delta {
            plan.commit
        } else {
            plan.base.len() as u32
        },
        min_txid: if delta { 2 } else { 1 },
        max_txid: if delta { 2 } else { 1 },
        timestamp: 1,
        pre_apply: if delta { pre } else { 0 },
        post_apply: post,
        ..Default::default()
    };
    let mut enc = Encoder::new(header).expect("valid header");
    if delta {
        for (p, data) in &plan.delta {
            enc.add_page(*p, data).expect("ascending pages");
        }
    } else {
        for (i, data) in plan.base.iter().enumerate() {
            enc.add_page(i as u32 + 1, data).expect("sequential pages");
        }
    }
    enc.finish().expect("post-apply checksum set")
}

/// Encodes a snapshot and an increment that the input describes, then checks
/// that decoding, applying and merging give back the pages.
pub fn ltx_roundtrip(data: &[u8]) {
    let plan = Plan::new(&mut Input(data));
    let (pre, post) = (checksum_of(&plan.base), checksum_of(&plan.final_pages()));
    let snapshot = encode(&plan, false, 0, pre);
    let increment = encode(&plan, true, pre, post);

    let (header, pages, trailer) = ltx::decode_all(&increment).expect("own file decodes");
    assert_eq!(header.commit, plan.commit);
    assert_eq!(trailer.post_apply, post);
    assert_eq!(pages, plan.delta);

    let path = scratch("roundtrip.db");
    let _ = dbsum::apply(&path, &snapshot, 0, 0).expect("snapshot applies");
    let applied = dbsum::apply(&path, &increment, pre, plan.ps as u32).expect("increment applies");
    assert_eq!(applied.post_apply, post);
    let chain = std::fs::read(&path).expect("database file");
    assert_eq!(chain, plan.final_pages().concat());
    assert_eq!(
        fold(&dbsum::file_sums(&path, plan.ps as u32).unwrap()),
        post
    );

    let merged = merge::merge(&[&snapshot, &increment]).expect("consecutive files merge");
    let _ = dbsum::apply(&path, &merged, 0, 0).expect("merged file applies");
    assert_eq!(std::fs::read(&path).expect("database file"), chain);
    let _ = std::fs::remove_file(&path);
}

/// Applies arbitrary bytes to a small database image.
pub fn page_apply(data: &[u8]) {
    let mut input = Input(data);
    let pages = usize::from(input.u8() % 6);
    let file = input.0;
    let current = match Header::from_bytes(file) {
        Ok(h) if u64::from(h.commit) * u64::from(h.page_size) > MAX_DB_BYTES => return,
        Ok(h) => h.pre_apply,
        Err(_) => 0,
    };
    let path = scratch("apply.db");
    let image: Vec<u8> = (0..pages * 512).map(|i| (i % 251) as u8).collect();
    std::fs::write(&path, image).expect("scratch file");
    if let Ok(applied) = dbsum::apply(&path, file, current, 0) {
        let ps = applied.header.page_size;
        let len = std::fs::metadata(&path).expect("database file").len();
        assert_eq!(len, u64::from(applied.header.commit) * u64::from(ps));
        // An increment trusts the checksum the caller says the image has.
        if applied.header.is_snapshot() {
            let sums = dbsum::file_sums(&path, ps).expect("sums of the result");
            assert_eq!(fold(&sums), applied.post_apply);
        }
    }
    let _ = std::fs::remove_file(&path);
}

/// Merges arbitrary files: input is a list of `u16` length and bytes.
pub fn ltx_merge(data: &[u8]) {
    let mut input = Input(data);
    let mut files = Vec::new();
    while files.len() < 4 && !input.0.is_empty() {
        let n = usize::from(input.u16());
        files.push(input.take(n).to_vec());
    }
    let files: Vec<&[u8]> = files.iter().map(Vec::as_slice).collect();
    if let Ok(out) = merge::merge(&files) {
        let (header, trailer) = dbsum::verify(&out).expect("a merged file verifies");
        let first = ltx::peek(files[0]).expect("verified input").0;
        assert_eq!(header.min_txid, first.min_txid);
        assert_eq!(header.pre_apply, first.pre_apply);
        assert!(trailer.post_apply & CHECKSUM_FLAG != 0);
    }
}

/// Reads arbitrary bytes as a WAL file, as they are and with the checksum
/// chain made valid so that the frames behind it are reached.
pub fn wal_scan(data: &[u8]) {
    scan_file(data);
    let mut fixed = data.to_vec();
    wal::repair(&mut fixed);
    scan_file(&fixed);
}

fn scan_file(bytes: &[u8]) {
    let path = scratch("scan.wal");
    std::fs::write(&path, bytes).expect("scratch file");
    // The page size the header claims, so frames are actually read.
    let claimed = bytes
        .get(8..12)
        .map_or(0, |b| u32::from_be_bytes(b.try_into().unwrap()));
    for ps in [claimed, 4096] {
        if !ltx::valid_page_size(ps) {
            continue;
        }
        let Ok(scan) = wal::scan(&path, ps, None, None) else {
            continue;
        };
        let frame = 24 + u64::from(ps);
        assert!(scan.pos.offset <= bytes.len() as u64);
        assert!(scan.commit.is_some() || scan.pages.is_empty());
        for (pgno, page) in &scan.pages {
            assert!(*pgno >= 1 && Some(*pgno) <= scan.commit);
            assert_eq!(page.len(), ps as usize);
        }
        if scan.commit.is_some() {
            assert_eq!((scan.pos.offset - wal::WAL_HEADER_SIZE) % frame, 0);
            // Nothing is left after the last commit, and a limit at it
            // changes nothing.
            let again = wal::scan(&path, ps, Some(scan.pos), None).expect("resumes");
            assert!(again.commit.is_none());
            assert_eq!(again.pos, scan.pos);
            let limited = wal::scan(&path, ps, None, Some(scan.pos)).expect("limits");
            assert_eq!(limited.pages, scan.pages);
        }
    }
    let _ = std::fs::remove_file(&path);
}

/// Captures chained segments of the transactions the input describes, with an
/// unfinished frame at the end of the WAL, and checks the chain: TXIDs and
/// checksums follow, applying in order gives the last database, and missing,
/// repeated or swapped segments are refused. The torn tail must not change a
/// byte of any segment.
pub fn capture_chain(data: &[u8]) {
    let mut input = Input(data);
    let tail = usize::from(input.u16()) % 600;
    let tx = |input: &mut Input| Tx {
        size: u32::from(input.u8() % 10) + 1,
        writes: (0..input.u8() % 5)
            .map(|_| (u32::from(input.u8() % 16), input.u8()))
            .collect(),
    };
    let initial: Vec<Tx> = (0..=input.u8() % 2).map(|_| tx(&mut input)).collect();
    let steps: Vec<Step> = (0..=input.u8() % 8)
        .map(|_| {
            let b = input.u8();
            (b % 5 != 0).then(|| (0..=(b / 5) % 3).map(|_| tx(&mut input)).collect())
        })
        .collect();

    let dir = scratch("chain");
    let (torn, clean) = (dir.join("torn"), dir.join("clean"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&torn).expect("scratch directory");
    std::fs::create_dir_all(&clean).expect("scratch directory");
    let chain = Chain::build(&torn, &initial, &steps, tail);
    chain.check_chain();
    chain.check_misuse();
    assert_eq!(
        chain.segments,
        Chain::build(&clean, &initial, &steps, 0).segments
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bytes from a fixed generator, for inputs that need no corpus.
    fn noise(seed: u64, n: usize) -> Vec<u8> {
        let mut x = seed | 1;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect()
    }

    #[test]
    fn every_target_survives_noise_and_short_inputs() {
        // Miri is slow: a few inputs are enough for it.
        let count = if cfg!(miri) { 4 } else { 24 };
        let inputs: Vec<Vec<u8>> = (0..count)
            .map(|i| noise(i + 1, (i as usize * 37) % 700))
            .chain([vec![], vec![0], vec![0xff; 200]])
            .collect();
        for input in &inputs {
            ltx_decode(input);
            lazy_plan(input);
            ltx_roundtrip(input);
            page_apply(input);
            ltx_merge(input);
            wal_scan(input);
            capture_chain(input);
        }
    }

    /// Rewrites the small seed corpus of `fuzz/corpus` (a few KB). Run it
    /// with `cargo test --lib write_seeds -- --ignored` after changing a
    /// format or a harness input.
    #[test]
    #[ignore = "writes into the source tree"]
    fn write_seeds() {
        use crate::wal::testing::WalBuilder;
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus");
        let put = |target: &str, name: &str, bytes: &[u8]| {
            let dir = root.join(target);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(name), bytes).unwrap();
        };
        // Files of the shapes the format tests use: a snapshot, an increment
        // that grows or shrinks, with and without header checksums.
        for (i, input) in (1..=4u64)
            .map(|i| (i, noise(i * 7, 48)))
            .collect::<Vec<_>>()
        {
            let plan = Plan::new(&mut Input(&input));
            let (pre, post) = (checksum_of(&plan.base), checksum_of(&plan.final_pages()));
            let snapshot = encode(&plan, false, 0, pre);
            let increment = encode(&plan, true, pre, post);
            put("ltx_roundtrip", &format!("noise-{i}"), &input);
            put("ltx_decode", &format!("snapshot-{i}"), &snapshot);
            put("ltx_decode", &format!("increment-{i}"), &increment);
            put(
                "page_apply",
                &format!("snapshot-{i}"),
                &[&[0], &snapshot[..]].concat(),
            );
            put(
                "page_apply",
                &format!("increment-{i}"),
                &[&[plan.base.len() as u8], &increment[..]].concat(),
            );
            let mut pair = Vec::new();
            for file in [&snapshot, &increment] {
                pair.extend_from_slice(&(file.len() as u16).to_be_bytes());
                pair.extend_from_slice(file);
            }
            put("ltx_merge", &format!("pair-{i}"), &pair);
            put("lazy_plan", &format!("pair-{i}"), &pair);
        }
        // Transaction scripts: a few sizes, fences, batches and torn tails.
        for i in 1..=4u64 {
            put(
                "capture_chain",
                &format!("noise-{i}"),
                &noise(i * 11, 24 + 12 * i as usize),
            );
        }
        // WAL files as the scan tests build them: both byte orders, a torn
        // tail, an uncommitted tail.
        for (name, be) in [("le", false), ("be", true)] {
            let mut wal = WalBuilder::new(512, (7, 9), be);
            wal.frame(1, 0, &[1; 512])
                .frame(2, 2, &[2; 512])
                .frame(2, 0, &[3; 512]);
            wal.frame(3, 3, &[4; 512]).frame(1, 0, &[5; 512]);
            put("wal_scan", &format!("{name}-committed"), &wal.bytes);
            let cut = wal.bytes.len() - 100;
            put("wal_scan", &format!("{name}-torn"), &wal.bytes[..cut]);
        }
    }

    #[test]
    fn seed_corpus_replays() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus");
        for (target, run) in [
            ("ltx_decode", ltx_decode as fn(&[u8])),
            ("lazy_plan", lazy_plan),
            ("ltx_roundtrip", ltx_roundtrip),
            ("page_apply", page_apply),
            ("ltx_merge", ltx_merge),
            ("wal_scan", wal_scan),
            ("capture_chain", capture_chain),
        ] {
            for entry in std::fs::read_dir(root.join(target)).expect("seed directory") {
                run(&std::fs::read(entry.unwrap().path()).unwrap());
            }
        }
    }
}
