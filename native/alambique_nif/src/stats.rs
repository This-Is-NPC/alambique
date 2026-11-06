//! Native counters and gauges, read with one NIF call (`stats/0`).
//!
//! Counters only grow; gauges go up and down. All are relaxed atomics, so a
//! read is a consistent value per field, not a snapshot across fields.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use libsqlite3_sys as ffi;
use rustler::{Encoder, Env, Term};

macro_rules! counters {
    ($($name:ident),* $(,)?) => {
        $(pub static $name: AtomicU64 = AtomicU64::new(0);)*
    };
}

/// Allocation counters are on the hot path of every thread, so each thread
/// adds to its own cache line and a read sums them.
#[repr(align(64))]
pub struct Shard {
    pub sqlite_alloc: AtomicU64,
    pub sqlite_free: AtomicU64,
    pub rust_alloc: AtomicU64,
    pub rust_free: AtomicU64,
}

static SHARDS: [Shard; 64] = [const {
    Shard {
        sqlite_alloc: AtomicU64::new(0),
        sqlite_free: AtomicU64::new(0),
        rust_alloc: AtomicU64::new(0),
        rust_free: AtomicU64::new(0),
    }
}; 64];

thread_local! {
    static MINE: usize = {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        NEXT.fetch_add(1, Relaxed) as usize % SHARDS.len()
    };
}

/// The calling thread's shard.
pub fn shard() -> &'static Shard {
    &SHARDS[MINE.try_with(|i| *i).unwrap_or(0)]
}

fn sum(f: impl Fn(&Shard) -> &AtomicU64) -> i64 {
    SHARDS.iter().map(|s| f(s).load(Relaxed)).sum::<u64>() as i64
}

counters!(
    CACHE_HITS,
    CACHE_MISSES,
    WAL_FRAMES_READ,
    LTX_BYTES_ENCODED,
    LTX_BYTES_DECODED,
);

pub static CONNECTIONS_OPEN: AtomicI64 = AtomicI64::new(0);
pub static FILES_OPEN: AtomicI64 = AtomicI64::new(0);

/// Dirty NIF calls of one class: how many, and their wall time.
pub struct Calls {
    count: AtomicU64,
    total_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl Calls {
    const fn new() -> Self {
        Calls {
            count: AtomicU64::new(0),
            total_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
        }
    }

    /// Times the call until the guard is dropped.
    pub fn time(&'static self) -> Timer {
        Timer {
            calls: self,
            start: Instant::now(),
        }
    }
}

pub static SQLITE_CALLS: Calls = Calls::new();
pub static REPLICATION_CALLS: Calls = Calls::new();
pub static LAZY_CALLS: Calls = Calls::new();

pub struct Timer {
    calls: &'static Calls,
    start: Instant,
}

impl Drop for Timer {
    fn drop(&mut self) {
        crate::alloc::flush_thread_cache();
        let ns = self.start.elapsed().as_nanos() as u64;
        self.calls.count.fetch_add(1, Relaxed);
        self.calls.total_ns.fetch_add(ns, Relaxed);
        self.calls.max_ns.fetch_max(ns, Relaxed);
    }
}

pub fn add(counter: &AtomicU64, n: usize) {
    counter.fetch_add(n as u64, Relaxed);
}

/// Every counter and gauge as `%{atom => integer}`. `sqlite_memory_highwater`
/// is reset by the read, so it is the peak since the previous read.
#[rustler::nif]
fn stats(env: Env) -> Term {
    // SAFETY: both only read SQLite's global memory counters.
    let (used, high) = unsafe { (ffi::sqlite3_memory_used(), ffi::sqlite3_memory_highwater(1)) };
    let get = |c: &AtomicU64| c.load(Relaxed) as i64;
    let calls = |name: &str, c: &Calls| {
        [
            (format!("{name}_calls"), c.count.load(Relaxed) as i64),
            (
                format!("{name}_call_ns_total"),
                c.total_ns.load(Relaxed) as i64,
            ),
            (format!("{name}_call_ns_max"), c.max_ns.load(Relaxed) as i64),
        ]
    };
    let mut pairs: Vec<(String, i64)> = vec![
        ("sqlite_alloc_bytes".into(), sum(|s| &s.sqlite_alloc)),
        ("sqlite_free_bytes".into(), sum(|s| &s.sqlite_free)),
        ("rust_alloc_bytes".into(), sum(|s| &s.rust_alloc)),
        ("rust_free_bytes".into(), sum(|s| &s.rust_free)),
        ("sqlite_memory_used".into(), used),
        ("sqlite_memory_highwater".into(), high),
        ("page_cache_hits".into(), get(&CACHE_HITS)),
        ("page_cache_misses".into(), get(&CACHE_MISSES)),
        ("connections_open".into(), CONNECTIONS_OPEN.load(Relaxed)),
        ("files_open".into(), FILES_OPEN.load(Relaxed)),
        ("wal_frames_read".into(), get(&WAL_FRAMES_READ)),
        ("ltx_bytes_encoded".into(), get(&LTX_BYTES_ENCODED)),
        ("ltx_bytes_decoded".into(), get(&LTX_BYTES_DECODED)),
    ];
    pairs.extend(calls("sqlite", &SQLITE_CALLS));
    pairs.extend(calls("replication", &REPLICATION_CALLS));
    pairs.extend(calls("lazy", &LAZY_CALLS));
    pairs.into_iter().fold(Term::map_new(env), |map, (k, v)| {
        let key = rustler::Atom::from_str(env, &k).unwrap();
        map.map_put(key.encode(env), v.encode(env)).unwrap()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_add_up_across_threads() {
        let before = sum(|s| &s.rust_alloc);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..1000 {
                        shard().rust_alloc.fetch_add(3, Relaxed);
                    }
                });
            }
        });
        assert_eq!(sum(|s| &s.rust_alloc) - before, 12_000);
    }

    #[test]
    fn timer_counts_one_call_and_keeps_the_longest() {
        static CALLS: Calls = Calls::new();
        drop(CALLS.time());
        let t = CALLS.time();
        std::thread::sleep(std::time::Duration::from_millis(5));
        drop(t);
        assert_eq!(CALLS.count.load(Relaxed), 2);
        assert!(CALLS.max_ns.load(Relaxed) >= 5_000_000);
        assert!(CALLS.total_ns.load(Relaxed) >= CALLS.max_ns.load(Relaxed));
    }
}
