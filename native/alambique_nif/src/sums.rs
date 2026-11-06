//! Per-page CRCs of a database, as the replicator keeps them between commits.
//!
//! The sums sit in fixed-size chunks behind `Arc`s. A clone copies one pointer
//! per chunk and a write copies only the chunk it touches, so a commit that
//! changes `k` pages costs `O(k * CHUNK + pages / CHUNK)` instead of
//! `O(pages)`, and every earlier value stays valid and unchanged (a state
//! whose upload failed can be used again).

use std::sync::Arc;

/// Sums per chunk (2 KiB).
const CHUNK: usize = 256;

type Chunk = [u64; CHUNK];

/// Invariant: entries at and after `len` are zero.
#[derive(Clone, Debug, Default)]
pub struct Sums {
    chunks: Vec<Arc<Chunk>>,
    len: usize,
}

impl Sums {
    /// `len` sums that are all zero (a lazy restore knows none yet). One
    /// shared chunk backs them all.
    pub fn zeroed(len: usize) -> Sums {
        let zero = Arc::new([0; CHUNK]);
        Sums {
            chunks: vec![zero; len.div_ceil(CHUNK)],
            len,
        }
    }

    pub fn from_slice(v: &[u64]) -> Sums {
        let chunks = v
            .chunks(CHUNK)
            .map(|c| {
                let mut a = [0; CHUNK];
                a[..c.len()].copy_from_slice(c);
                Arc::new(a)
            })
            .collect();
        Sums {
            chunks,
            len: v.len(),
        }
    }

    /// Reads little-endian sums; `None` unless `b` is a multiple of 8 bytes.
    pub fn from_le_bytes(b: &[u8]) -> Option<Sums> {
        if !b.len().is_multiple_of(8) {
            return None;
        }
        let v: Vec<u64> = b
            .as_chunks::<8>()
            .0
            .iter()
            .map(|c| u64::from_le_bytes(*c))
            .collect();
        Some(Sums::from_slice(&v))
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get(&self, i: usize) -> u64 {
        assert!(i < self.len);
        self.chunks[i / CHUNK][i % CHUNK]
    }

    pub fn set(&mut self, i: usize, v: u64) {
        assert!(i < self.len);
        if self.chunks[i / CHUNK][i % CHUNK] != v {
            Arc::make_mut(&mut self.chunks[i / CHUNK])[i % CHUNK] = v;
        }
    }

    /// Cuts or extends (with zeros) to `len` sums.
    pub fn resize(&mut self, len: usize) {
        if len < self.len {
            self.chunks.truncate(len.div_ceil(CHUNK));
            let end = self.len.min(self.chunks.len() * CHUNK);
            for i in len..end {
                self.set(i, 0);
            }
        } else if len > self.len {
            let zero = Arc::new([0; CHUNK]);
            self.chunks.resize(len.div_ceil(CHUNK), zero);
        }
        self.len = len;
    }

    /// XOR of all sums.
    pub fn fold(&self) -> u64 {
        self.chunks
            .iter()
            .flat_map(|c| c.iter())
            .fold(0, |a, s| a ^ s)
    }

    #[cfg(test)]
    pub fn to_vec(&self) -> Vec<u64> {
        self.chunks
            .iter()
            .flat_map(|c| c.iter().copied())
            .take(self.len)
            .collect()
    }

    /// Chunks of `self` and `other` that are the same allocation.
    #[cfg(test)]
    pub fn shared_chunks(&self, other: &Sums) -> usize {
        self.chunks
            .iter()
            .zip(&other.chunks)
            .filter(|(a, b)| Arc::ptr_eq(a, b))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_write_copies_one_chunk_and_leaves_the_original() {
        let a = Sums::from_slice(&(0..10 * CHUNK as u64).collect::<Vec<_>>());
        let mut b = a.clone();
        assert_eq!(a.shared_chunks(&b), 10);
        b.set(3 * CHUNK + 5, 99);
        assert_eq!(a.shared_chunks(&b), 9);
        assert_eq!(a.get(3 * CHUNK + 5), (3 * CHUNK + 5) as u64);
        assert_eq!(b.get(3 * CHUNK + 5), 99);
        // The same value is no write.
        let mut c = a.clone();
        c.set(0, 0);
        assert_eq!(a.shared_chunks(&c), 10);
    }

    #[test]
    fn resize_keeps_the_zero_tail_invariant() {
        let mut s = Sums::from_slice(&[1; CHUNK + 10]);
        s.resize(CHUNK + 3);
        s.resize(CHUNK + 10);
        assert_eq!(s.get(CHUNK + 2), 1);
        assert_eq!(s.get(CHUNK + 3), 0);
        s.resize(5);
        s.resize(3 * CHUNK);
        assert_eq!(s.fold(), 1);
        assert_eq!(s.len(), 3 * CHUNK);
        assert_eq!(s.to_vec().len(), 3 * CHUNK);
    }

    #[test]
    fn bytes_round_trip() {
        let v: Vec<u64> = (0..700).map(|i| i * 0x0101_0101).collect();
        let s = Sums::from_slice(&v);
        let bytes: Vec<u8> = v.iter().flat_map(|x| x.to_le_bytes()).collect();
        assert_eq!(s.to_vec(), v);
        assert_eq!(Sums::from_le_bytes(&bytes).unwrap().to_vec(), v);
        assert!(Sums::from_le_bytes(&[0; 7]).is_none());
        assert_eq!(Sums::zeroed(300).to_vec(), vec![0; 300]);
    }
}

#[cfg(test)]
mod props {
    use super::*;
    use proptest::prelude::*;

    #[derive(Debug, Clone)]
    enum Op {
        Set(usize, u64),
        Resize(usize),
        Fork,
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        let max = 3 * CHUNK;
        prop::collection::vec(
            prop_oneof![
                4 => (0..max, prop_oneof![Just(0u64), any::<u64>()]).prop_map(|(i, v)| Op::Set(i, v)),
                2 => (0..max).prop_map(Op::Resize),
                1 => Just(Op::Fork),
            ],
            0..40,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Sums behave like a plain vector, and a clone is never changed by
        /// writes to the original.
        #[test]
        #[cfg_attr(miri, ignore = "proptest is too slow under Miri")]
        fn sums_match_a_vector_and_forks_stay_unchanged(
            start in prop::collection::vec(any::<u64>(), 0..2 * CHUNK),
            ops in ops(),
        ) {
            let mut model = start.clone();
            let mut sums = Sums::from_slice(&start);
            let mut forks: Vec<(Sums, Vec<u64>)> = Vec::new();
            for op in ops {
                match op {
                    Op::Set(i, v) if i < model.len() => {
                        model[i] = v;
                        sums.set(i, v);
                    }
                    Op::Set(..) => {}
                    Op::Resize(n) => {
                        model.resize(n, 0);
                        sums.resize(n);
                    }
                    Op::Fork => forks.push((sums.clone(), model.clone())),
                }
                prop_assert_eq!(sums.len(), model.len());
            }
            for (s, m) in forks.iter().chain([&(sums, model)]) {
                prop_assert_eq!(&s.to_vec(), m);
                prop_assert_eq!(s.fold(), m.iter().fold(0, |a, x| a ^ x));
            }
        }
    }
}
