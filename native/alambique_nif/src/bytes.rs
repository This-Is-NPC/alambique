//! Big-endian reads over untrusted bytes: a short input is a `None`, never a
//! panic.

/// A cursor that consumes fixed-size fields from the front of a slice.
pub struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Reader(bytes)
    }

    /// The next `N` bytes.
    pub fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }

    pub fn u16(&mut self) -> Option<u16> {
        self.array().map(u16::from_be_bytes)
    }

    pub fn u32(&mut self) -> Option<u32> {
        self.array().map(u32::from_be_bytes)
    }

    pub fn u64(&mut self) -> Option<u64> {
        self.array().map(u64::from_be_bytes)
    }

    /// Drops the next `n` bytes.
    pub fn skip(&mut self, n: usize) -> Option<()> {
        self.0 = self.0.get(n..)?;
        Some(())
    }
}

/// The big-endian `u32` at `at`.
pub fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    let mut r = Reader::new(bytes);
    r.skip(at)?;
    r.u32()
}

/// The big-endian `u64` at `at`.
pub fn u64_at(bytes: &[u8], at: usize) -> Option<u64> {
    let mut r = Reader::new(bytes);
    r.skip(at)?;
    r.u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_fields_in_order_and_stops_at_the_end() {
        let mut r = Reader::new(&[0, 1, 0, 0, 0, 2, 9]);
        assert_eq!(r.u16(), Some(1));
        assert_eq!(r.u32(), Some(2));
        assert_eq!(r.u16(), None);
        assert_eq!(r.array::<1>(), Some([9]));
        assert_eq!(r.array::<1>(), None);
    }

    #[test]
    fn a_read_past_the_end_fails_without_consuming() {
        let mut r = Reader::new(&[1, 2, 3]);
        assert_eq!(r.u32(), None);
        assert_eq!(r.array::<3>(), Some([1, 2, 3]));
    }

    #[test]
    fn reads_at_an_offset() {
        let b = [0, 0, 0, 0, 0, 0, 0, 7, 0xff];
        assert_eq!(u64_at(&b, 0), Some(7));
        assert_eq!(u32_at(&b, 4), Some(7));
        assert_eq!(u32_at(&b, 6), None);
        assert_eq!(u32_at(&b, usize::MAX), None);
        assert_eq!(u64_at(&b, 2), None);
    }
}
