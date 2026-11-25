//! CRC-64/ISO as implemented by Go's `hash/crc64` with `crc64.ISO`.

const POLY: u64 = 0xD800_0000_0000_0000;

const fn make_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u64;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static TABLE: [u64; 256] = make_table();

/// Streaming CRC-64/ISO hasher.
#[derive(Clone)]
pub struct Crc64 {
    state: u64,
}

impl Default for Crc64 {
    fn default() -> Self {
        Crc64 { state: !0 }
    }
}

impl Crc64 {
    pub fn update(&mut self, data: &[u8]) {
        let mut crc = self.state;
        for &b in data {
            crc = TABLE[((crc as u8) ^ b) as usize] ^ (crc >> 8);
        }
        self.state = crc;
    }

    pub fn sum(&self) -> u64 {
        !self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crc64(data: &[u8]) -> u64 {
        let mut h = Crc64::default();
        h.update(data);
        h.sum()
    }

    #[test]
    fn matches_the_go_iso_check_value() {
        assert_eq!(crc64(b"123456789"), 0xb909_56c7_75a4_1001);
    }

    #[test]
    fn streaming_equals_one_shot() {
        let mut h = Crc64::default();
        h.update(b"1234");
        h.update(b"56789");
        assert_eq!(h.sum(), crc64(b"123456789"));
    }

    #[test]
    fn the_polynomial_as_a_message_error_leaves_the_checksum_unchanged() {
        // x^64 + x^4 + x^3 + x + 1 with the last bit of the data as x^0: the
        // reflected algorithm reads the low bit of each byte first.
        let mut data: Vec<u8> = (0..64u32).map(|i| (i * 7 + 3) as u8).collect();
        let before = crc64(&data);
        let n = data.len();
        data[n - 9] ^= 0x80;
        data[n - 1] ^= 0xD8;
        assert_eq!(crc64(&data), before);
    }
}
