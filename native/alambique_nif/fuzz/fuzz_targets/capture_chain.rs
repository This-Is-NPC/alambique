#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| alambique_nif::fuzzing::capture_chain(data));
