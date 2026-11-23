#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| stratadb_fuzz::fuzz_wal_recovery(data));
