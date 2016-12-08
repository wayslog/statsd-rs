#![feature(proc_macro, test)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate futures;

extern crate serde;
extern crate serde_json;
extern crate env_logger;
extern crate net2;
extern crate num_cpus;
extern crate tokio_core;
extern crate test;
extern crate crossbeam;

mod worker;
mod backend;

#[cfg(test)]
mod tests {
    use test::Bencher;

    #[test]
    fn test_it_works() {}
    #[bench]
    fn bench_empty(_b: &mut Bencher) {}
}

pub mod com {
    use std::time;
    pub fn now() -> u64 {
        let now = time::SystemTime::now();
        let nowd = now.duration_since(time::UNIX_EPOCH).unwrap();
        nowd.as_secs()
    }
}
