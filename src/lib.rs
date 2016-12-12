#![feature(proc_macro, test)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate futures;

extern crate crossbeam;
extern crate env_logger;
extern crate fnv;
extern crate net2;
extern crate num_cpus;
extern crate serde;
extern crate serde_json;
extern crate tokio_core;
extern crate test;

mod worker;
mod backend;
mod ring;

use std::path::Path;
use std::thread;

use worker::{Worker, Adapter};
use ring::HashRing;

pub fn run() {
    env_logger::init().unwrap();
    let ring = HashRing::new(CONFIG.stddev);
    let workers: Vec<_> = (0..CONFIG.worker)
        .map(|_| {
            let nring = ring.clone();
            thread::spawn(move || {
                Worker::run(nring);
            })
        })
        .collect();
    let adapters: Vec<_> = ring.rings()
        .into_iter()
        .map(|input| {
            thread::spawn(move || {
                Adapter::run(input);
            })
        })
        .collect();

    for (worker, adapter) in workers.into_iter().zip(adapters.into_iter()) {
        worker.join().unwrap();
        adapter.join().unwrap();
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub thresholds: Vec<i64>,
    pub graphite: GraphiteConfig,
    pub banshee: BansheeConfig,
    pub interval: u64,
    pub stddev: usize,
    pub bind: String,
    pub worker: usize,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphiteConfig {
    pub address: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct BansheeConfig {
    pub address: String,
    pub allow: Vec<String>,
}

fn usage() -> &'static str {
    "\nuseage: statsd /srv/statsd-rs/etc/statsd.json"
}

impl Config {
    fn load<P: AsRef<Path>>(p: P) -> Config {
        use std::fs::File;
        use std::io::Read;

        let mut fp = File::open(p.as_ref())
            .map_err(|x| {
                println!("can not open the config file: {:?}, error: {}",
                         p.as_ref(),
                         x);
                x
            })
            .expect(usage());
        let mut content = String::new();
        let _content_len = fp.read_to_string(&mut content).unwrap();
        serde_json::from_str(&content)
            .map_err(|x| {
                error!("can not load content from config file, error: {}", x);
                x
            })
            .expect("config file is not a regular json file")
    }
}

lazy_static! {
    pub static ref CONFIG: Config  = {
        use std::env;
        let mut args = env::args().into_iter().skip(1);
        let pth = args.next().unwrap_or("/srv/statsd-rs/etc/statsd.json".to_owned());
        let config = Config::load(pth);
        info!("load Config as: {:?}", config);
        config
    };
}

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

    pub fn max<T: PartialOrd + Copy>(rhs: T, lhs: T) -> T {
        if rhs > lhs { rhs } else { lhs }
    }

    pub fn min<T: PartialOrd + Copy>(rhs: T, lhs: T) -> T {
        if rhs > lhs { lhs } else { rhs }
    }
}
