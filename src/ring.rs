use std::hash::Hasher;
use std::default::Default;

use fnv::FnvHasher;
use num_cpus;

use com;

// Generate for a ring buffer into sub threads
#[derive(Clone)]
pub struct HashRing {
    num: usize,
    dup: usize,
}

impl HashRing {
    pub fn new(num: usize, dup: usize) -> HashRing {
        let allow_num = com::min(num_cpus::get(), num);
        let allow_dup = com::min(dup, 256);
        HashRing {
            num: allow_num,
            dup: allow_dup,
        }
    }

    pub fn num(&self) -> usize {
        self.num
    }

    pub fn position(&self, metric: &str) -> usize {
        let mut hasher = FnvHasher::default();
        hasher.write(metric.as_bytes());
        (hasher.finish() as usize) % self.dup % self.num
    }
}
