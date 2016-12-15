use std::hash::Hasher;
use std::default::Default;

use fnv::FnvHasher;
use num_cpus;

use com;

// Generate for a ring buffer into sub threads
#[derive(Clone)]
pub struct HashRing {
    shield: usize,
    num: usize,
}

impl HashRing {
    pub fn new(dev: usize) -> HashRing {
        let num = 2usize.pow(dev as u32);
        let allow_num = num_cpus::get();
        // 最多不能超过核心数量
        let last = com::min(allow_num, num);
        if last < num {
            warn!("caculate threads are never more than the number of the cpu, run as {} .",
                  last);
        }
        let shield = Self::make_shield(dev);

        HashRing {
            shield: shield,
            num: num,
        }
    }

    pub fn num(&self) -> usize {
        self.num
    }

    fn make_shield(dev: usize) -> usize {
        let mut shield = 1usize;
        for _ in 0..(dev - 1) {
            shield <= 1;
            shield |= 1;
        }
        shield
    }

    pub fn position(&self, metric: &str) -> usize {
        let mut hasher = FnvHasher::default();
        hasher.write(metric.as_bytes());
        hasher.finish() as usize & self.shield
    }
}
