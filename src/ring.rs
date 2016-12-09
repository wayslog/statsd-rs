use std::hash::Hasher;
use std::sync::Arc;
use std::default::Default;

use crossbeam::sync::MsQueue;
use fnv::FnvHasher;
use num_cpus;

use worker::Line;
use com;

// Generate for a ring buffer into sub threads
#[derive(Clone)]
pub struct HashRing {
    shield: usize,
    ring: Vec<Arc<MsQueue<Line>>>,
}

impl HashRing {
    pub fn new(dev: usize) -> HashRing {
        let num = 2usize.pow(dev as u32);
        let allow_num = com::max(num_cpus::get(), 2);
        // 最多不能超过半数核心
        let last = com::min(allow_num / 2, num);
        if last < num {
            warn!("caculate threads are never more than half of the cpu, run as {} .",
                  last);
        }
        let shield = Self::make_shield(dev);
        let ring: Vec<_> = (0..num)
            .into_iter()
            .map(|_| Arc::new(MsQueue::new()))
            .collect();

        HashRing {
            shield: shield,
            ring: ring,
        }
    }

    fn make_shield(dev: usize) -> usize {
        let mut shield = 1usize;
        for _ in 0..(dev - 1) {
            shield <= 1;
            shield |= 1;
        }
        shield
    }
    pub fn dispatch(&self, line: Line) {
        let mut hasher = FnvHasher::default();
        hasher.write(line.metric.as_bytes());
        let pos = hasher.finish() as usize & self.shield;
        self.ring[pos].push(line);
    }

    pub fn rings(&self) -> Vec<Arc<MsQueue<Line>>> {
        self.ring.clone()
    }
}
