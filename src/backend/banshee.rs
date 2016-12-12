use std::default::Default;

use worker::{ValueCount, TimeData, CountData, GaugeData};
use backend::BackEnd;

pub struct Banshee {
    allow_time: Vec<String>,
    prefix_counter: String,
    prefix_timer: String,
    prefix_gauge: String,
    validate: bool,
}

impl Default for Banshee {
    fn default() -> Self {
        Banshee {
            allow_time: vec!["mean_90".to_owned(), "count_ps".to_owned()],
            prefix_counter: "counter".to_owned(),
            prefix_timer: "timer".to_owned(),
            prefix_gauge: "gauge".to_owned(),
            validate: true,
        }
    }
}

impl BackEnd for Banshee {
    fn validate(&self) -> bool {
        debug!("is validate backend for banshee: {}", self.validate);
        self.validate
    }

    fn counting(&self, ts: u64, count: &CountData, buf: &mut Vec<u8>) {
        let iter = count.into_iter()
            .map(|(key, &ValueCount(value, _count))| {
                format!("{}.{} {} {}\n", self.prefix_counter, key, ts, value)
            });
        for line in iter {
            buf.extend_from_slice(line.as_bytes());
        }
    }

    fn gauging(&self, ts: u64, gauge: &GaugeData, buf: &mut Vec<u8>) {
        let iter = gauge.into_iter()
            .map(|(key, value)| format!("{}.{} {} {}\n", self.prefix_gauge, key, ts, value));
        for line in iter {
            buf.extend_from_slice(line.as_bytes());
        }
    }

    fn timing(&self, ts: u64, time: &TimeData, buf: &mut Vec<u8>) {
        for (key, submap) in time {
            for sub_key in &self.allow_time {
                let value = submap.get(sub_key).expect("never empty");
                let line = format!("{}.{}.{} {} {}\n",
                                   self.prefix_timer,
                                   key,
                                   sub_key,
                                   ts,
                                   value);
                buf.extend_from_slice(line.as_bytes());
            }
        }
    }
}
