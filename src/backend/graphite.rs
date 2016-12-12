use std::default::Default;

use worker::{ValueCount, TimeData, CountData, GaugeData};
use backend::BackEnd;

pub struct Graphite {
    prefix_counter: String,
    prefix_stats_count: String,
    prefix_timer: String,
    prefix_gauge: String,
}

impl Default for Graphite {
    fn default() -> Self {
        Graphite {
            prefix_counter: "stats".to_owned(),
            prefix_stats_count: "stats_counts".to_owned(),
            prefix_timer: "stats.timers".to_owned(),
            prefix_gauge: "stats.gauges".to_owned(),
        }
    }
}

impl BackEnd for Graphite {
    fn counting(&self, ts: u64, count: &CountData, buffer: &mut Vec<u8>) {
        let iter = count.into_iter()
            .map(|(key, &ValueCount(v, c))| {
                format!("{pc}.{key} {val} {ts}\n{psc}.{key} {count} {ts}\n",
                        pc = self.prefix_counter,
                        psc = self.prefix_stats_count,
                        key = key,
                        ts = ts,
                        // count rate
                        val = v,
                        count = c)
            });
        for dline in iter {
            buffer.extend_from_slice(dline.as_bytes());
        }
    }

    fn gauging(&self, ts: u64, gauge: &GaugeData, buffer: &mut Vec<u8>) {
        let iter = gauge.into_iter()
            .map(|(key, val)| format!("{}.{} {} {}\n", self.prefix_gauge, key, val, ts));
        for line in iter {
            buffer.extend_from_slice(line.as_bytes());
        }
    }

    fn timing(&self, ts: u64, time: &TimeData, buffer: &mut Vec<u8>) {
        for (thekey, submap) in time.into_iter() {
            for (subkey, val) in submap.into_iter() {
                let line = format!("{}.{}.{} {} {}\n",
                                   self.prefix_timer,
                                   thekey,
                                   subkey,
                                   val,
                                   ts);
                buffer.extend_from_slice(line.as_bytes());
            }
        }
    }
}
