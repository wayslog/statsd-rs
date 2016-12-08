
use futures::stream::Stream;
use futures::{Future, Async, Poll};
use worker::{ValueCount, LightBuffer, TimeMap, CountMap, GaugeMap, TimeSet};
use worker::{Result, StatsdError};

pub struct BackEnd<S>
    where S: Stream<Item = LightBuffer, Error = StatsdError>
{
    input: S,
    prefix_counters: String,
    prefix_stats_count: String,
    prefix_timers: String,
    prefix_gauges: String,
    thresholds: Vec<usize>,
    interval: f64,
}

pub struct Entry {
    ts: u64,
    inmap: HashMap<String, f64>,
}

impl<S> BackEnd<S>
    where S: Stream<Item = LightBuffer, Error = StatsdError>
{
    fn counting(&self, ts: u64, count: CountMap, buffer: &mut Vec<u8>) {
        let iter = count.into_iter()
            .map(|(key, ValueCount(v, c))| {
                format!("{pc:}.{key} {val} {ts}\n{psc}.{key} {count} {ts}\n",
                        pc = self.prefix_counters,
                        psc = self.prefix_stats_count,
                        key = key,
                        ts = ts,
                        // count rate
                        val = v / self.interval,
                        count = c)
            });
        for dline in iter {
            buffer.extend_from_slice(dline.as_bytes());
        }
    }

    fn gaguing(&self, ts: u64, gauge: GaugeMap, buffer: &mut Vec<u8>) {
        let iter = gauge.into_iter()
            .map(|(key, val)| format!("{}.{} {} {}", self.prefix_gauges, key, val, ts));
        for line in iter {
            buffer.extend_from_slice(line.as_bytes());
        }
    }

    fn timing(&self, ts: u64, time: TimeMap, buffer: &mut Vec<u8>) {}
}


impl<S> Stream for BackEnd<S>
    where S: Stream<Item = LightBuffer, Error = StatsdError>
{
    type Item = Entry;
    type Error = StatsdError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let buf = try_ready!(self.input.poll()).expect("never term");
        let LightBuffer { timestamp: ts, time, count, gauge } = buf;
        let mut buffer = Vec::new();
        self.counting(ts, count, &mut buffer);
        self.gaguing(ts, gauge, &mut buffer);
        Ok(Async::NotReady)
    }
}
