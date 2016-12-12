
use std::collections::HashMap;
use std::convert::From;
use std::io::{self, Error};
use std::net::{SocketAddr, ToSocketAddrs};
use std::num::ParseFloatError;
use std::mem;
use std::ops::DerefMut;
use std::result;
use std::string::FromUtf8Error;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam::sync::MsQueue;
use futures::stream::Stream;
use futures::{Async, Poll};
use tokio_core::net::{UdpCodec, UdpSocket};
use tokio_core::reactor::{Handle, Core};
use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;

use ::CONFIG;
use backend::BackEndSender;
use com::now;
use ring::HashRing;

const CLCR: u8 = '\n' as u8;

pub struct Worker;

impl Worker {
    pub fn run(ring: HashRing) {
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let socket = Self::build_socket(&*CONFIG.bind.clone(), &handle, true);
        info!("worker: bind at {:?}", &CONFIG.bind);
        let service =
            socket.framed(RecvCodec {}).flatten().for_each(|item| Ok(ring.dispatch(item)));
        core.run(service).unwrap();
    }

    fn build_socket<T: ToSocketAddrs>(bind: T, handle: &Handle, reuse_port: bool) -> UdpSocket {
        let socket = UdpBuilder::new_v4()
            .expect("udp port is full")
            .reuse_address(true)
            .expect("SO_REUSEPORT not support")
            .reuse_port(reuse_port)
            .expect("SO_REUSEPORT not support")
            .bind(bind)
            .map_err(|err| {
                error!("bind Faild error: {}", err);
                err
            })
            .unwrap();
        UdpSocket::from_socket(socket, handle).expect("can't convert from std::net::UdpSocket")
    }
}

pub struct RecvCodec;

impl UdpCodec for RecvCodec {
    type In = Packet;
    type Out = ();

    fn decode(&mut self, _addr: &SocketAddr, buf: &[u8]) -> io::Result<Self::In> {
        debug!("get a new packet");
        Ok(Packet::from(buf.to_vec()))
    }

    fn encode(&mut self, _out: Self::Out, _into: &mut Vec<u8>) -> SocketAddr {
        unreachable!("never send back !");
    }
}

pub struct Packet {
    buf: Vec<u8>,
}

impl Packet {
    pub fn from(buf: Vec<u8>) -> Packet {
        Packet { buf: buf }
    }
}


impl Stream for Packet {
    type Item = Line;
    type Error = StatsdError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.buf.len() == 0 {
            debug!("a packet was parsed");
            return Ok(Async::Ready(None));
        }

        debug!("send a part of one packet");
        let pos = self.buf.iter().position(|x| x == &CLCR).unwrap_or(self.buf.len());

        let mut drained: Vec<_> = self.buf.drain(..pos + 1).into_iter().collect();
        let _clcr = drained.pop().unwrap();
        let line_str = match String::from_utf8(drained) {
            Ok(item) => {
                debug!("get a new line {}", &item);
                item
            }
            Err(err) => {
                warn!("parse line error {:?}", err);
                return Ok(Async::NotReady);
            }
        };

        match Line::parse(line_str) {
            Ok(line) => Ok(Async::Ready(Some(line))),
            Err(err) => {
                warn!("parse error : {:?}", err);
                Ok(Async::NotReady)
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Kind {
    /// Gauge(Gauge)
    Gauge(f64),
    /// Count(Count)
    Count(f64),
    /// Time(Value, count)
    Time(f64, f64),
}

pub use self::Kind::{Time, Count, Gauge};

impl Kind {
    fn parse(value_str: &str, kind_str: &str, rate_str: &str) -> Result<Kind> {
        let rate = rate_str.parse::<f64>()?;

        match kind_str {
            "ms" => {
                let value = value_str.parse::<f64>().unwrap_or(0.0);
                Ok(Time(value, 1.0 / rate))
            }
            "c" => {
                let value = value_str.parse::<f64>().unwrap_or(1.0);
                Ok(Count(value / rate))
            }
            "g" => {
                let value = value_str.parse::<f64>().unwrap_or(0.0);
                Ok(Gauge(value))
            }
            _ => Err(StatsdError::UnknownKind(kind_str.to_owned())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Line {
    pub metric: String,
    pub kind: Kind,
}

impl Line {
    fn parse(input: String) -> Result<Line> {
        let mut lsp = input.split(":");
        let metric = lsp.next().ok_or(StatsdError::WrongLine)?;
        let bits = lsp.next().ok_or(StatsdError::WrongLine)?;
        let mut bsp = bits.split("|");
        let value_str = bsp.next().ok_or(StatsdError::WrongLine)?;
        let kind_str = bsp.next().ok_or(StatsdError::WrongLine)?;
        // sample rate support
        let rate_str = bsp.next().unwrap_or("1.0");
        let kind = Kind::parse(value_str.trim(), kind_str.trim(), rate_str.trim())?;
        Ok(Line {
            metric: metric.to_owned(),
            kind: kind,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ValueCount(pub f64, pub f64);
#[derive(Clone, Debug)]
pub struct TimeSet(pub Vec<f64>, pub f64);


pub type TimeMap = HashMap<String, TimeSet>;
pub type TimeData = HashMap<String, HashMap<String, f64>>;

pub type CountMap = HashMap<String, ValueCount>;
pub type CountData = CountMap;
pub type GaugeMap = HashMap<String, f64>;
pub type GaugeData = GaugeMap;

#[derive(Clone)]
pub struct LightBuffer {
    pub timestamp: u64,
    pub gauge: GaugeData,
    pub count: CountData,
    pub time: TimeData,
}

impl LightBuffer {
    fn caculate_time(time: TimeMap) -> TimeData {
        debug!("caculate time value start");
        let mut time_data = TimeData::new();
        for (key, TimeSet(mut values, sample_count)) in time {
            let mut current = HashMap::new();
            if values.len() == 0 {
                current.insert("count".to_string(), 0.0f64);
                current.insert("count_ps".to_string(), 0.0);
                time_data.insert(key, current);
                continue;
            }
            values.sort_by(|v1, v2| v1.partial_cmp(v2).unwrap());
            let count = values.len();
            let min = values[0];
            let max = values[count - 1];
            let mut cumulative: Vec<f64> = vec![min];
            let mut latest = min;

            for &val in &values[1..] {
                cumulative.push(val + latest);
                latest = val;
            }

            let mut sum = values[0];
            let mut mean = min;
            let mut boundary = max;
            for &threshold in &CONFIG.thresholds[..] {
                let abs_threshold = threshold.abs();
                let mut threshold_num = count;
                if count > 1 {
                    threshold_num = (threshold.abs() / 100 * (count as i64)) as usize;
                    if threshold_num == 0 {
                        continue;
                    }
                    if threshold > 0 {
                        boundary = values[threshold_num - 1];
                        sum = cumulative[threshold_num - 1];
                    } else {
                        boundary = values[count - threshold_num];
                        sum = cumulative[count - 1] - cumulative[count - threshold_num - 1];
                    }
                    mean = sum / threshold_num as f64;
                }
                current.insert(format!("count_{}", abs_threshold), threshold_num as f64);
                current.insert(format!("mean_{}", abs_threshold), mean);
                current.insert(format!("{}_{}",
                                       if threshold > 0 { "upper" } else { "lower" },
                                       abs_threshold),
                               boundary);
                current.insert(format!("sum_{}", abs_threshold), sum);
            }
            sum = cumulative[count - 1];
            mean = sum / count as f64;
            let median = if count % 2 == 0 {
                values[count / 2]
            } else {
                (values[count / 2 - 1] + values[count / 2]) / 2.0
            };
            // NOT SUPPORT stddev
            current.insert("upper".to_owned(), max);
            current.insert("lower".to_owned(), min);
            current.insert("count".to_owned(), sample_count);
            current.insert("lower".to_owned(), sample_count / CONFIG.interval as f64);

            current.insert("sum".to_owned(), sum);
            current.insert("mean".to_owned(), mean);
            current.insert("median".to_owned(), median);

            time_data.insert(key, current);
        }
        time_data
    }
}

pub struct MergeBuffer {
    interval: Duration,
    time: Arc<Mutex<TimeMap>>,
    count: Arc<Mutex<CountMap>>,
    gauge: Arc<Mutex<GaugeMap>>,
}

impl Stream for MergeBuffer {
    type Item = LightBuffer;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        debug!("get a sleep with {:?}", self.interval);
        thread::sleep(self.interval);
        Ok(Async::Ready(Some(self.truncate())))
    }
}

impl MergeBuffer {
    pub fn new(into: Arc<MsQueue<Line>>, interval: u64) -> MergeBuffer {
        let buf = MergeBuffer {
            interval: Duration::from_secs(interval),
            time: Arc::new(Mutex::new(TimeMap::new())),
            count: Arc::new(Mutex::new(CountMap::new())),
            gauge: Arc::new(Mutex::new(GaugeMap::new())),
        };
        (&buf).collect(into);
        buf
    }

    fn truncate(&self) -> LightBuffer {
        let mut time = self.time.lock().unwrap();
        let mut count = self.count.lock().unwrap();
        let mut gauge = self.gauge.lock().unwrap();
        let now = now();

        let mut ntime = TimeMap::new();
        mem::swap(&mut ntime, time.deref_mut());
        mem::drop(time);

        let mut ncount = CountMap::new();
        mem::swap(&mut ncount, count.deref_mut());
        mem::drop(count);

        let mut ngauge = GaugeMap::new();
        mem::swap(&mut ngauge, gauge.deref_mut());
        mem::drop(gauge);

        let time_data = LightBuffer::caculate_time(ntime);
        LightBuffer {
            timestamp: now,
            time: time_data,
            count: ncount,
            gauge: ngauge,
        }
    }

    fn collect(&self, input: Arc<MsQueue<Line>>) {
        let time = self.time.clone();
        let count = self.count.clone();
        let gauge = self.gauge.clone();
        thread::spawn(move || {
            loop {
                let item = input.pop();
                let Line { metric: m, kind: k } = item;
                match k {
                    Time(v, c) => {
                        let mut time_guard = time.lock().unwrap();
                        let tinst = time_guard.entry(m).or_insert(TimeSet(Vec::new(), 0.0));
                        tinst.0.push(v);
                        tinst.1 += c;
                    }
                    Count(v) => {
                        let mut count_guard = count.lock().unwrap();
                        let mut cinst = count_guard.entry(m).or_insert(ValueCount(0.0, 0.0));
                        cinst.0 += v;
                        cinst.1 += 1.0;
                    }
                    Gauge(v) => {
                        let mut gauge_guard = gauge.lock().unwrap();
                        *gauge_guard.entry(m).or_insert(0.0) += v;
                    }
                }
            }
        });
    }
}

pub type Result<T> = result::Result<T, StatsdError>;

#[derive(Debug)]
pub enum StatsdError {
    WrongLine,
    UnknownKind(String),
    ParseFloatError(ParseFloatError),
    FromUtf8Error(FromUtf8Error),
    IoError(Error),
}

impl From<Error> for StatsdError {
    fn from(oe: Error) -> StatsdError {
        StatsdError::IoError(oe)
    }
}

impl From<ParseFloatError> for StatsdError {
    fn from(oe: ParseFloatError) -> StatsdError {
        StatsdError::ParseFloatError(oe)
    }
}

impl From<FromUtf8Error> for StatsdError {
    fn from(oe: FromUtf8Error) -> StatsdError {
        StatsdError::FromUtf8Error(oe)
    }
}

pub struct Adapter;

impl Adapter {
    pub fn run(input: Arc<MsQueue<Line>>) {
        let merge_buffer = MergeBuffer::new(input, CONFIG.interval);
        let mut sender = BackEndSender::default();
        debug!("start an adaptor");
        sender.serve(merge_buffer);
    }
}
