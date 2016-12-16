pub mod graphite;
pub mod banshee;

use std::io::{Write, ErrorKind, Result, Error};
use std::net::SocketAddr;
use std::time::{Instant, Duration};
use std::thread;

use futures::{self, Future};
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use worker::{LightBuffer, TimeData, CountData, GaugeData, MergeBuffer};

use self::graphite::Graphite;
use self::banshee::Banshee;
use ::CONFIG;

pub trait BackEnd {
    fn counting(&self, ts: u64, count: &CountData, buf: &mut Vec<u8>);
    fn gauging(&self, ts: u64, gauge: &GaugeData, buf: &mut Vec<u8>);
    fn timing(&self, ts: u64, time: &TimeData, buf: &mut Vec<u8>);

    fn validate(&self) -> bool {
        debug!("validate backend");
        true
    }

    /// auto apply function
    fn apply(&mut self, light: &LightBuffer) -> Vec<u8> {
        let mut buffer = Vec::new();
        if !self.validate() {
            return buffer;
        }
        let ts = light.timestamp;
        self.counting(ts, &light.count, &mut buffer);
        self.gauging(ts, &light.gauge, &mut buffer);
        self.timing(ts, &light.time, &mut buffer);
        buffer
    }
}

pub struct BackEndSender {
    banshee: Banshee,
    graphite: Graphite,
}

impl Default for BackEndSender {
    fn default() -> Self {
        BackEndSender {
            banshee: Banshee::default(),
            graphite: Graphite::default(),
        }
    }
}

impl BackEndSender {
    pub fn serve(&mut self, input: &MergeBuffer) {
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let graphite_addr = &CONFIG.graphite.address.parse::<SocketAddr>().unwrap();
        let banshee_addr = &CONFIG.banshee.address.parse::<SocketAddr>().unwrap();
        let dur = Duration::from_secs(CONFIG.interval);
        let mut last = Instant::now();
        // hard code and ugly way to implment it

        loop {
            let now = Instant::now();
            let ndur = now.duration_since(last);
            if ndur < dur {
                thread::sleep(ndur);
                continue;
            }
            last = now;
            let item = input.truncate();
            let graphite_buf = self.graphite.apply(&item);
            let graphite_empty = graphite_buf.is_empty();
            let graphite_service = TcpStream::connect(&graphite_addr, &handle).and_then(|socket| {
                debug!("get a new connection");
                send_to(socket, graphite_buf)
            });
            let graphite = futures::lazy(|| if !graphite_empty {
                    Ok(())
                } else {
                    Err(Error::new(ErrorKind::Other, "empty graphite buffer"))
                })
                .and_then(|_| graphite_service);

            let banshee_buf = self.banshee.apply(&item);
            let banshee_empty = banshee_buf.is_empty();
            let banshee_service = TcpStream::connect(&banshee_addr, &handle).and_then(|socket| {
                debug!("get a new connection");
                send_to(socket, banshee_buf)
            });

            let banshee = futures::lazy(|| if !banshee_empty {
                    Ok(())
                } else {
                    Err(Error::new(ErrorKind::Other, "empty banshee buffer"))
                })
                .and_then(|_| banshee_service);

            let service = graphite.join(banshee);
            match core.run(service) {
                Ok(_) => {}
                Err(ref err) if err.kind() == ErrorKind::Other => {
                    debug!("empty buffer, skip");
                }
                Err(err) => {
                    error!("unknown error when send to backend, error: {}", err);
                }
            };
        }
    }
}


fn send_to(mut socket: TcpStream, buf: Vec<u8>) -> Result<()> {
    debug!("want to write the buffer");
    if buf.len() == 0 {
        debug!("but get a zero len of buffer");
        return Ok(());
    }
    loop {
        debug!("write back all the value");
        let ret = match socket.write_all(&buf) {
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => continue,
            var => {
                debug!("get a result {:?}", &var);
                var
            }
        };
        return ret;
    }
}
