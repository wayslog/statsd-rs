pub mod graphite;
pub mod banshee;

use std::io::{Error, Write, ErrorKind, Result};
use std::net::SocketAddr;

use futures::stream::Stream;
use futures::Future;
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use worker::{LightBuffer, TimeData, CountData, GaugeData};

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
    pub fn serve<S>(&mut self, input: S)
        where S: Stream<Item = LightBuffer, Error = Error>
    {
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let graphite_addr = &CONFIG.graphite.address.parse::<SocketAddr>().unwrap();
        let banshee_addr = &CONFIG.banshee.address.parse::<SocketAddr>().unwrap();

        let service = input.for_each(|item| {
            if self.graphite.validate() {
                let graphite_buf = self.graphite.apply(&item);
                let graphite = TcpStream::connect(&graphite_addr, &handle)
                    .and_then(move |socket| send_to(socket, &graphite_buf))
                    .or_else(|err| Err(error!("unexcept error: {:?}", err)));

                handle.spawn(graphite.and_then(|_| Ok(())));
            }
            if self.banshee.validate() {
                let banshee_buf = self.banshee.apply(&item);
                let banshee = TcpStream::connect(&banshee_addr, &handle)
                    .and_then(move |socket| send_to(socket, &banshee_buf))
                    .or_else(|err| Err(error!("unexcept error: {:?}", err)));
                handle.spawn(banshee.and_then(|_| Ok(())));
            }

            Ok(())
        });
        core.run(service).unwrap();
    }
}

fn send_to(mut socket: TcpStream, buf: &[u8]) -> Result<()> {
    if buf.len() == 0 {
        return Ok(());
    }
    loop {
        let ret = match socket.write_all(buf) {
            Ok(n) => Ok(n),
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => continue,
            Err(err) => Err(err),
        };
        return ret;
    }
}
