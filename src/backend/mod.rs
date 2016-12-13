pub mod graphite;
pub mod banshee;

use std::io::{Write, ErrorKind, Result};
use std::net::SocketAddr;
use std::time::Duration;
use std::thread;

use futures::Future;
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
    pub fn serve(&mut self, input: MergeBuffer) {
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let graphite_addr = &CONFIG.graphite.address.parse::<SocketAddr>().unwrap();
        let banshee_addr = &CONFIG.banshee.address.parse::<SocketAddr>().unwrap();
        let dur = Duration::from_secs(CONFIG.interval);
        loop {
            thread::sleep(dur);
            let item = input.truncate();
            let graphite_buf = self.graphite.apply(&item);
            let graphite = TcpStream::connect(&graphite_addr, &handle).and_then(|socket| {
                info!("get a new connection");
                send_to(socket, graphite_buf)
            });

            let banshee_buf = self.banshee.apply(&item);
            let banshee = TcpStream::connect(&banshee_addr, &handle).and_then(|socket| {
                info!("get a new connection");
                send_to(socket, banshee_buf)
            });
            let service = graphite.join(banshee);
            core.run(service).unwrap();
        }
    }
}


fn send_to(mut socket: TcpStream, buf: Vec<u8>) -> Result<()> {
    info!("want to write the buffer");
    if buf.len() == 0 {
        info!("but get a zero len of buffer");
        return Ok(());
    }
    loop {
        info!("write back all the value");
        let ret = match socket.write_all(&buf) {
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => continue,
            var => {
                info!("get a result {:?}", &var);
                var
            }
        };
        return ret;
    }
}
