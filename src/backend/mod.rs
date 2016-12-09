pub mod graphite;
pub mod banshee;

use std::io::{Error, Write};
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
    fn counting(&self, ts: u64, count: CountData, buf: &mut Vec<u8>);
    fn gauging(&self, ts: u64, gauge: GaugeData, buf: &mut Vec<u8>);
    fn timing(&self, ts: u64, time: TimeData, buf: &mut Vec<u8>);

    /// auto apply function
    fn apply(&mut self, light: LightBuffer) -> Vec<u8> {
        let LightBuffer { timestamp: ts, time, count, gauge } = light;
        let mut buffer = Vec::new();
        self.counting(ts, count, &mut buffer);
        self.gauging(ts, gauge, &mut buffer);
        self.timing(ts, time, &mut buffer);
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
            let graphite_buf = self.graphite.apply(item.clone());
            let graphite = TcpStream::connect(&graphite_addr, &handle)
                .and_then(move |mut socket| socket.write_all(&graphite_buf))
                .or_else(|err| {
                    error!("unexcept error: {:?}", err);
                    Err(())
                });

            let banshee_buf = self.banshee.apply(item);
            let banshee = TcpStream::connect(&banshee_addr, &handle)
                .and_then(move |mut socket| socket.write_all(&banshee_buf))
                .or_else(|err| {
                    error!("unexcept error: {:?}", err);
                    Err(())
                });
            handle.spawn(graphite.join(banshee).and_then(|_| Ok(())));
            Ok(())
        });
        core.run(service).unwrap();
    }
}
