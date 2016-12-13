extern crate futures;
extern crate tokio_core;
#[macro_use]
extern crate log;

extern crate env_logger;

use futures::{Future, Async};
use futures::stream::Stream;
use tokio_core::reactor::Core;
use tokio_core::io::Io;
use tokio_core::net::TcpListener;

use std::env;
use std::io::{Read, BufReader, ErrorKind};
use std::net::SocketAddr;
use std::thread;

fn main() {
    env_logger::init().unwrap();
    let args = env::args();
    let address: Vec<_> = args.skip(1)
        .next()
        .unwrap()
        .split(",")
        .map(|x| x.parse::<SocketAddr>().expect(&format!("invalidate ip level address {:?}", x)))
        .collect();
    let mut jhs = Vec::new();
    for addr in address {
        let jh = thread::spawn(move || {
            work(&addr);
        });
        jhs.push(jh);
    }

    for jh in jhs {
        jh.join().unwrap();
    }
}

fn read_line<R: Read + Sized>(r: &mut R) -> bool {
    let mut line = Vec::new();

    match r.read(&mut line) {
        Ok(size) => {
            if size == 0 {
                info!("get a message as ---{}--- with {} bytes",
                      String::from_utf8_lossy(&line),
                      size);
                false
            } else {
                info!("get a message as ---{}--- with {} bytes",
                      String::from_utf8_lossy(&line),
                      size);
                true
            }
        }
        Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
            debug!("get a wouldblock error");
            false
        }
        Err(err) => {
            panic!("unexpect error {:?}", err);
        }
    }
}

fn work(addr: &SocketAddr) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let socket = TcpListener::bind(&addr, &handle).unwrap();
    let service = socket.incoming().for_each(|(sock, src_addr)| {
        debug!("new connection occur from {:?}", src_addr);
        let lazy = futures::lazy(|| Ok(sock.split()));
        let amt = lazy.and_then(|(mut reader, _writer)| {
            loop {
                if let Async::NotReady = reader.poll_read() {
                    continue;
                } else {
                    break;
                }
            }
            read_line(&mut reader);
            Ok(())
        });
        handle.spawn(amt);
        Ok(())
    });
    core.run(service).unwrap();
}