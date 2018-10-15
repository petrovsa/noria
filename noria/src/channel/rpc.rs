use async_bincode;
use bincode;
use net2;
use serde::{Deserialize, Serialize};
use std;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use tokio::prelude::*;

pub struct RpcClient<Q, R> {
    stream: async_bincode::AsyncBincodeStream<
        tokio::net::TcpStream,
        R,
        Q,
        async_bincode::AsyncDestination,
    >,
    // TODO: is_local(?)
}

impl<Q, R> RpcClient<Q, R> {
    fn new(stream: std::net::TcpStream) -> Result<Self, io::Error> {
        stream.set_nodelay(true)?;
        Ok(Self {
            stream: stream.into(),
        })
    }

    pub fn connect_from(sport: Option<u16>, addr: &SocketAddr) -> Result<Self, io::Error> {
        let s = net2::TcpBuilder::new_v4()?
            .reuse_address(true)?
            .bind((Ipv4Addr::UNSPECIFIED, sport.unwrap_or(0)))?
            .connect(addr)?;
        Self::new(s)
    }

    pub fn connect(addr: &SocketAddr) -> Result<Self, io::Error> {
        Self::connect_from(None, addr)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.get_ref().local_addr()
    }
}

impl<Q, R> RpcClient<Q, R>
where
    Q: Serialize,
    for<'de> R: Deserialize<'de>,
{
    pub fn call(self, q: Q) -> impl Future<Item = (Option<R>, Self), Error = bincode::Error> {
        self.send(q)
            .and_then(|s| Stream::into_future(s).map_err(|(e, _)| e))
    }
}

impl<Q, R> Sink for RpcClient<Q, R>
where
    Q: Serialize,
{
    type SinkItem = Q;
    type SinkError = bincode::Error;

    fn start_send(
        &mut self,
        item: Self::SinkItem,
    ) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        self.stream.start_send(item)
    }
    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        self.stream.poll_complete()
    }
}

impl<Q, R> Stream for RpcClient<Q, R>
where
    for<'de> R: Deserialize<'de>,
{
    type Item = R;
    type Error = bincode::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.stream.poll()
    }
}

#[derive(Debug)]
pub enum RpcSendError {
    SerializationError(bincode::Error),
    Disconnected,
    StillNeedsFlush,
}
