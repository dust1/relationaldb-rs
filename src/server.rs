use crate::error::{Error, Result};

use ::log::{error, info};
use futures::sink::SinkExt as _;
use serde_derive::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt as _;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct Server {
    listener: Option<TcpListener>,
}

pub struct Session {}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Execute(String),
    GetTable(String),
    ListTables,
    Status,
}
/// A server response.
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    ListTables(Vec<String>),
}

impl Server {
    pub async fn new() -> Result<Self> {
        Ok(Server { listener: None })
    }

    pub async fn listen(mut self) -> Result<Self> {
        let (listener,) = tokio::try_join!(TcpListener::bind("127.0.0.1:9601"),)?;
        self.listener = Some(listener);
        Ok(self)
    }

    pub async fn serve(self) -> Result<()> {
        let listener = self
            .listener
            .ok_or_else(|| Error::Internal("Must listen before serving".to_string()))?;

        tokio::try_join!(Self::serve_sql(listener),)?;
        Ok(())
    }

    async fn serve_sql(listener: TcpListener) -> Result<()> {
        let mut listener = TcpListenerStream::new(listener);
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let session = Session::new()?;
            tokio::spawn(async move {
                info!("Client {} connected!", peer);
                match session.handle(socket).await {
                    Ok(()) => info!("Client {} disconnected", peer),
                    Err(err) => error!("Client {} error: {}", peer, err),
                }
            });
        }

        Ok(())
    }
}

impl Session {
    fn new() -> Result<Self> {
        Ok(Session {})
    }

    async fn handle(mut self, socket: TcpStream) -> Result<()> {
        let mut stream = tokio_serde::Framed::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::Bincode::default(),
        );
        while let Some(request) = stream.try_next().await? {
            let response = tokio::task::block_in_place(|| self.request(request));
            let rows: Box<dyn Iterator<Item = Result<Response>> + Send> =
                Box::new(std::iter::empty());
            stream.send(response).await?;
            stream.send_all(&mut tokio_stream::iter(rows.map(Ok))).await?;
        }
        Ok(())
    }

    pub fn request(&mut self, _request: Request) -> Result<Response> {
        todo!()
    }
}
