use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::net::ToSocketAddrs;

use log::debug;
use log::error;
use serde_json::Deserializer;

use crate::protocol::GetResponse;
use crate::protocol::RemoveResponse;
use crate::protocol::Request;
use crate::protocol::SetResponse;
use crate::KvsEngine;
use crate::Result;

/// The server of key-value store.
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

/// Implement the server of key-value store.
impl<E: KvsEngine> KvsServer<E> {
    /// Create a new server with the given storage engine.
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    /// Run the server with the given address.
    pub fn run<A: ToSocketAddrs>(mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.serve(stream) {
                        error!("starting server error: {}", e);
                    }
                }
                Err(e) => error!("connection failed: {}", e),
            }
        }
        Ok(())
    }

    fn serve(&mut self, conn: TcpStream) -> Result<()> {
        let cli_addr = conn.peer_addr()?;
        let reader = BufReader::new(&conn);
        let mut writer = BufWriter::new(&conn);
        let req_reader = Deserializer::from_reader(reader).into_iter::<Request>();

        macro_rules! send_resp {
            ($resp:expr) => {{
                let resp = $resp;
                serde_json::to_writer(&mut writer, &resp)?;
                writer.flush()?;
                debug!("Response sent to {}: {:?}", cli_addr, resp);
            }};
        }

        for req in req_reader {
            let req = req?;
            debug!("Receive request from {}: {:?}", cli_addr, req);
            match req {
                Request::Get { key } => send_resp!(match self.engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(format!("{}", e)),
                }),
                Request::Set { key, value } => send_resp!(match self.engine.set(key, value) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(format!("{}", e)),
                }),
                Request::Remove { key } => send_resp!(match self.engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(format!("{}", e)),
                }),
            };
        }

        Ok(())
    }
}
