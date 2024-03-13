use std::{
    env::current_dir, fmt::Display, fs, net::SocketAddr, path::Path, process::exit, str::FromStr,
};

use clap::{Parser, ValueEnum};
use kvs::{KvsEngine, KvsServer, Result};
use log::{error, info, warn};

// NOTE: we can also use `structopt` instead of `clap` for parsing command line arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_name = "IP:PORT", default_value = "127.0.0.1:4000", value_parser = validate_addr)]
    addr: Option<String>,

    #[arg(value_enum)]
    #[clap(short, long, value_name = "ENGINE", default_value = "kvs")]
    engine: Engine,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum Engine {
    Kvs,
    Sled,
}

impl FromStr for Engine {
    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Engine::Kvs),
            "sled" => Ok(Engine::Sled),
            _ => Err(format!("Unknown engine: {}", s)),
        }
    }

    type Err = String;
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Kvs => write!(f, "kvs"),
            Engine::Sled => write!(f, "sled"),
        }
    }
}

fn validate_addr(s: &str) -> std::result::Result<String, String> {
    const PORT_RANGE: std::ops::RangeInclusive<usize> = 1..=65535;
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid address: {}", s));
    }
    // we do not check ip address here, just check port
    let _ip = parts[0];
    let port: usize = parts[1]
        .parse()
        .map_err(|_| format!("Invalid port: {}", parts[1]))?;
    if PORT_RANGE.contains(&port) {
        Ok(s.to_string())
    } else {
        Err(format!("Invalid port: {}", port))
    }
}

fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    let args = Args::parse();
    let cwd = current_dir()?;

    check_engine(args.engine);

    info!("kvs-server startup args: {:?}", args);
    info!("kvs-server working directory: {}", cwd.display());
    info!("kvs-server version: {}", env!("CARGO_PKG_VERSION"));
    info!("kvs-server engine: {:?}", args.engine);
    info!("kvs-server listening on: {}", args.addr.clone().unwrap());

    // write engine to file named "engine" in current directory
    fs::write(current_dir()?.join("engine"), format!("{}", args.engine))?;

    let path = Path::new(&cwd);
    let socket_addr = args.addr.unwrap().parse::<SocketAddr>().unwrap();

    match args.engine {
        Engine::Kvs => start_engine(kvs::KvStore::open(&path)?, socket_addr)?,
        Engine::Sled => start_engine(kvs::SledStore::new(sled::open(&path)?), socket_addr)?,
    }

    Ok(())
}

fn start_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let server = KvsServer::new(engine);
    server.run(addr)
}

fn check_engine(target_engine: Engine) {
    match current_engine() {
        Err(e) => {
            error!("Failed to check current engine: {}", e);
            exit(1);
        }
        Ok(None) => {
            info!("No engine file found, starting with {:?}", target_engine);
        }
        Ok(Some(engine)) => {
            if target_engine != engine {
                error!(
                    "Current engine is {:?}, but you are trying to start {:?} engine",
                    engine, target_engine
                );
                exit(1);
            }
        }
    }
}

fn current_engine() -> Result<Option<Engine>> {
    let engine_file = current_dir()?.join("engine");
    if !engine_file.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine_file)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("Failed to parse engine file: {}", e);
            Ok(None)
        }
    }
}
