use std::{process::exit};

use clap::{Parser, Subcommand};

use kvs::{KvsClient, KvsError, Result};
use log::debug;

// NOTE: we can also use `structopt` instead of `clap` for parsing command line arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Command,

    #[clap(short, long, value_name = "IP:PORT", default_value = "127.0.0.1:4000", value_parser = validate_addr)]
    addr: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Command {
    Set {
        key: String,
        value: String,
    },
    Get {
        key: String,
    },
    #[clap(alias = "rm")]
    Remove {
        key: String,
    },
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
        .filter_level(log::LevelFilter::Debug)
        .init();
    let args = Args::parse();

    // let log_file = format!("{}/rust/kvs/kvs.log", env!("HOME"));
    // let log_file = current_dir().unwrap();
    // let mut kv_store = kvs::KvStore::open(std::path::Path::new(&log_file))?;

    let mut cli = KvsClient::connect(args.addr.unwrap())?;

    match args.command {
        Command::Set { key, value } => {
            debug!("set key: {}, value: {}", key, value);
            cli.set(key, value)?;
            Ok(())
        }
        Command::Get { key } => {
            debug!("get key: {}", key);
            match cli.get(key)? {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
            Ok(())
        }
        Command::Remove { key } => {
            debug!("remove key: {}", key);
            match cli.remove(key) {
                Ok(()) => Ok(()),
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => Err(e),
            }
        }
    }
}
