use std::{env::current_dir, process::exit};

use clap::{Parser, Subcommand};

use kvs::{KvsError, Result};

// NOTE: we can also use `structopt` instead of `clap` for parsing command line arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Command,
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

fn main() -> Result<()> {
    let args = Args::parse();

    // let log_file = format!("{}/rust/kvs/kvs.log", env!("HOME"));
    let log_file = current_dir().unwrap();
    let mut kv_store = kvs::KvStore::open(std::path::Path::new(&log_file))?;

    match args.command {
        Command::Set { key, value } => {
            eprintln!("[DEBUG] set key: {}, value: {}", key, value);
            kv_store.set(key, value)?;
            Ok(())
        }
        Command::Get { key } => {
            eprintln!("[DEBUG] get key: {}", key);
            match kv_store.get(key) {
                Ok(Some(value)) => {
                    println!("{}", value);
                    Ok(())
                }
                Ok(None) => {
                    println!("{}", KvsError::KeyNotFound);
                    Ok(())
                }
                Err(e) => {
                    println!("{}", e);
                    exit(1);
                }
            }
        }
        Command::Remove { key } => {
            eprintln!("[DEBUG] remove key: {}", key);
            match kv_store.remove(key) {
                Ok(_) => Ok(()),
                Err(e) => {
                    println!("{}", e);
                    exit(1);
                }
            }
        }
    }
}
