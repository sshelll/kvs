use std::process::exit;

use clap::{Parser, Subcommand};

use kvs::KvsError;

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

fn main() -> Result<(), KvsError> {
    let args = Args::parse();

    let log_file = format!("{}/kvs.log", env!("HOME"));
    let mut kv_store = kvs::KvStore::open(std::path::Path::new(&log_file))?;

    match args.command {
        Command::Set { key, value } => {
            kv_store.set(key, value)?;
            Ok(())
        }
        Command::Get { key } => {
            eprintln!("unimplemented, cmd: get {}", key);
            exit(1);
        }
        Command::Remove { key } => {
            kv_store.remove(key)?;
            Ok(())
        }
    }
}
