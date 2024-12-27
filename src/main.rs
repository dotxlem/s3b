mod commands;
mod s3;
mod sql;

use std::io::{Read, Write};
use std::path::PathBuf;

use clap::{arg, command};
use colored::Colorize;
use serde::{Deserialize, Serialize};

use s3::S3;
use sql::Sql;

use commands::{find::find, info::info, plan::plan, push::push};

#[tokio::main]
async fn main() {
    let matches = command!()
        .subcommand_required(true)
        .subcommand(
            command!("find")
                .arg(arg!(--"bucket" <BUCKET>).required(true))
                .arg(arg!(--"where" <QUERY>).required(true)),
        )
        .subcommand(
            command!("info")
                .arg(arg!(--"bucket" <BUCKET>).required(true))
                .arg(arg!(--"key" <KEY>).required(true)),
        )
        .subcommand(
            command!("plan")
                .arg(arg!(--"bucket" <BUCKET>).required(true))
                .arg(
                    arg!(--"exclude" <EXCLUDE>)
                        .value_delimiter(' ')
                        .num_args(1..),
                )
                .arg(
                    arg!(--"include" <INCLUDE>)
                        .value_delimiter(' ')
                        .num_args(1..),
                ),
        )
        .subcommand(command!("push"))
        .get_matches();

    if let Err(err) = match matches.subcommand() {
        Some(("find", subcommand)) => find(subcommand).await,
        Some(("info", subcommand)) => info(subcommand).await,
        Some(("plan", subcommand)) => plan(subcommand).await,
        Some(("push", subcommand)) => push(subcommand).await,
        _ => unreachable!("skipper's drunk!"),
    } {
        println!("{}", format!("ERROR: {:?}", err).red());
    }
}



#[derive(Debug, Serialize, Deserialize)]
pub struct Plan {
    bucket_name: String,
    base_path: PathBuf,
    entries: Vec<PlanEntry>, // TODO this might be more efficient as a map
}

impl Plan {
    fn read() -> Self {
        let fin = std::fs::File::open("./s3b_plan.bin").unwrap();
        let mut decompressor = brotli::Decompressor::new(fin, 4096);
        let mut buf: Vec<u8> = Vec::new();
        decompressor.read_to_end(&mut buf).unwrap();
        bincode::deserialize(&buf).unwrap()
    }

    fn write(&self) {
        let plan_bytes = bincode::serialize(&self).unwrap();
        let fout = std::fs::File::create("./s3b_plan.bin").unwrap();
        let mut compressor = brotli::CompressorWriter::new(fout, 4096, 11, 22);
        compressor.write_all(&plan_bytes).unwrap();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanEntry {
    key: String,
    path: PathBuf,
    hash: String,
    modified: u64,
}
