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

use commands::{drop::drop, find::find, info::info, plan::plan, push::push};

#[tokio::main]
async fn main() {
    let matches = command!()
        .subcommand_required(true)
        .subcommand(
            command!("find")
                .about("Run an SQL SELECT query against the embedded database in the given bucket, using the specified WHERE clause")
                .arg(arg!(--"bucket" <BUCKET>).required(true))
                .arg(arg!(--"where" <QUERY>).required(true))
                .arg(arg!(--"endpoint" <ENDPOINT>).required(false)),
        )
        .subcommand(
            command!("info")
                .about("Print information such as hash and origin path for the given key")
                .arg(arg!(--"bucket" <BUCKET>).required(true))
                .arg(arg!(--"key" <KEY>).required(true))
                .arg(arg!(--"endpoint" <ENDPOINT>).required(false)),
        )
        .subcommand(
            command!("plan")
                .about("Generates a plan file against the specified bucket for files in the current directory. Warnings will be shown for any existing objects having the same hash as a new file in the plan")
                .arg(arg!(--"bucket" <BUCKET>).required(true))
                .arg(arg!(--"endpoint" <ENDPOINT>).required(false))
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
        .subcommand(
            command!("push")
            .about("If there is an s3b_plan.bin in the current directory, execute the plan and push any listed files to the bucket specified in the plan")
            .arg(arg!(--"endpoint" <ENDPOINT>).required(false))
        )
        .subcommand(
            command!("drop")
            .about("Delete remote object(s) at the specified path. Deletes all objects under prefix if path has a trailing slash (/)")
            .arg(arg!(--"bucket" <BUCKET>).required(true))
            .arg(arg!(--"path" <PATH>).required(true))
            .arg(arg!(--"endpoint" <ENDPOINT>).required(false))
        )
        .get_matches();

    if let Err(err) = match matches.subcommand() {
        Some(("drop", subcommand)) => drop(subcommand).await,
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
