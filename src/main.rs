mod commands;
mod s3;
mod sql;

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::anyhow;
use aws_config::meta::region::RegionProviderChain;
use clap::{arg, command, ArgAction, ArgMatches};
use gluesql::core::sqlparser::keywords::EXISTS;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use s3::S3;
use sql::Sql;

#[tokio::main]
async fn main() {
    let matches = command!()
        .arg(arg!(--"bucket" <BUCKET>).required(true))
        .subcommand_required(true)
        .subcommand(command!("init").arg(arg!(--"create").action(ArgAction::SetTrue)))
        .subcommand(
            command!("plan")
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

    let bucket_name = matches.get_one::<String>("bucket").unwrap();
    if let Err(err) = match matches.subcommand() {
        Some(("init", subcommand)) => init(subcommand, bucket_name).await,
        Some(("plan", subcommand)) => plan(subcommand, bucket_name).await,
        Some(("push", subcommand)) => push(subcommand, bucket_name).await,
        _ => unreachable!("skipper's drunk!"),
    } {
        println!("ERROR: {:?}", err);
    }
}

async fn init(matches: &ArgMatches, bucket_name: &String) -> anyhow::Result<()> {
    // init
    //  Check bucket for DB file; if not present, create & upload

    // let bucket_name = matches.get_one::<String>("bucket").unwrap();
    let create_infra = matches.get_flag("create");
    println!("Bucket: {:?}", &bucket_name);
    println!("Create: {:?}", create_infra);

    let s3 = S3::new(&bucket_name).await?;

    // does bucket have db?
    //  if so, pull it
    let exists = s3.key_exists("_s3b_db/db").await?;
    println!("key_exists: {:?}", exists);
    if exists {
        s3.get("_s3b_db/").await?;
    }

    // let mut sql = Sql::new();
    // sql.init().await?;
    // if !exists {
    //     s3.put(Path::new("_s3b_db")).await?;
    // }

    // sql.add_origin("xlemovo").await?;
    // sql.add_origin("xlemstation").await?;
    // sql.get_origins()
    //     .await?
    //     .iter()
    //     .for_each(|h| println!("hostname={:?}", h.hostname));

    // does lock table exist?
    //   if not, create it?
    //     if not created then exit
    // let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    // let config = aws_config::from_env().region(region_provider).load().await;
    // let ddb = aws_sdk_dynamodb::Client::new(&config);
    // let paginator = ddb.list_tables().into_paginator().items().send();
    // let tables = paginator.collect::<Result<Vec<_>, _>>().await?;
    // for name in &tables {
    //     println!("table: {}", name);
    // }

    Ok(())
}

async fn push(matches: &ArgMatches, bucket_name: &String) -> anyhow::Result<()> {
    // push
    //  Fetch DB file and lock bucket
    //  Check uplist against DB for duplicates
    //  Iterate over uplist and hash with blake3 as upped; write to local DB file as upped.
    //  Upload DB file after list is complete
    let fin = std::fs::File::open("./s3b_plan.bin").unwrap();
    let mut decompressor = brotli::Decompressor::new(fin, 4096);
    let mut buf: Vec<u8> = Vec::new();
    decompressor.read_to_end(&mut buf).unwrap();
    let plan: Plan = bincode::deserialize(&buf).unwrap();
    println!("{:?}", plan);
    Ok(())
}

async fn plan(matches: &ArgMatches, bucket_name: &String) -> anyhow::Result<()> {
    use rayon::prelude::*;

    let exclude: Vec<&String> = match matches.get_many("exclude") {
        Some(m) => m.collect(),
        None => Vec::new(),
    };

    let include: Vec<&String> = match matches.get_many("include") {
        Some(m) => m.collect(),
        None => Vec::new(),
    };

    println!("exclude: {:?}", exclude);
    println!("include: {:?}", include);
    if include.len() > 0 && exclude.len() > 0 {
        return Err(anyhow!("exclude and include flags are mutually exclusive"));
    }

    let s3 = S3::new(&bucket_name).await?;
    let exists = s3.key_exists("_s3b_db/db").await?;
    println!("key_exists: {:?}", exists);
    if exists {
        s3.get("_s3b_db/").await?;
    }

    let mut sql = Sql::new();
    sql.init().await?;
    let entries = sql.get_entries().await?;
    for entry in entries {
        println!("remote = {}", entry.id);
    }

    let spinner = indicatif::ProgressBar::new_spinner();
    spinner.enable_steady_tick(Duration::from_millis(120));
    spinner.set_message("Finding files...");
    let mut planned_entries: Vec<PathBuf> = Vec::new();
    for entry in WalkDir::new("./").min_depth(1) {
        let entry = entry.unwrap();
        let entry = entry.path().canonicalize().unwrap();
        if exclude.len() > 0 {
            if !filter(exclude.clone(), entry.clone()) {
                planned_entries.push(entry);
            }
            continue;
        }
        if include.len() > 0 {
            if filter(include.clone(), entry.clone()) {
                planned_entries.push(entry);
                continue;
            }
        }
        if include.len() == 0 && exclude.len() == 0 {
            planned_entries.push(entry);
        }
    }
    spinner.finish_with_message(format!("Found {} entries", planned_entries.len()));

    let plan_entries: Mutex<Vec<PlanEntry>> = Mutex::new(Vec::with_capacity(planned_entries.len()));
    let pb = indicatif::ProgressBar::new(planned_entries.len() as u64);
    planned_entries.par_iter().for_each(|entry| {
        if entry.is_file() {
            let contents = std::fs::read(entry.clone()).unwrap();
            let plan_entry = PlanEntry {
                path: entry.clone(),
                hash: blake3::hash(&contents).to_string(),
            };

            plan_entries.lock().unwrap().push(plan_entry);
            pb.inc(1);
        }
    });

    let plan = Plan { base_path: PathBuf::from("./").canonicalize().unwrap(), entries: plan_entries.into_inner().unwrap() };
    let plan_bytes = bincode::serialize(&plan).unwrap();
    let fout = std::fs::File::create("./s3b_plan.bin").unwrap();
    let mut compressor = brotli::CompressorWriter::new(fout, 4096, 11, 22);
    compressor.write_all(&plan_bytes).unwrap();

    pb.finish_with_message("done");
    Ok(())
}

fn filter(list: Vec<&String>, entry: PathBuf) -> bool {
    for l in list {
        if entry
            .components()
            .find(|&c| c.as_os_str().to_str().unwrap() == l.as_str())
            .is_some()
        {
            return true;
        }
    }
    return false;
}

#[derive(Debug, Serialize, Deserialize)]
struct Plan {
    base_path: PathBuf,
    entries: Vec<PlanEntry>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PlanEntry {
    path: PathBuf,
    hash: String,
}
