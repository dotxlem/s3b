mod commands;
mod s3;
mod sql;

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use anyhow::anyhow;
use aws_config::meta::region::RegionProviderChain;
use clap::{arg, command, ArgAction, ArgMatches};
use gluesql::core::sqlparser::keywords::EXISTS;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use s3::S3;
use sql::{EntriesRow, Sql};

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
    // TODO a lot of this will be handled by plan instead

    // push
    //  Fetch DB file and lock bucket
    //  Check uplist against DB for duplicates
    //  Iterate over uplist and hash with blake3 as upped; write to local DB file as upped.
    //  Upload DB file after list is complete

    // let fin = std::fs::File::open("./s3b_plan.bin").unwrap();
    // let mut decompressor = brotli::Decompressor::new(fin, 4096);
    // let mut buf: Vec<u8> = Vec::new();
    // decompressor.read_to_end(&mut buf).unwrap();
    // let plan: Plan = bincode::deserialize(&buf).unwrap();
    let plan = Plan::read();
    let mut sql = Sql::new().await?;
    for entry in &plan.entries {
        sql.put_entry(entry).await?;
    }
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

    let spinner = indicatif::ProgressBar::new_spinner();
    spinner.enable_steady_tick(Duration::from_millis(120));
    spinner.set_message("Finding files...");
    let mut filtered_entries: Vec<PathBuf> = Vec::new();
    for entry in WalkDir::new("./").min_depth(1) {
        let entry = entry.unwrap();
        let entry = entry.path().canonicalize().unwrap();
        if entry.is_file() && !entry.is_symlink() {
            if exclude.len() > 0 {
                if !filter(exclude.clone(), entry.clone()) {
                    filtered_entries.push(entry);
                }
                continue;
            }
            if include.len() > 0 {
                if filter(include.clone(), entry.clone()) {
                    filtered_entries.push(entry);
                    continue;
                }
            }
            if include.len() == 0 && exclude.len() == 0 {
                filtered_entries.push(entry);
            }
        }
    }
    let filtered_entries: Vec<PathBuf> = filtered_entries
        .into_iter()
        .unique()
        .filter(|path| {
            path.components()
                .find(|&c| c.as_os_str().to_str().unwrap() == "_s3b_db")
                .is_none()
                && path.file_name().unwrap() != "s3b_plan.bin"
        })
        .collect();
    spinner.finish_with_message(format!("Found {} entries", filtered_entries.len()));

    // TODO check for lock, lock bucket
    //      lock should be its own operation, i.e. s3b lock & s3b lock --release
    // let s3 = S3::new(&bucket_name).await?;
    // let exists = s3.key_exists("_s3b_db/db").await?;
    // println!("key_exists: {:?}", exists);
    // if exists {
    //     s3.get("_s3b_db/").await?;
    // }

    let mut sql = Sql::new().await?;
    let remote_entries = sql.get_entries().await?;

    let base_path = PathBuf::from("./").canonicalize().unwrap();
    let planned_entries: Mutex<Vec<PlanEntry>> =
        Mutex::new(Vec::with_capacity(filtered_entries.len()));
    let pb = indicatif::ProgressBar::new(filtered_entries.len() as u64);
    println!("Processing entries...");
    let num_skipped: AtomicU64 = AtomicU64::new(0);
    filtered_entries.into_par_iter().for_each(|path| {
        let metadata = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(_err) => panic!("could not stat {:?}", &path),
        };
        let modified = metadata.modified().unwrap();
        let contents = std::fs::read(&path).unwrap();
        let hash = blake3::hash(&contents).to_string();
        let key = path
            .to_str()
            .unwrap()
            .replace(&format!("{}/", base_path.to_str().unwrap()), "");
        let plan_entry = PlanEntry {
            key: key.clone(),
            path,
            hash: hash.clone(),
            modified,
        };
        // println!("key={}, hash={}", key.clone(), hash.clone());

        // if plan_entries.lock().unwrap().iter().find(|e| e.key == plan_entry.key).is_some() {
        //     println!("dupe={:?}", &plan_entry.key);
        // }
        // TODO how much checking against the DB does `plan` do?
        //      - identical hash & key should be skipped
        //      - identical hash at different keys should be highlighted
        //      - different hash at same key should be prompted for options, check modified time
        // TODO how are identical keys from multiple base paths (origins) handled?
        //      - emergent from above; prompt if different hash should show base path & modified time
        //      - opting to keep both should append the base path somehow to the key? or is keeping both not possible?
        //          - if not possible, options are skip or overwrite
        // if let None = remote_entries.iter().find(|&e| e.hash.eq(&hash.clone()) && e.key.eq(&key.clone())) {
        //     planned_entries.lock().unwrap().push(plan_entry);
        // } else {
        //     num_skipped.fetch_add(1, Ordering::Relaxed);
        // }

        let mut modified_key: Option<&EntriesRow> = None;
        let mut existing_hashes: Vec<&EntriesRow> = Vec::new();
        let this_hash = hash.clone();
        let this_key = key.clone();
        remote_entries.iter().for_each(|remote| {
            if remote.hash == this_hash {
                existing_hashes.push(remote);
            }
            if remote.key == this_key && remote.hash != this_hash {
                modified_key = Some(remote);
            }
        });
        let mut skip = false;
        if let Some(remote) = modified_key {
            // different hash at same key, prompt
            let this_modified_time = modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            if remote.modified > this_modified_time {
                println!("Key {} exists, remote is newer", &remote.key);
            } else if remote.modified < this_modified_time {
                println!("Key {} exists, remote is older", &remote.key);
            } else {
                println!(
                    "Key {} exists with different hash but timestamp is same; this may be a bug",
                    &remote.key
                );
            }
        }
        if existing_hashes
            .iter()
            .find(|&remote| remote.key == this_key)
            .is_some()
        {
            // key exists with same hash, skip
            println!("Key {} exists with same hash", &this_key);
            skip = true;
        }
        if !skip && existing_hashes.len() > 0 {
            // not skipped but identical hashes found, flag
            println!(
                "Key {} does not exist but remote objects with identical hashes found: {:?}",
                &this_key, existing_hashes
            );
        }
        if skip && existing_hashes.len() > 1 {
            // skipped but there are multiple identical hashes, flag
            println!(
                "Key {} skipped but remote objects with identical hashes found: {:?}",
                &this_key,
                existing_hashes
                    .iter()
                    .filter(|e| e.key != this_key)
                    .collect::<Vec<_>>()
            );
        }

        if !skip {
            planned_entries.lock().unwrap().push(plan_entry);
        } else {
            num_skipped.fetch_add(1, Ordering::Relaxed);
        }

        pb.inc(1);
    });
    pb.finish_with_message("done");
    println!(
        "Skipped {} existing, umodified entries",
        num_skipped.load(Ordering::Relaxed)
    );

    let entries = planned_entries.into_inner().unwrap();

    let plan = Plan { base_path, entries };
    println!("plan={:?}", plan);
    plan.write();
    // let plan_bytes = bincode::serialize(&plan).unwrap();
    // let fout = std::fs::File::create("./s3b_plan.bin").unwrap();
    // let mut compressor = brotli::CompressorWriter::new(fout, 4096, 11, 22);
    // compressor.write_all(&plan_bytes).unwrap();

    Ok(())
}

fn filter(list: Vec<&String>, entry: PathBuf) -> bool {
    for l in list {
        if entry.to_str().unwrap().contains(l.as_str()) {
            return true;
        }
        // if entry
        //     .components()
        //     .find(|&c| c.as_os_str().to_str().unwrap() == l.as_str())
        //     .is_some()
        // {
        //     return true;
        // }
    }
    return false;
}

#[derive(Debug, Serialize, Deserialize)]
struct Plan {
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

#[derive(Debug, Serialize, Deserialize)]
struct PlanEntry {
    key: String,
    path: PathBuf,
    hash: String,
    modified: SystemTime,
}
