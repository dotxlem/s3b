mod commands;
mod s3;
mod sql;

use aws_config::meta::region::RegionProviderChain;
use clap::{arg, command, ArgAction, ArgMatches};
use gluesql::core::sqlparser::keywords::EXISTS;
use std::path::Path;

use s3::S3;
use sql::Sql;

#[tokio::main]
async fn main() {
    let matches = command!()
        .subcommand_required(true)
        .subcommand(
            command!("init")
                .arg(arg!(--"bucket" <BUCKET>).required(true))
                .arg(arg!(--"create").action(ArgAction::SetTrue)),
        )
        .subcommand(command!("up"))
        .get_matches();

    if let Err(err) = match matches.subcommand() {
        Some(("init", matches)) => init(matches).await,
        Some(("up", matches)) => up(matches).await,
        _ => unreachable!("skipper's drunk!"),
    } {
        println!("ERROR: {:?}", err);
    }

    // init
    //  Check bucket for DB file; if not present, create & upload

    // up
    //  Get path
    //  Walkdir from path & enumerate files to upload
    //  Fetch DB file and lock bucket
    //  Check uplist against DB for duplicates
    //  Iterate over uplist and hash with blake3 as upped; write to local DB file as upped.
    //  Upload DB file after list is complete
}

async fn init(matches: &ArgMatches) -> anyhow::Result<()> {
    let bucket_name = matches.get_one::<String>("bucket").unwrap();
    let create_infra = matches.get_flag("create");
    println!("Bucket: {:?}", &bucket_name);
    println!("Create: {:?}", create_infra);

    let s3 = S3::new(&bucket_name).await?;

    // does bucket have db?
    //  if so, pull it
    let exists = s3.key_exists("_s3b_db/db").await?;
    println!("key_exists: {:?}", exists);
    if exists {
        // s3.get("_s3b_db/").await?;
    }

    let mut sql = Sql::new();
    sql.init().await?;
    if !exists {
        s3.put(Path::new("_s3b_db")).await?;
    }

    sql.add_hostname("xlemovo").await?;
    sql.add_hostname("xlemstation").await?;
    sql.get_hostnames().await;

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

async fn up(matches: &ArgMatches) -> anyhow::Result<()> {
    Ok(())
}
