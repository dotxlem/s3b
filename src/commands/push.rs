use std::path::Path;

use clap::ArgMatches;
use colored::Colorize;

use crate::{Plan, Sql, S3};

pub async fn push(matches: &ArgMatches) -> anyhow::Result<()> {
    let endpoint = matches.get_one::<String>("endpoint");
    let plan = Plan::read();
    let num_entries = plan.entries.len();
    let bucket_name = plan.bucket_name;

    println!(
        "Pushing {} objects to bucket {}...",
        num_entries, &bucket_name
    );

    // TODO check for lock
    //      lock should be its own operation, i.e. s3b lock & s3b lock --release
    let s3 = S3::new(&bucket_name, endpoint.map(|s| s.as_str())).await?;
    let exists = s3.key_exists("_s3b_db/entries.sql").await?;
    if exists {
        s3.get("_s3b_db/").await?;
    }

    let mut sql = Sql::new().await?;
    let remote_entries = sql.get_entries().await?;
    let pb = indicatif::ProgressBar::new(num_entries as u64);
    for entry in &plan.entries {
        s3.put(Path::new(&entry.key)).await?;
        match remote_entries.iter().find(|&e| e.key == entry.key) {
            Some(_) => sql.update_entry(entry).await?,
            None => sql.put_entry(entry).await?,
        };
        pb.inc(1);
    }
    pb.finish();
    println!(
        "{}",
        format!("Done! Uploaded {} objects.", num_entries).green()
    );

    s3.put(Path::new("_s3b_db/")).await?;
    std::fs::remove_dir_all("_s3b_db").unwrap();
    std::fs::remove_file("s3b_plan.bin").unwrap();

    Ok(())
}
