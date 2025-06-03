use chrono::{prelude::DateTime, Utc};
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use clap::ArgMatches;
use cli_table::{Cell, Style, Table};

use crate::{Sql, S3};

pub async fn find(matches: &ArgMatches) -> anyhow::Result<()> {
    let bucket_name = matches.get_one::<String>("bucket").unwrap();
    let endpoint = matches.get_one::<String>("endpoint");
    let wherestr = matches.get_one::<String>("where").unwrap();

    // TODO check for lock
    //      lock should be its own operation, i.e. s3b lock & s3b lock --release
    let s3 = S3::new(&bucket_name, endpoint.map(|s| s.as_str())).await?;
    let exists = s3.key_exists("_s3b_db/entries.sql").await?;
    if exists {
        s3.get("_s3b_db/").await?;
    }

    let mut sql = Sql::new().await?;
    let remote_entries = sql.get_entries_where(&wherestr).await?;
    let table = remote_entries
        .iter()
        .map(|entry| {
            let modified_time =
                DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_secs(entry.modified))
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string();
            vec![
                entry.key.clone().cell(),
                entry.hash.clone().cell(),
                entry.path.clone().cell(),
                modified_time.cell(),
            ]
        })
        .collect::<Vec<_>>()
        .table()
        .title(vec![
            "Key".cell().bold(true),
            "Hash".cell().bold(true),
            "Origin Path".cell().bold(true),
            "Modified Time (UTC)".cell().bold(true),
        ]);

    println!("{}", table.display().unwrap());
	
    s3.put(Path::new("_s3b_db/")).await?;
    std::fs::remove_dir_all("_s3b_db").unwrap();
	
    Ok(())
}
