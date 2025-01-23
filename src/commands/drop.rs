use std::path::Path;

use clap::ArgMatches;

use crate::{Sql, S3};

pub async fn drop(matches: &ArgMatches) -> anyhow::Result<()> {
    let bucket_name = matches.get_one::<String>("bucket").unwrap();
    let path = matches.get_one::<String>("path").unwrap();
    let endpoint = matches.get_one::<String>("endpoint");

    // TODO check for lock
    //      lock should be its own operation, i.e. s3b lock & s3b lock --release
    let s3 = S3::new(&bucket_name, endpoint.map(|s| s.as_str())).await?;
    let exists = s3.key_exists("_s3b_db/entries.sql").await?;
    if exists {
        s3.get("_s3b_db/").await?;
    }

    if let Ok(deleted) = s3.delete(&path).await {
        let mut sql = Sql::new().await?;
        for key in deleted {
            sql.delete_entry_by_key(&key).await?;
        }
    }

    s3.put(Path::new("_s3b_db/")).await?;
    std::fs::remove_dir_all("_s3b_db").unwrap();

    Ok(())
}
