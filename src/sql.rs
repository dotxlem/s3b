use std::collections::HashMap;
use std::time::SystemTime;

use anyhow::anyhow;
use gluesql::prelude::{Glue, SledStorage, Value};

use crate::PlanEntry;

pub struct Sql {
    glue: Glue<SledStorage>,
}

impl Sql {
    pub async fn new() -> anyhow::Result<Self> {
        let storage = SledStorage::new("_s3b_db").map_err(|e| anyhow!(e))?;
        let mut glue = Glue::new(storage);

        let queries = r#"
        CREATE TABLE IF NOT EXISTS entries (key TEXT PRIMARY KEY, hash TEXT, path TEXT, modified UINT64);
        "#;

        match glue.execute(queries).await {
            Ok(_) => Ok(Self { glue }),
            Err(err) => Err(anyhow!(err)),
        }
    }

    // pub async fn init(&mut self) -> anyhow::Result<()> {
    //     // let queries = r#"
    //     // CREATE TABLE IF NOT EXISTS entries;
    //     // CREATE TABLE IF NOT EXISTS origins (id INT PRIMARY KEY, hostname TEXT, device TEXT);
    //     // "#;
    //     let queries = r#"
    //     CREATE TABLE IF NOT EXISTS entries;
    //     "#;

    //     match self.glue.execute(queries).await {
    //         Ok(_) => Ok(()),
    //         Err(err) => Err(anyhow!(err)),
    //     }
    // }

    pub async fn get_entries(&mut self) -> anyhow::Result<Vec<EntriesRow>> {
        let query = "SELECT * FROM entries;";
        self.query_entries(query).await
    }

    pub async fn get_entry_by_hash(&mut self, hash: &str) -> anyhow::Result<Vec<EntriesRow>> {
        let query = format!("SELECT * FROM entries WHERE hash='{}' LIMIT 1;", hash);
        self.query_entries(&query).await
    }

    pub async fn put_entry(&mut self, entry: &PlanEntry) -> anyhow::Result<()> {
        let query = format!(
            // "INSERT INTO entries VALUES ('{{\"key\": \"{}\", \"hash\": \"{}\", \"path\": \"{}\", \"modified\": {}}}');",
            "INSERT INTO entries VALUES ('{}', '{}', '{}', {});",
            entry.key,
            entry.hash,
            entry.path.to_str().unwrap(),
            entry
                .modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        match self.glue.execute(query).await {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!(err)),
        }
    }

    // pub async fn add_origin(&mut self, hostname: &str, device: &str) -> anyhow::Result<()> {
    //     let id = self.get_next_origin_id().await?;
    //     let queries = format!(
    //         "INSERT INTO origins VALUES ({}, '{}', '{}');",
    //         id, hostname, device
    //     );
    //     match self.glue.execute(queries).await {
    //         Ok(_) => Ok(()),
    //         Err(err) => Err(anyhow!(err)),
    //     }
    // }

    // pub async fn get_origins(&mut self) -> anyhow::Result<Vec<OriginsRow>> {
    //     let queries = "SELECT * FROM origins;";
    //     match self.glue.execute(queries).await {
    //         Ok(mut res) => {
    //             let payload = res.remove(0);
    //             Ok(payload
    //                 .select()
    //                 .unwrap()
    //                 .map(TryInto::<OriginsRow>::try_into)
    //                 .collect::<anyhow::Result<_>>()?)
    //         }
    //         Err(err) => Err(anyhow!(err)),
    //     }
    // }

    // pub async fn get_next_origin_id(&mut self) -> anyhow::Result<i64> {
    //     let queries = "SELECT id FROM origins ORDER BY id DESC LIMIT 1;";
    //     match self.glue.execute(queries).await {
    //         Ok(mut res) => {
    //             let payload = res.remove(0);
    //             let rows = payload
    //                 .select()
    //                 .unwrap()
    //                 .map(|row| match *row.get("id").unwrap() {
    //                     Value::I64(v) => Ok(*v),
    //                     _ => Err(anyhow!("`id` expected to be i64")),
    //                 })
    //                 .collect::<anyhow::Result<Vec<i64>>>()?;
    //             println!("rows={:?}", rows);

    //             match rows.get(0) {
    //                 Some(id) => Ok(*id + 1),
    //                 None => Ok(0i64),
    //             }
    //         }
    //         Err(err) => Err(anyhow!(err)),
    //     }
    // }
}

impl Sql {
    pub async fn query_entries(&mut self, query: &str) -> anyhow::Result<Vec<EntriesRow>> {
        match self.glue.execute(query).await {
            Ok(mut res) => {
                let payload = res.remove(0);
                Ok(payload
                    .select()
                    .unwrap()
                    .map(TryInto::<EntriesRow>::try_into)
                    .collect::<anyhow::Result<_>>()?)
            }
            Err(err) => Err(anyhow!(err)),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct EntriesRow {
    pub key: String,
    pub path: String,
    pub hash: String,
    pub modified: u64,
}

impl TryFrom<HashMap<&str, &Value>> for EntriesRow {
    type Error = anyhow::Error;

    fn try_from(value: HashMap<&str, &Value>) -> Result<Self, Self::Error> {
        let key = match *value.get("key").unwrap() {
            Value::Str(v) => v.clone(),
            _ => return Err(anyhow!("`key` expected to be Str")),
        };
        let path = match *value.get("path").unwrap() {
            Value::Str(v) => v.clone(),
            _ => return Err(anyhow!("`path` expected to be Str")),
        };
        let hash = match *value.get("hash").unwrap() {
            Value::Str(v) => v.clone(),
            _ => return Err(anyhow!("`hash` expected to be Str")),
        };
        let modified = match *value.get("modified").unwrap() {
            Value::U64(v) => *v,
            _ => return Err(anyhow!("`modified` expected to be u64")),
        };
        Ok(EntriesRow {
            key,
            path,
            hash,
            modified,
        })
    }
}

// pub struct OriginsRow {
//     pub id: i64,
//     pub hostname: String,
//     pub device: String,
// }

// impl TryFrom<HashMap<&str, &Value>> for OriginsRow {
//     type Error = anyhow::Error;

//     fn try_from(value: HashMap<&str, &Value>) -> Result<Self, Self::Error> {
//         let id = match *value.get("id").unwrap() {
//             Value::I64(v) => *v,
//             _ => return Err(anyhow!("`id` expected to be i64")),
//         };
//         let hostname = match *value.get("hostname").unwrap() {
//             Value::Str(v) => v.clone(),
//             _ => return Err(anyhow!("`name` is expected to be str")),
//         };
//         let device = match *value.get("device").unwrap() {
//             Value::Str(v) => v.clone(),
//             _ => return Err(anyhow!("`name` is expected to be str")),
//         };
//         Ok(OriginsRow {
//             id,
//             hostname,
//             device,
//         })
//     }
// }
