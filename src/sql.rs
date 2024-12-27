use std::collections::HashMap;

use anyhow::anyhow;
use gluesql::prelude::{Glue, JsonStorage, Value};

use crate::PlanEntry;

pub struct Sql {
    glue: Glue<JsonStorage>,
}

impl Sql {
    pub async fn new() -> anyhow::Result<Self> {
        let storage = JsonStorage::new("_s3b_db").map_err(|e| anyhow!(e))?;
        let mut glue = Glue::new(storage);

        let queries = r#"
        CREATE TABLE IF NOT EXISTS entries (key TEXT PRIMARY KEY, hash TEXT, path TEXT, modified UINT64);
        "#;

        match glue.execute(queries).await {
            Ok(_) => Ok(Self { glue }),
            Err(err) => Err(anyhow!(err)),
        }
    }

    pub async fn get_entries(&mut self) -> anyhow::Result<Vec<EntriesRow>> {
        let query = "SELECT * FROM entries;";
        self.select_entries(query).await
    }

    pub async fn get_entries_by_hash(&mut self, hash: &str) -> anyhow::Result<Vec<EntriesRow>> {
        let query = format!("SELECT * FROM entries WHERE hash='{}'", hash);
        self.select_entries(&query).await
    }

    pub async fn put_entry(&mut self, entry: &PlanEntry) -> anyhow::Result<()> {
        let query = format!(
            "INSERT INTO entries VALUES ('{}', '{}', '{}', {});",
            entry.key,
            entry.hash,
            entry.path.to_str().unwrap(),
            entry.modified,
        );

        match self.glue.execute(query).await {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!(err)),
        }
    }

    pub async fn update_entry(&mut self, entry: &PlanEntry) -> anyhow::Result<()> {
        let query = format!(
            "UPDATE entries SET hash='{}', path='{}', modified={} WHERE key='{}';",
            entry.hash,
            entry.path.to_str().unwrap(),
            entry.modified,
            entry.key,
        );

        match self.glue.execute(query).await {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!(err)),
        }
    }
 }

impl Sql {
    pub async fn select_entries(&mut self, query: &str) -> anyhow::Result<Vec<EntriesRow>> {
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
