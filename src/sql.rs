use std::collections::HashMap;

use anyhow::anyhow;
use gluesql::prelude::{Glue, Payload, SledStorage, Value};

pub struct Sql {
    glue: Glue<SledStorage>,
}

impl Sql {
    pub fn new() -> Self {
        let storage = SledStorage::new("_s3b_db").expect("Something went wrong!");
        Self {
            glue: Glue::new(storage),
        }
    }

    pub async fn init(&mut self) -> anyhow::Result<()> {
        let queries = r#"
        CREATE TABLE IF NOT EXISTS entries;
        CREATE TABLE IF NOT EXISTS origins (id INT PRIMARY KEY, hostname TEXT, device TEXT);
        "#;

        match self.glue.execute(queries).await {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!(err)),
        }
    }

    pub async fn get_entries(&mut self) -> anyhow::Result<Vec<EntriesRow>> {
        let queries = "SELECT * FROM entries;";
        match self.glue.execute(queries).await {
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

    pub async fn add_origin(&mut self, hostname: &str, device: &str) -> anyhow::Result<()> {
        let id = self.get_next_origin_id().await?;
        let queries = format!(
            "INSERT INTO origins VALUES ({}, '{}', '{}');",
            id, hostname, device
        );
        match self.glue.execute(queries).await {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!(err)),
        }
    }

    pub async fn get_origins(&mut self) -> anyhow::Result<Vec<OriginsRow>> {
        let queries = "SELECT * FROM origins;";
        match self.glue.execute(queries).await {
            Ok(mut res) => {
                let payload = res.remove(0);
                Ok(payload
                    .select()
                    .unwrap()
                    .map(TryInto::<OriginsRow>::try_into)
                    .collect::<anyhow::Result<_>>()?)
            }
            Err(err) => Err(anyhow!(err)),
        }
    }

    pub async fn get_next_origin_id(&mut self) -> anyhow::Result<i64> {
        let queries = "SELECT id FROM origins ORDER BY id DESC LIMIT 1;";
        match self.glue.execute(queries).await {
            Ok(mut res) => {
                let payload = res.remove(0);
                let rows = payload
                    .select()
                    .unwrap()
                    .map(|row| match *row.get("id").unwrap() {
                        Value::I64(v) => Ok(*v),
                        _ => Err(anyhow!("`id` expected to be i64")),
                    })
                    .collect::<anyhow::Result<Vec<i64>>>()?;
                println!("rows={:?}", rows);

                match rows.get(0) {
                    Some(id) => Ok(*id + 1),
                    None => Ok(0i64),
                }
            }
            Err(err) => Err(anyhow!(err)),
        }
    }
}

pub struct EntriesRow {
    pub id: i64,
}

impl TryFrom<HashMap<&str, &Value>> for EntriesRow {
    type Error = anyhow::Error;

    fn try_from(value: HashMap<&str, &Value>) -> Result<Self, Self::Error> {
        let id = match *value.get("id").unwrap() {
            Value::I64(v) => *v,
            _ => return Err(anyhow!("`id` expected to be i64")),
        };
        Ok(EntriesRow { id })
    }
}

pub struct OriginsRow {
    pub id: i64,
    pub hostname: String,
    pub device: String,
}

impl TryFrom<HashMap<&str, &Value>> for OriginsRow {
    type Error = anyhow::Error;

    fn try_from(value: HashMap<&str, &Value>) -> Result<Self, Self::Error> {
        let id = match *value.get("id").unwrap() {
            Value::I64(v) => *v,
            _ => return Err(anyhow!("`id` expected to be i64")),
        };
        let hostname = match *value.get("hostname").unwrap() {
            Value::Str(v) => v.clone(),
            _ => return Err(anyhow!("`name` is expected to be str")),
        };
        let device = match *value.get("device").unwrap() {
            Value::Str(v) => v.clone(),
            _ => return Err(anyhow!("`name` is expected to be str")),
        };
        Ok(OriginsRow {
            id,
            hostname,
            device,
        })
    }
}
