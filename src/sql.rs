use anyhow::anyhow;
use gluesql::prelude::{Glue, SledStorage, Value};

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
        CREATE TABLE IF NOT EXISTS entries (key TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS hostnames (id INT PRIMARY KEY, name TEXT);
        "#;

        match self.glue.execute(queries).await {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!(err)),
        }
    }

    pub async fn add_hostname(&mut self, hostname: &str) -> anyhow::Result<()> {
        let queries = format!("INSERT INTO hostnames VALUES ({}, '{}');", 0, hostname);
        match self.glue.execute(queries).await {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!(err)),
        }
    }

    pub async fn get_hostnames(&mut self) {
        let queries = "SELECT * FROM hostnames ORDER BY id DESC;";
        match self.glue.execute(queries).await {
            Ok(mut res) => {
                let payload = res.remove(0);
                let rows = payload.select().unwrap().map(|row| {
                    let id = match *row.get("id").unwrap() {
                        Value::I64(v) => v,
                        _ => panic!("`id` expected to be i64"),
                    };
                    println!("id = {}", *id);
                    HostnamesRow { id: *id, name: "xlemovo".into() }
                }).collect::<Vec<_>>();
            },
            Err(err) => ()/*Err(anyhow!(err))*/,
        }
    }
}

struct HostnamesRow {
    id: i64,
    name: String,
}
