use anyhow::anyhow;
use gluesql::prelude::{Glue, SledStorage};

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
        let queries = "CREATE TABLE IF NOT EXISTS s3b (bucket TEXT, key TEXT);";

        match self.glue.execute(queries).await {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!(err)),
        }
    }
}
