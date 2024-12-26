use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::anyhow;
use aws_config::meta::region::RegionProviderChain;
use aws_credential_types::provider::ProvideCredentials;
use futures::StreamExt;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    path::Path as ObjectPath,
    Error, ObjectStore, PutPayload,
};
use walkdir::WalkDir;

pub struct S3 {
    client: AmazonS3,
}

impl S3 {
    pub async fn new(bucket_name: &str) -> anyhow::Result<Self> {
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
        let config = aws_config::from_env().region(region_provider).load().await;
        let creds = config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        match AmazonS3Builder::new()
            .with_access_key_id(creds.access_key_id())
            .with_secret_access_key(creds.secret_access_key())
            .with_region(
                config
                    .region()
                    .expect("expected aws region to be set")
                    .as_ref(),
            )
            .with_bucket_name(bucket_name)
            .build()
        {
            Ok(client) => Ok(Self { client }),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn key_exists(&self, key: &str) -> anyhow::Result<bool> {
        match self.client.head(&ObjectPath::from(key)).await {
            Ok(_) => Ok(true),
            Err(Error::NotFound { path: _, source: _ }) => Ok(false),
            Err(Error::Generic {
                store: _,
                source: _,
            }) => Err(anyhow!("Generic S3 error: does the bucket exist?")),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn put(&self, path: &Path) -> anyhow::Result<()> {
        if path.is_file() {
            self.put_one(path).await
        } else if path.is_dir() {
            for entry in WalkDir::new(path).min_depth(1) {
                let entry = entry.unwrap();
                let entry = entry.path();
                if entry.is_file() {
                    self.put_one(entry).await?;
                }
            }
            Ok(())
        } else if path.is_symlink() {
            Err(anyhow!("symlinks are not supported"))
        } else {
            Err(anyhow!("path {:?} is not a file or a directory", &path))
        }
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<()> {
        let last_char = key.get(key.len() - 1..key.len()).unwrap();
        if last_char == "/" {
            let mut list_stream = self.client.list(Some(&ObjectPath::from(key)));
            while let Some(meta) = list_stream.next().await.transpose().unwrap() {
                if meta.size != 0 {
                    self.get_one(meta.location.to_string().as_str()).await?
                }
            }
        } else {
            self.get_one(key).await?
        }
        Ok(())
    }
}

impl S3 {
    async fn put_one(&self, path: &Path) -> anyhow::Result<()> {
        if !path.is_file() {
            return Err(anyhow!("{:?} is not a file", path));
        }

        let bytes = read_file_to_bytes(path)?;
        let payload = PutPayload::from_bytes(bytes.into());
        if let Err(err) = self
            .client
            .put(&ObjectPath::from(path.to_str().unwrap()), payload)
            .await
        {
            return Err(err.into());
        }

        Ok(())
    }

    async fn get_one(&self, path: &str) -> anyhow::Result<()> {
        match self.client.get(&ObjectPath::from(path)).await {
            Ok(result) => {
                write_bytes_to_file(&Path::new(path), result.bytes().await.unwrap().as_ref())
            }
            Err(err) => Err(err.into()),
        }
    }
}

fn read_file_to_bytes(path: &Path) -> anyhow::Result<Vec<u8>> {
    match File::open(path) {
        Ok(mut file) => {
            let mut buf = Vec::<u8>::new();
            file.read_to_end(&mut buf)?;
            Ok(buf)
        }
        Err(err) => Err(err.into()),
    }
}

fn write_bytes_to_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut buf = PathBuf::from(path);
    buf.pop();
    std::fs::create_dir_all(buf).unwrap();

    match File::options().write(true).create(true).open(path) {
        Ok(mut file) => file.write_all(bytes).map_err(|err| err.into()),
        Err(err) => Err(err.into()),
    }
}
