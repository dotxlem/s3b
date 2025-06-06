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
    Error, ObjectStore, PutPayload, WriteMultipart,
};
use walkdir::WalkDir;

pub struct S3 {
    client: AmazonS3,
}

impl S3 {
    pub async fn new(bucket_name: &str, endpoint: Option<&str>) -> anyhow::Result<Self> {
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
        let config = aws_config::from_env().region(region_provider).load().await;
        let creds = config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        let mut builder = AmazonS3Builder::new()
            .with_access_key_id(creds.access_key_id())
            .with_secret_access_key(creds.secret_access_key())
            .with_region(
                config
                    .region()
                    .expect("expected aws region to be set")
                    .as_ref(),
            )
            .with_bucket_name(bucket_name);

        if let Some(ep) = endpoint {
            builder = builder.with_endpoint(ep);
        }

        match builder.build() {
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
                self.get_one(meta.location.to_string().as_str()).await?
            }
        } else {
            self.get_one(key).await?
        }
        Ok(())
    }

    pub async fn delete(&self, path: &str) -> anyhow::Result<Vec<String>> {
        let mut deleted: Vec<String> = Vec::new();
        let last_char = path.get(path.len() - 1..path.len()).unwrap();
        if last_char == "/" {
            let mut list_stream = self.client.list(Some(&ObjectPath::from(path)));
            while let Some(meta) = list_stream.next().await.transpose().unwrap() {
                self.delete_one(meta.location.to_string().as_str()).await?;
                deleted.push(meta.location.to_string());
            }
        } else {
            self.delete_one(path).await?;
            deleted.push(path.into());
        }

        Ok(deleted)
    }
}

impl S3 {
    async fn put_one(&self, path: &Path) -> anyhow::Result<()> {
        if !path.is_file() {
            return Err(anyhow!("{:?} is not a file", path));
        }

        let metadata = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(_err) => panic!("could not stat {:?}", &path),
        };
        let part_size = 50_000_000u64;

        if metadata.len() <= part_size {
            let bytes = read_file_to_bytes(path)?;
            let payload = PutPayload::from_bytes(bytes.into());
            if let Err(err) = self
                .client
                .put(&ObjectPath::from(path.to_str().unwrap()), payload)
                .await
            {
                return Err(err.into());
            }
        } else {
            let upload = self
                .client
                .put_multipart(&ObjectPath::from(path.to_str().unwrap()))
                .await
                .unwrap();
            let mut writer = WriteMultipart::new(upload);
            match File::open(path) {
                Ok(mut file) => {
                    for i in 0u64..(metadata.len() as f64 / part_size as f64).ceil() as u64 {
                        let num_bytes = std::cmp::min(part_size, metadata.len() - (i * part_size));
                        let mut buf = Vec::new();
                        buf.resize(num_bytes as usize, 0);
                        file.read_exact(&mut buf).unwrap();
                        writer.write(&buf);
                    }
                    writer.finish().await.unwrap();
                }
                Err(err) => return Err(err.into()),
            }
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

    async fn delete_one(&self, key: &str) -> anyhow::Result<()> {
        if let Err(err) = self.client.delete(&ObjectPath::from(key)).await {
            return Err(err.into())
        }

        Ok(())
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
