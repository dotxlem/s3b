use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use chrono::{DateTime, Local, Utc};
use clap::ArgMatches;
use colored::Colorize;
use itertools::Itertools;
use rayon::prelude::*;
use walkdir::WalkDir;

use crate::{sql::EntriesRow, Plan, PlanEntry, Sql, S3};

pub async fn plan(matches: &ArgMatches) -> anyhow::Result<()> {
    let bucket_name = matches.get_one::<String>("bucket").unwrap();
    let endpoint = matches.get_one::<String>("endpoint");

    let exclude: Vec<&String> = match matches.get_many("exclude") {
        Some(m) => m.collect(),
        None => Vec::new(),
    };

    let include: Vec<&String> = match matches.get_many("include") {
        Some(m) => m.collect(),
        None => Vec::new(),
    };

    let spinner = indicatif::ProgressBar::new_spinner();
    spinner.enable_steady_tick(Duration::from_millis(120));
    spinner.set_message("Finding files...");
    let mut filtered_entries: Vec<PathBuf> = Vec::new();
    for entry in WalkDir::new("./").min_depth(1) {
        let entry = entry.unwrap();
        let entry = match entry.path().canonicalize() {
            Ok(entry) => entry,
            Err(_) => return Err(anyhow!("could not resolve {:?}; is this a symlink which no longer exists?", entry.path())),
        };
        if entry.is_file() && !entry.is_symlink() {
            if include.len() > 0 {
                if filter(include.clone(), entry.clone()) {
                    filtered_entries.push(entry);
                }
            } else {
                filtered_entries.push(entry);
            }
        }
    }
    let filtered_entries: Vec<PathBuf> = filtered_entries
        .into_iter()
        .filter(|path| !filter(exclude.clone(), path.clone()))
        .collect();
    let filtered_entries: Vec<PathBuf> = filtered_entries
        .into_iter()
        .unique()
        .filter(|path| {
            path.components()
                .find(|&c| c.as_os_str().to_str().unwrap() == "_s3b_db")
                .is_none()
                && path.file_name().unwrap() != "s3b_plan.bin"
        })
        .collect();
    spinner.finish_with_message(format!("Found {} entries", filtered_entries.len()));

    // TODO check for lock
    //      lock should be its own operation, i.e. s3b lock & s3b lock --release
    let s3 = S3::new(&bucket_name, endpoint.map(|s| s.as_str())).await?;
    let exists = s3.key_exists("_s3b_db/entries.sql").await?;
    if exists {
        s3.get("_s3b_db/").await?;
    }

    let mut sql = Sql::new().await?;
    let remote_entries = sql.get_entries().await?;
    // println!("remote={:?}", remote_entries);

    println!("Processing entries...");
    let base_path = PathBuf::from("./").canonicalize().unwrap();
    let warnings: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let prompt_list: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let prompt_entries: Mutex<Vec<PlanEntry>> = Mutex::new(Vec::new());
    let planned_entries: Mutex<Vec<PlanEntry>> =
        Mutex::new(Vec::with_capacity(filtered_entries.len()));
    let pb = indicatif::ProgressBar::new(filtered_entries.len() as u64);
    let num_skipped: AtomicU64 = AtomicU64::new(0);
    let num_new: AtomicU64 = AtomicU64::new(0);
    filtered_entries.into_par_iter().for_each(|path| {
        let metadata = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(_err) => panic!("could not stat {:?}", &path),
        };
        let modified = metadata.modified().unwrap();
        let dt = DateTime::<Local>::from(
            UNIX_EPOCH
                + Duration::from_secs(
                    modified
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ),
        );
        let dt_utc = DateTime::<Utc>::from(dt);
        let timestamp = dt_utc.timestamp() as u64;

        let contents = std::fs::read(&path).unwrap();
        let hash = blake3::hash(&contents).to_string();

        let key = path
            .to_str()
            .unwrap()
            .replace(&format!("{}/", base_path.to_str().unwrap()), "");

        let plan_entry = PlanEntry {
            key: key.clone(),
            path,
            hash: hash.clone(),
            modified: timestamp,
        };

        let mut modified_key: Option<&EntriesRow> = None;
        let mut existing_hashes: Vec<&EntriesRow> = Vec::new();
        let this_hash = hash.clone();
        let this_key = key.clone();
        remote_entries.iter().for_each(|remote| {
            if remote.hash == this_hash {
                existing_hashes.push(remote);
            }
            if remote.key == this_key && remote.hash != this_hash {
                modified_key = Some(remote);
            }
        });
        let mut skip = false;
        let mut prompt = false;
        if let Some(remote) = modified_key {
            // different hash at same key, prompt
            let local_modified_time = modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            if remote.modified > local_modified_time {
                // println!("Key {} exists, remote is newer", &remote.key);
                prompt_list
                    .lock()
                    .unwrap()
                    .push(format!("{} [remote is newer]", &remote.key));
                prompt_entries.lock().unwrap().push(plan_entry.clone());
            } else if remote.modified < local_modified_time {
                // println!("Key {} exists, remote is older", &remote.key);
                prompt_list
                    .lock()
                    .unwrap()
                    .push(format!("{} [remote is older]", &remote.key));
                prompt_entries.lock().unwrap().push(plan_entry.clone());
            } else {
                panic!(
                    "Key {} exists with different hash but timestamp is same; this may be a bug",
                    &remote.key
                );
            }
            prompt = true;
            skip = true;
        }
        if existing_hashes
            .iter()
            .find(|&remote| remote.key == this_key)
            .is_some()
        {
            // key exists with same hash, skip
            // println!("Key {} exists with same hash", &this_key);
            num_skipped.fetch_add(1, Ordering::Relaxed);
            skip = true;
        }

        if !skip && !prompt {
            num_new.fetch_add(1, Ordering::Relaxed);
        }

        if !skip && existing_hashes.len() > 0 {
            // not skipped but identical hashes found, flag
            let mut warning = format!(
                "Identical hashes found for new object {} at keys:\n",
                &this_key.bold().white()
            );

            for i in existing_hashes.iter().map(|e| &e.key).collect::<Vec<_>>() {
                warning.push_str(&format!("    - {}\n", i.bold().white()));
            }
            warning.push('\n');
            warnings.lock().unwrap().push(warning);
        }
        // TODO this is potentially an overwhelming number of warnings, which also needs to be deduped between entries
        //      could instead make this a separate command operating on the remote store
        // if skip && !prompt && existing_hashes.len() > 1 {
        //     // skipped but there are multiple identical hashes, flag
        //     let mut warning = format!(
        //         "Key {} skipped but remote objects with identical hashes found: {:?}",
        //         &this_key,
        //         existing_hashes
        //             .iter()
        //             .filter(|e| e.key != this_key)
        //             .collect::<Vec<_>>()
        //     );
        //     warning.push('\n');
        //     warnings.lock().unwrap().push(warning);
        // }

        if !skip {
            planned_entries.lock().unwrap().push(plan_entry);
        }

        pb.inc(1);
    });
    pb.finish();
    let mut entries = planned_entries.into_inner().unwrap();

    let prompt_list = prompt_list.into_inner().unwrap();
    if prompt_list.len() > 0 {
        let ans = inquire::MultiSelect::new("Select conflicting objects to include", prompt_list)
            .prompt()
            .unwrap();
        let selected_keys = ans
            .iter()
            .map(|i| i.split(" [").collect::<Vec<_>>()[0])
            .collect::<Vec<_>>();
        let mut prompt_entries = prompt_entries
            .into_inner()
            .unwrap()
            .into_iter()
            .filter(|e| selected_keys.iter().find(|&s| *s == e.key).is_some())
            .collect::<Vec<_>>();
        entries.append(&mut prompt_entries);
        // println!("Selected {:?}", selected_keys);
    }

    println!("\n{}", "Warnings:".yellow().bold());
    for warning in warnings.into_inner().unwrap() {
        println!(" - {}", warning);
    }

    let num_entries = entries.len() as u64;
    let num_new = num_new.load(Ordering::Relaxed);
    let num_skipped = num_skipped.load(Ordering::Relaxed);
    let num_updated = num_entries - num_new;
    if entries.len() > 0 {
        println!(
            "\n{}",
            format!(
                "Plan will upload {} objects: {} new, {} updated. Skipped {} identical entries.",
                num_entries, num_new, num_updated, num_skipped,
            )
            .green()
        );
    } else {
        println!("\n{}", "Plan is empty; nothing new to upload.".white());
    }

    let plan = Plan {
        bucket_name: bucket_name.to_string(),
        base_path,
        entries,
    };
    // println!("{:?}", plan);
    plan.write();
    std::fs::remove_dir_all("_s3b_db").unwrap();

    Ok(())
}

fn filter(list: Vec<&String>, entry: PathBuf) -> bool {
    for l in list {
        if entry.to_str().unwrap().contains(l.as_str()) {
            return true;
        }
    }
    return false;
}
