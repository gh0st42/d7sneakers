#![forbid(unsafe_code)]
use anyhow::Result;
use bp7::Bundle;
use log::debug;
use log::warn;
use walkdir::WalkDir;

mod db;
mod fs;

use std::convert::TryFrom;
use std::convert::TryInto;
use std::path::Path;

pub use db::Constraints;
pub use db::D7DB;
pub use fs::D7sFs;

pub const D7S_VERSION: u32 = 1;

use log::info;

pub struct SneakerWorld {
    pub db: D7DB,
    pub fs: D7sFs,
}

impl SneakerWorld {
    pub fn new(basepath: &str) -> Self {
        let db_file = Path::new(basepath)
            .join("db.sqlite3")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_owned();
        let file_path = Path::new(basepath)
            .join("files")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_owned();
        let db = db::D7DB::open(&db_file).unwrap();
        Self {
            db,
            fs: D7sFs::new(&file_path),
        }
    }
    pub fn sync(&self) -> Result<()> {
        self.fs.sync_to_db(&self.db)?;
        self.db.sync_with_fs(&self.fs)
    }
    pub fn push(&self, bndl: &mut Bundle) -> Result<()> {
        self.fs.save_bundle(bndl)?;
        self.db.insert(bndl)
    }
    pub fn remove(&self, bid: &str) -> Result<()> {
        self.fs.remove_bundle(bid)?;
        self.db.delete(bid)
    }
    pub fn import_dir(&self, path: &str, recursive: bool) -> Result<()> {
        info!("importing {} (recursive: {})", path, recursive);
        let walker = if recursive {
            WalkDir::new(path)
        } else {
            WalkDir::new(path).max_depth(1)
        };
        for entry in walker.into_iter().filter_map(|e| e.ok()).filter(|f| {
            f.file_name()
                .to_str()
                .unwrap_or_default()
                .ends_with(".bundle")
        }) {
            let (filebase, _extension) = entry
                .file_name()
                .to_str()
                .unwrap()
                .rsplit_once('.')
                .unwrap();
            if filebase.starts_with("dtn") {
                let bid = filebase.replace('_', "/").replacen("dtn", "dtn:/", 1);
                let is_in_db = self.db.exists(&bid);
                if !is_in_db {
                    let buf = std::fs::read(entry.path())?;
                    let mut bndl: Bundle = buf.try_into()?;
                    self.fs.save_bundle(&mut bndl)?;
                    self.db.insert(&bndl)?;
                    info!("imported {} from {:?}", bndl.id(), entry.path());
                } else {
                    debug!("{} already in store", bid);
                }
            } else {
                let buf = std::fs::read(entry.path())?;
                if let Ok(mut bndl) = Bundle::try_from(buf) {
                    let bid = bndl.id();
                    let is_in_db = self.db.exists(&bid);
                    if !is_in_db {
                        self.fs.save_bundle(&mut bndl)?;
                        self.db.insert(&bndl)?;
                        info!("imported {} from {:?}", bndl.id(), entry.path());
                    } else {
                        debug!("{} already in store", bid);
                    }
                } else {
                    warn!("could not parse bundle file: {:?}", entry.path());
                }
            };
        }
        Ok(())
    }
}
