#![forbid(unsafe_code)]
use anyhow::Result;
use bp7::Bundle;
use db::BundleEntry;
use log::debug;
use log::warn;
use walkdir::DirEntry;
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

#[derive(Debug, Clone)]
pub struct SneakerWorld {
    pub db: D7DB,
    pub fs: D7sFs,
}

impl SneakerWorld {
    pub fn open(basepath: &str) -> Result<Self> {
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
        Ok(Self {
            db: db::D7DB::open(&db_file)?,
            fs: D7sFs::open(&file_path)?,
        })
    }
    pub fn sync(&self) -> Result<()> {
        self.fs.sync_to_db(&self.db)?;
        self.db.sync_with_fs(&self.fs)
    }
    pub fn push(&self, bndl: &mut Bundle) -> Result<()> {
        let bundle_size = self.fs.save_bundle(bndl)?;
        self.db.insert(bndl, bundle_size)
    }
    pub fn remove(&self, bid: &str) -> Result<()> {
        self.fs.remove_bundle(bid)?;
        self.db.delete(bid)
    }
    fn import_file(&self, entry: DirEntry) -> Result<Option<(String, BundleEntry)>> {
        let (filebase, _extension) = entry
            .file_name()
            .to_str()
            .unwrap()
            .rsplit_once('.')
            .unwrap();
        let res = if filebase.starts_with("dtn") {
            let bid = filebase.replace('_', "/").replacen("dtn", "dtn:/", 1);
            let is_in_db = self.db.exists(&bid);
            if !is_in_db {
                let buf = std::fs::read(entry.path())?;
                let bundle_size = buf.len();

                let mut bndl: Bundle = buf.try_into()?;
                self.fs.save_bundle(&mut bndl)?;
                //self.db.insert(&bndl, bundle_size as u64)?;
                info!("imported {} from {:?}", bndl.id(), entry.path());
                let mut be = BundleEntry::from(&bndl);
                be.size = bundle_size as u64;
                Some((bndl.id(), be))
            } else {
                debug!("{} already in store", &bid);
                None
            }
        } else {
            let buf = std::fs::read(entry.path())?;

            if let Ok(mut bndl) = Bundle::try_from(buf) {
                let bid = bndl.id();
                let is_in_db = self.db.exists(&bid);
                if !is_in_db {
                    let bundle_size = self.fs.save_bundle(&mut bndl)?;
                    //self.db.insert(&bndl, bundle_size)?;
                    info!("imported {} from {:?}", bndl.id(), entry.path());
                    let mut be = BundleEntry::from(&bndl);
                    be.size = bundle_size;
                    Some((bndl.id(), be))
                } else {
                    debug!("{} already in store", bid);
                    None
                }
            } else {
                warn!("could not parse bundle file: {:?}", entry.path());
                None
            }
        };
        Ok(res)
    }
    pub fn import_dir(&self, path: &str, recursive: bool) -> Result<()> {
        info!("importing {} (recursive: {})", path, recursive);
        //let w: crossbeam_deque::Worker<DirEntry> = crossbeam_deque::Worker::new_fifo();
        let mut bes = Vec::new();
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
            //w.push(entry);
            if let Ok(Some(be)) = self.import_file(entry) {
                bes.push(be);
            }
        }
        self.db.insert_bulk(&bes)?;

        // parallel import was not faster..
        /*let mut handles = Vec::new();
        let stealer = w.stealer();
        for _ in 1..4 {
            let s2 = stealer.clone();
            let world = self.clone();
            let handle = std::thread::spawn(move || -> Result<()> {
                while !s2.is_empty() {
                    if let crossbeam_deque::Steal::Success(entry) = s2.steal() {
                        world.import_file(entry)?;
                    }
                }
                Ok(())
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap()?;
        }*/

        Ok(())
    }
}
