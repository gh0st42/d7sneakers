use anyhow::{bail, Result};
use bp7::Bundle;
use log::{debug, error, info};
use sanitize_filename_reader_friendly::sanitize;
use std::path::{Path, PathBuf};
use std::{convert::TryInto, fs};
use walkdir::WalkDir;

pub struct D7sFs {
    base: String,
}

impl D7sFs {
    pub fn new(base: &str) -> Self {
        Self { base: base.into() }
    }
    pub fn setup(&self) -> Result<()> {
        let basepath = Path::new(&self.base);
        fs::create_dir_all(basepath)?;

        fs::create_dir_all(self.path_single())?;
        fs::create_dir_all(self.path_administrative())?;
        fs::create_dir_all(self.path_group())?;

        let version_file = basepath.join("version.txt");
        if version_file.exists() {
            let version: u32 = fs::read_to_string(version_file)?.parse()?;
            if version < crate::D7S_VERSION {
                info!("old filesystem structure detected, upgrade needed");
                unimplemented!();
            } else if version > crate::D7S_VERSION {
                error!("filesystem structure is newer, upgrade program to newest version");
                bail!("outdated program version");
            }
        }
        let version_file = basepath.join("version.txt");
        fs::write(version_file, format!("{}", crate::D7S_VERSION))?;

        Ok(())
    }
    pub fn path_single(&self) -> PathBuf {
        let basepath = Path::new(&self.base);
        basepath.join("single")
    }
    pub fn path_administrative(&self) -> PathBuf {
        let basepath = Path::new(&self.base);
        basepath.join("adm")
    }
    pub fn path_group(&self) -> PathBuf {
        let basepath = Path::new(&self.base);
        basepath.join("group")
    }
    pub fn path_for_bundle(&self, bndl: &Bundle) -> PathBuf {
        let dst = sanitize(&bndl.primary.destination.node().unwrap_or("none".to_owned()));
        if bndl.is_administrative_record() {
            self.path_administrative().join(&dst)
        } else {
            match &bndl.primary.destination {
                bp7::EndpointID::Dtn(_, addr) => {
                    if addr.is_non_singleton() {
                        self.path_group().join(&dst)
                    } else {
                        self.path_single().join(&dst)
                    }
                }
                bp7::EndpointID::DtnNone(_, _) => {
                    unimplemented!()
                }
                bp7::EndpointID::Ipn(_, _addr) => {
                    unimplemented!()
                }
            }
        }
    }
    pub fn path_for_bundle_with_filename(&self, bndl: &Bundle) -> PathBuf {
        let filename = format!("{}.bundle", sanitize(&bndl.id()));
        self.path_for_bundle(bndl).join(&filename)
    }
    pub fn exists(&self, bndl: &Bundle) -> bool {
        self.path_for_bundle_with_filename(bndl).exists()
    }
    pub fn save_bundle(&self, bndl: &mut Bundle) -> Result<()> {
        let bid = bndl.id();
        let filename = format!("{}.bundle", sanitize(&bid));
        let dest_path = self.path_for_bundle(&bndl);

        fs::create_dir_all(&dest_path)?;
        let dest_path = dest_path.join(&filename);
        if dest_path.exists() {
            debug!("File {} already exists, skipping", filename);
        } else {
            fs::write(&dest_path, bndl.to_cbor())?;
        }
        debug!("saved {} to {}", bid, dest_path.to_string_lossy());
        //info!("filename {}", filename);
        Ok(())
    }
    pub fn remove_bundle(&self, bid: &str) -> Result<()> {
        if let Some(filename) = self.find_file_by_bid(bid) {
            fs::remove_file(filename)?;
        } else {
            bail!("bundle ID not found");
        }
        Ok(())
    }
    pub fn find_file_by_bid(&self, bid: &str) -> Option<PathBuf> {
        for entry in WalkDir::new(&self.base)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|f| {
                f.file_name()
                    .to_str()
                    .unwrap_or_default()
                    .ends_with(".bundle")
            })
        {
            let filename = entry.file_name().to_str()?;
            if filename == format!("{}.bundle", sanitize(bid)) {
                return Some(entry.into_path());
            }
        }
        None
    }
    pub fn sync_to_db(&self, db: &crate::D7DB) -> Result<()> {
        info!("syncing fs to db");
        for entry in WalkDir::new(&self.base)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|f| {
                f.file_name()
                    .to_str()
                    .unwrap_or_default()
                    .ends_with(".bundle")
            })
        {
            let (filebase, _extension) = entry
                .file_name()
                .to_str()
                .unwrap()
                .rsplit_once('.')
                .unwrap();
            let bid = if filebase.starts_with("dtn") {
                filebase.replace('_', "/").replacen("dtn", "dtn:/", 1)
            } else {
                unimplemented!();
            };
            let is_in_db = db.exists(&bid);
            debug!("{} in db: {}", entry.path().display(), is_in_db);
            if !is_in_db {
                let buf = fs::read(entry.path())?;
                let bndl: Bundle = buf.try_into()?;
                db.insert(&bndl)?;
                info!("adding {} to db", bndl.id());
            }
        }
        Ok(())
    }
    pub fn import_hex(&self, hexstr: &str) -> Result<Bundle> {
        let mut bndl: Bundle = bp7::helpers::unhexify(hexstr)?.try_into()?;

        self.save_bundle(&mut bndl)?;
        Ok(bndl)
    }

    pub fn import_vec(&self, buf: Vec<u8>) -> Result<Bundle> {
        let mut bndl: Bundle = buf.try_into()?;

        self.save_bundle(&mut bndl)?;
        Ok(bndl)
    }
}
