use std::{fs, path::Path};

use anyhow::{bail, Result};
use bp7::Bundle;
use log::{debug, error, info, warn};
use rusqlite::{params, Connection};

use bitflags::bitflags;

bitflags! {
    pub struct Constraints: u32 {
        const DISPATCH_PENDING =         0b00000001;
        const FORWARD_PENDING =          0b00000010;
        const REASSEMBLY_PENDING =       0b00000100;
        const CONTRAINDICATED =          0b00001000;
        const LOCAL_ENDPOINT =           0b00010000;
    }
}

#[derive(Debug)]
pub struct BundleEntry {
    id: i32,
    src_name: Option<String>,
    src_service: Option<String>,
    dst_name: Option<String>,
    dst_service: Option<String>,
    timestamp: u64,
    seqno: u64,
    lifetime: u64,
}

#[derive(Debug)]
pub struct D7DB {
    db_file: String,
}

impl D7DB {
    /*pub fn new() -> Self {
        let conn = Connection::open_in_memory().expect("error opening in-memory sqlite database");

        let me = Self {
            db_file: None,
            in_mem: Some(conn),
        };
        me.create().expect("error creating tables");
        me
    }*/
    pub fn open(path: &str) -> Result<Self> {
        let dir_path = Path::new(&path)
            .parent()
            .expect("error getting directory path");
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)?;
        }

        let me = Self {
            db_file: path.to_owned(),
        };
        me.create()?;
        Ok(me)
    }
    fn get_connection(&self) -> Result<Connection> {
        Ok(Connection::open(&self.db_file.clone())?)
    }
    fn create(&self) -> Result<()> {
        let conn = self.get_connection()?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS bundles (
                      id              INTEGER PRIMARY KEY,
                      src_name        TEXT,
                      src_service     TEXT,
                      dst_name        TEXT,
                      dst_service     TEXT,
                      timestamp       INTEGER,
                      seqno           INTEGER,
                      lifetime        INTEGER
                      )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS bids (
                      id                INTEGER PRIMARY KEY,
                      bid               TEXT NOT NULL,
                      bundle_idx        INTEGER,
                      constraints_idx   INTEGER
                      )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS constraints (
                      id              INTEGER PRIMARY KEY,
                      constraints     INTEGER
                      )",
            [],
        )?;
        Ok(())
    }
    pub fn delete(&self, bid: &str) -> Result<()> {
        if !self.exists(bid) {
            bail!("no such database entry found");
        }
        let conn = self.get_connection()?;
        if let Ok(idx) = self.find_bundle_number_by_bid(bid) {
            let mut stmt = conn.prepare("DELETE FROM bids WHERE id = ?")?;
            stmt.execute([idx.0])?;
            let mut stmt = conn.prepare("DELETE FROM bundles WHERE id = ?")?;
            stmt.execute([idx.1])?;
            let mut stmt = conn.prepare("DELETE FROM constraints WHERE id = ?")?;
            stmt.execute([idx.2])?;
        }
        Ok(())
    }
    pub fn find_bundle_number_by_bid(&self, bid: &str) -> Result<(usize, usize, usize)> {
        let conn = self.get_connection()?;
        let mut stmt =
            conn.prepare("SELECT id, bundle_idx, constraints_idx FROM bids WHERE bid = ?")?;
        let mut rows = stmt.query([bid])?;
        while let Some(row) = rows.next()? {
            let idx: usize = row.get(0)?;
            let bndl_idx: usize = row.get(1)?;
            let constraint_idx: usize = row.get(2)?;
            return Ok((idx, bndl_idx, constraint_idx));
        }
        bail!("bundle ID not found in database");
    }
    pub fn get_bundle_entry(&self, bid: &str) -> Result<BundleEntry> {
        let (_, b_idx, _) = self.find_bundle_number_by_bid(bid)?;
        let conn = self.get_connection()?;
        let mut stmt = conn.prepare("SELECT * FROM bundles WHERE id = ?")?;
        let mut rows = stmt.query([b_idx])?;
        let row = rows.next()?.expect("bundle id not found in database");
        let be = BundleEntry {
            id: row.get(0)?,
            src_name: row.get(1)?,
            src_service: row.get(2)?,
            dst_name: row.get(3)?,
            dst_service: row.get(4)?,
            timestamp: row.get(5)?,
            seqno: row.get(6)?,
            lifetime: row.get(7)?,
        };
        Ok(be)
    }
    pub fn insert(&self, bndl: &Bundle) -> Result<()> {
        if self.exists(&bndl.id()) {
            return Ok(());
        }
        let conn = self.get_connection()?;

        let be = BundleEntry {
            id: 0,
            src_name: bndl.primary.source.node(),
            src_service: bndl.primary.source.service_name(),
            dst_name: bndl.primary.destination.node(),
            dst_service: bndl.primary.destination.service_name(),
            timestamp: bndl.primary.creation_timestamp.dtntime(),
            seqno: bndl.primary.creation_timestamp.seqno(),
            lifetime: bndl.primary.lifetime.as_secs(),
        };

        conn.execute(
            "INSERT INTO bundles (                
                src_name,
                src_service,
                dst_name,
                dst_service,
                timestamp,
                seqno,
                lifetime) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                be.src_name,
                be.src_service,
                be.dst_name,
                be.dst_service,
                be.timestamp,
                be.seqno,
                be.lifetime
            ],
        )?;
        let last_bundle_id = conn.last_insert_rowid();

        conn.execute(
            "INSERT INTO constraints (                
                constraints) VALUES (?1)",
            params![0],
        )?;
        let last_constraint_id = conn.last_insert_rowid();

        conn.execute(
            "INSERT INTO bids ( bid, bundle_idx, constraints_idx) VALUES ( ?1, ?2, ?3) ",
            params![bndl.id(), last_bundle_id, last_constraint_id],
        )?;
        Ok(())
    }
    pub fn exists(&self, bid: &str) -> bool {
        /*let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM bundles WHERE src_name = ? AND src_service = ? AND timestamp = ? AND seqno = ?")
            .unwrap();
        //dbg!(&bid);
        let cts: Vec<&str> = bid.split('-').collect();
        let timestamp: u64 = cts[1].parse().unwrap();
        let seqno: u64 = cts[2].parse().unwrap();

        let (name, service) = if bid.starts_with("dtn") {
            let mut tmp = cts[0].split('/').skip(2);
            (tmp.next(), tmp.next())
        } else {
            /*let mut tmp = cts[0].split('/').skip(2).collect::<Vec<&str>>().split(".");
            (tmp.next(), tmp.next())*/
            unimplemented!();
        };*/
        let conn = self.get_connection().unwrap();
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM bids WHERE bid = ?")
            .unwrap();
        //dbg!(name, service, timestamp, seqno);
        let mut rows = stmt.query([bid]).unwrap();
        while let Some(row) = rows.next().expect("") {
            let count: usize = row.get(0).expect("");
            if count > 0 {
                return true;
            };
        }
        false
    }
    /// returns the list of bundle ids in the database
    pub fn ids(&self) -> Vec<String> {
        let mut res: Vec<String> = Vec::new();
        let conn = self.get_connection().unwrap();
        let mut stmt = conn.prepare("SELECT bid FROM bids").unwrap();
        //dbg!(name, service, timestamp, seqno);
        let mut rows = stmt.query([]).unwrap();
        while let Some(row) = rows.next().expect("") {
            res.push(row.get(0).expect(""));
        }
        res
    }
    /// returns a list of bundle ids where either src or dst is the given node
    pub fn filter_node(&self, node: &str) -> Vec<String> {
        let mut res = Vec::new();
        let conn = self.get_connection().unwrap();
        let mut stmt = conn
            .prepare("SELECT bid FROM bids INNER JOIN bundles ON bundles.id = bids.bundle_idx WHERE src_name LIKE ?1 OR dst_name LIKE ?1")
            .unwrap();
        let mut rows = stmt.query([node]).unwrap();
        while let Some(row) = rows.next().expect("") {
            let bid = row.get(0).expect("");
            res.push(bid);
        }
        res
    }
    pub fn set_constraints(&self, bid: &str, constraints: Constraints) -> Result<()> {
        let (_, _, c_idx) = self.find_bundle_number_by_bid(bid)?;
        self.get_connection()?.execute(
            "UPDATE constraints 
            SET constraints = ?1
            WHERE id = ?2",
            params![constraints.bits(), c_idx],
        )?;
        Ok(())
    }
    pub fn get_constraints(&self, bid: &str) -> Result<Constraints> {
        let (_, _, c_idx) = self.find_bundle_number_by_bid(bid)?;
        let conn = self.get_connection()?;
        let mut stmt = conn.prepare("SELECT constraints FROM constraints WHERE id = ? LIMIT 1")?;
        let mut rows = stmt.query([c_idx])?;
        let res = rows
            .next()
            .expect("error fetching constraints row")
            .unwrap()
            .get(0)
            .expect("error fetching constraints");
        Ok(Constraints::from_bits(res).expect("could not parse constraint bits"))
    }
    pub fn add_constraints(&self, bid: &str, constraints: Constraints) -> Result<()> {
        let (_, _, c_idx) = self.find_bundle_number_by_bid(bid)?;
        self.get_connection()?.execute(
            "UPDATE constraints 
            SET constraints = constraints | ?1
            WHERE id = ?2",
            params![constraints.bits(), c_idx],
        )?;
        Ok(())
    }
    pub fn remove_constraints(&self, bid: &str, constraints: Constraints) -> Result<()> {
        let (_, _, c_idx) = self.find_bundle_number_by_bid(bid)?;
        self.get_connection()?.execute(
            "UPDATE constraints 
            SET constraints = constraints & (~?1)
            WHERE id = ?2",
            params![constraints.bits(), c_idx],
        )?;
        Ok(())
    }
    /// returns the current constraints for all bundle ids in the database
    pub fn all_constraints(&self) -> Vec<(String, Constraints)> {
        let mut res = Vec::new();
        let conn = self.get_connection().unwrap();
        let mut stmt = conn.prepare("SELECT bid,constraints FROM bids INNER JOIN constraints ON constraints.id = bids.constraints_idx").unwrap();
        let mut rows = stmt.query([]).unwrap();
        while let Some(row) = rows.next().expect("") {
            let bid = row.get(0).expect("");
            let constraints = Constraints::from_bits(row.get(1).expect(""))
                .expect("could not parse constraint bits");
            res.push((bid, constraints));
        }
        res
    }
    /// returns the current constraints for all bundle ids in the database
    pub fn filter_constraints(&self, constraints: Constraints) -> Vec<String> {
        let mut res = Vec::new();
        let conn = self.get_connection().unwrap();
        let mut stmt = conn.prepare("SELECT bid FROM bids INNER JOIN constraints ON constraints.id = bids.constraints_idx WHERE constraints.constraints & ?1").unwrap();
        let mut rows = stmt.query([constraints.bits()]).unwrap();
        while let Some(row) = rows.next().expect("") {
            let bid = row.get(0).expect("");
            res.push(bid);
        }
        res
    }
    pub fn sync_with_fs(&self, fs: &crate::D7sFs) -> Result<()> {
        /*let mut stmt = self
            .conn
            .prepare("SELECT COUNT(*) FROM bundles WHERE src_name = ? AND src_service = ? AND timestamp = ? AND seqno = ?")
            .unwrap();
        //dbg!(&bid);
        let cts: Vec<&str> = bid.split('-').collect();
        let timestamp: u64 = cts[1].parse().unwrap();
        let seqno: u64 = cts[2].parse().unwrap();

        let (name, service) = if bid.starts_with("dtn") {
            let mut tmp = cts[0].split('/').skip(2);
            (tmp.next(), tmp.next())
        } else {
            /*let mut tmp = cts[0].split('/').skip(2).collect::<Vec<&str>>().split(".");
            (tmp.next(), tmp.next())*/
            unimplemented!();
        };*/
        let conn = self.get_connection()?;
        let mut stmt = conn.prepare("SELECT * FROM bids")?;
        //dbg!(name, service, timestamp, seqno);
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let bid: String = row.get(1)?;
            //let bundle_idx: usize = row.get(2)?;
            if let Some(bundle_path) = fs.find_file_by_bid(&bid) {
                debug!("path still exists: {}", bundle_path.to_string_lossy());
            } else {
                warn!(
                    "bundle {} is missing in filesystem, removing from database",
                    &bid
                );
                self.delete(&bid)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::D7DB;

    #[test]
    fn simple_db_test() {
        let test_bundle = bp7::helpers::rnd_bundle(bp7::CreationTimestamp::now());
        //let db = D7DB::new();
        let db = D7DB::open("/tmp/d7s.db").unwrap();

        assert!(db.exists(&test_bundle.id()) == false);
        db.insert(&test_bundle).unwrap();
        assert!(db.exists(&test_bundle.id()) == true);
        db.insert(&test_bundle).unwrap();
    }
}
