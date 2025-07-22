use std::{collections::HashSet, path::PathBuf};

use log::debug;
use rusqlite::Connection;

use crate::{error_exit, settings::StorageLocations};

pub struct RedumpDatabase {
    connection: Connection,
}

impl RedumpDatabase {
    /// Initializes a Redump database with the given file path
    pub fn init(path: &PathBuf) -> RedumpDatabase {
        // open the database connection
        let connection = match Connection::open(path) {
            Ok(conn) => conn,
            Err(_) => error_exit!("Failed to open Redump database"),
        };
        debug!(r#"Opened Redump database at "{}""#, path.to_str().unwrap());
        // get a list of the database's tables
        let tables = {
            let mut statement = connection
                .prepare("SELECT * FROM sqlite_master WHERE type = ?")
                .unwrap();
            let mut tables: HashSet<String> = HashSet::new();
            let mut rows = statement.query(("table",)).unwrap();
            while let Some(row) = rows.next().unwrap() {
                tables.insert(row.get("tbl_name").unwrap());
            }
            tables
        };
        // create missing tables
        if !tables.contains("datfiles") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "datfiles" (
                            "file_id"	INTEGER NOT NULL UNIQUE,
                            "console"	TEXT NOT NULL UNIQUE,
                            "version"	TEXT NOT NULL,
                            "last_updated"	INTEGER NOT NULL,
                            PRIMARY KEY("file_id")
                        )
                    "#,
                    (),
                )
                .unwrap();
            debug!("Created \"datfiles\" table");
        }
        if !tables.contains("games") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "games" (
                            "file_id"	INTEGER NOT NULL,
                            "game_id"	INTEGER NOT NULL UNIQUE,
                            "name"	TEXT NOT NULL,
                            "rom_revision"	INTEGER NOT NULL DEFAULT 0,
                            PRIMARY KEY("game_id")
                        )
                    "#,
                    (),
                )
                .unwrap();
            debug!("Created \"games\" table");
        }
        if !tables.contains("roms") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "roms" (
                            "game_id"	INTEGER NOT NULL,
                            "name"	TEXT NOT NULL,
                            "size"	INTEGER NOT NULL,
                            "crc"	TEXT NOT NULL,
                            "sha1"	TEXT NOT NULL UNIQUE
                        )
                    "#,
                    (),
                )
                .unwrap();
            debug!("Created \"roms\" table");
        }
        // return the database
        RedumpDatabase { connection }
    }

    /// Initializes a Redump database at the default file location relative to a storage location
    pub fn init_default(locations: &StorageLocations) -> RedumpDatabase {
        let database_path = locations.default_data_path.join("redump.sqlite3");
        Self::init(&database_path)
    }
}
