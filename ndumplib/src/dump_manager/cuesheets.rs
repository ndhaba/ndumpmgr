use std::{collections::HashSet, path::Path};

use chrono::{DateTime, TimeDelta, Utc};
use log::{debug, info};
use once_cell::sync::OnceCell;
use rusqlite::{Connection, OptionalExtension};
use sha1::{Digest, Sha1};
use tempfile::TempDir;

use crate::{
    Error, GameConsole, Result, ResultUtils,
    utils::{
        CanPrepare, get_database_indexes, get_database_tables, regex, setup_database_default_config,
    },
};

mod redump;

struct Cuesheet {
    pub console: GameConsole,
    pub last_updated: DateTime<Utc>,
}
impl Cuesheet {
    fn get(connection: &impl CanPrepare, console: GameConsole) -> Result<Cuesheet> {
        let mut statement = connection
            .prepare_cached_common("SELECT * FROM cuesheets WHERE console = ?")
            .ndl("Failed to retrieve cuesheet meta from cuesheet DB")?;
        let cuesheet = statement
            .query_one((console.formal_name(),), |row| {
                Ok(Cuesheet {
                    console,
                    last_updated: DateTime::from_timestamp_millis(row.get("last_updated").unwrap())
                        .unwrap(),
                })
            })
            .optional()
            .ndl("Failed to retrieve cuesheet meta from cuesheet DB")?;
        drop(statement);
        match cuesheet {
            Some(cuesheet) => Ok(cuesheet),
            None => {
                let mut statement = connection
                    .prepare_cached_common(
                        "INSERT INTO cuesheets (console, last_updated) VALUES (?, ?)",
                    )
                    .ndl("Failed to update cuesheet meta in cuesheet DB")?;
                statement
                    .execute((console.formal_name(), 0))
                    .ndl("Failed to update cuesheet meta in cuesheet DB")?;
                // unless some SQLite tomfoolery happens, there will at most be 1 recursive call
                drop(statement);
                Cuesheet::get(connection, console)
            }
        }
    }
    fn update(&self, connection: &impl CanPrepare) -> Result<()> {
        let mut statement = connection
            .prepare_cached_common("UPDATE cuesheets SET last_updated = ? WHERE console = ?")
            .ndl("Failed to update cuesheets in cuesheet DB")?;
        let rows_changed = statement
            .execute((
                self.last_updated.timestamp_millis(),
                self.console.formal_name(),
            ))
            .ndl("Failed to update cuesheets in cuesheet DB")?;
        if rows_changed == 1 {
            Ok(())
        } else {
            Err(Error::new_original(
                "Failed to update cuesheets in cuesheet DB\nAttempted to update non-existant cuesheets in DB",
            ))
        }
    }
}

pub struct Cuesheets {
    connection: Connection,
    cue_update_delay: TimeDelta,
}

static SUPPORTED_COMMANDS: OnceCell<HashSet<&'static str>> = OnceCell::new();

pub fn get_track_filenames(content: &impl AsRef<str>) -> Vec<String> {
    regex!(r#"(?<=FILE ")[^"]+"#)
        .captures_iter(content.as_ref())
        .map(|v| v.unwrap().get(0).unwrap().as_str().to_string())
        .collect()
}

pub fn neutralize(content: &impl AsRef<str>, path: &impl AsRef<Path>) -> String {
    let supported_commands = SUPPORTED_COMMANDS.get_or_init(|| {
        let mut set = HashSet::new();
        set.insert("FILE");
        set.insert("TRACK");
        set.insert("PREGAP");
        set.insert("INDEX");
        set.insert("POSTGAP");
        set
    });
    content
        .as_ref()
        .trim()
        .split("\n")
        .filter(|v| {
            let v = v.trim();
            supported_commands.contains(&v[0..v.find(' ').unwrap()])
        })
        .collect::<Vec<&str>>()
        .join("\n")
        .replace(path.as_ref().file_stem().unwrap().to_str().unwrap(), "$")
}

impl Drop for Cuesheets {
    fn drop(&mut self) {
        self.connection.execute("VACUUM", ()).unwrap();
        self.connection.execute("PRAGMA optimize;", ()).unwrap();
    }
}

impl Cuesheets {
    pub fn find_cue_hash(
        &self,
        content: &impl AsRef<str>,
        path: &impl AsRef<Path>,
    ) -> Result<Option<[u8; 20]>> {
        let content = neutralize(content, path);
        let mut statement = self
            .connection
            .prepare_cached("SELECT sha1 FROM cues WHERE content = ?")
            .ndl("Failed to lookup cue in cuesheet DB")?;
        statement
            .query_one((content,), |row| Ok(row.get(0).unwrap()))
            .optional()
            .ndl("Failed to lookup cue in cuesheet DB")
    }

    pub fn init(path: &impl AsRef<Path>) -> Result<Cuesheets> {
        let connection = Connection::open(path).ndl("Failed to open cuesheet DB")?;
        setup_database_default_config(&connection)?;
        debug!(
            r#"Opened cuesheet database at "{}""#,
            path.as_ref().to_str().unwrap()
        );
        // create missing tables and indexes
        let tables = get_database_tables(&connection)?;
        let indexes = get_database_indexes(&connection)?;
        let mut changed = false;
        if !tables.contains("cuesheets") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "cuesheets" (
                            "console"	TEXT NOT NULL UNIQUE,
                            "last_updated"	INTEGER NOT NULL,
                            PRIMARY KEY("console")
                        )
                    "#,
                    (),
                )
                .ndl("Failed to create tables in cuesheet DB")?;
            debug!("Created \"cuesheets\" table");
            changed = true;
        }
        if !tables.contains("cues") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "cues" (
                            "sha1"	BLOB NOT NULL UNIQUE,
                            "content"	TEXT NOT NULL,
                            PRIMARY KEY("sha1")
                        )
                    "#,
                    (),
                )
                .ndl("Failed to create tables in cuesheet DB")?;
            debug!("Created \"cues\" table");
            changed = true;
        }
        if !indexes.contains_key("content_to_cue") {
            connection
                .execute(
                    r#"
                        CREATE INDEX "content_to_cue" ON "cues" (
                            "content"	DESC
                        )
                    "#,
                    (),
                )
                .ndl("Failed to create tables in cuesheet DB")?;
            debug!("Created \"content_to_cue\" index");
            changed = true;
        }
        // optimize the database if the tables were changed
        if changed {
            connection
                .execute("PRAGMA optimize;", ())
                .ndl("Failed to optimize cuesheet DB")?;
            debug!("Optimized cuesheet database");
        }
        // return the database
        Ok(Cuesheets {
            connection,
            cue_update_delay: TimeDelta::days(7),
        })
    }

    fn import_cues(&mut self, dir: TempDir) -> Result<()> {
        let transaction = self
            .connection
            .transaction()
            .ndl("Failed to import cues to cuesheet DB")?;
        let mut statement = transaction
            .prepare_cached("INSERT OR IGNORE INTO cues (sha1, content) VALUES (?, ?)")
            .ndl("Failed to import cues to cuesheet DB")?;
        for file in std::fs::read_dir(&dir).ndl("Failed to import cues to cuesheet DB")? {
            let dir_entry = file.ndl("Failed to import cues to cuesheet DB")?;
            let path = dir_entry.path();
            if !path.is_file() {
                continue;
            }
            let content =
                std::fs::read_to_string(path).ndl("Failed to import cues to cuesheet DB")?;
            let mut sha1 = Sha1::new();
            sha1.update(&content);
            let hash: [u8; 20] = sha1.finalize().into();
            statement
                .execute((
                    hash,
                    neutralize(&content, &dir_entry.file_name().to_str().unwrap()),
                ))
                .ndl("Failed to import cues to cuesheet DB")?;
        }
        drop(statement);
        transaction
            .commit()
            .ndl("Failed to import cues to cuesheet DB")?;
        Ok(())
    }

    fn update_redump_cuesheets(&mut self, console: GameConsole) -> Result<()> {
        let mut cuesheet = Cuesheet::get(&self.connection, console)?;
        if Utc::now()
            < cuesheet
                .last_updated
                .checked_add_signed(self.cue_update_delay)
                .unwrap()
        {
            return Ok(());
        }
        self.import_cues(redump::download_cuesheets(
            console.redump_cue_slug().unwrap(),
        )?)?;
        cuesheet.last_updated = Utc::now();
        cuesheet.update(&self.connection)?;
        info!("Updated {} cuesheet", console.formal_name());
        Ok(())
    }

    pub fn update_all_consoles(&mut self) -> Result<()> {
        self.update_redump_cuesheets(GameConsole::PSX)
    }
}
