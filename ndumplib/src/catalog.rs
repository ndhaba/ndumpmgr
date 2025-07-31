use std::{
    collections::{HashMap, HashSet},
    hash::*,
    path::Path,
};

use chrono::{DateTime, TimeDelta, Utc};
use log::{debug, info};
use logiqx::*;
use rusqlite::{
    Connection, OptionalExtension, ToSql,
    types::{FromSql, FromSqlError, ToSqlOutput},
};
use ureq::{Agent, agent};

use crate::{Error, GameConsole, Result, ResultUtils, catalog::logiqx::GameElement, utils::*};

mod logiqx;
mod nointro;
mod redump;

fn decompress_rom_name(rom_name: &str, game_name: &str) -> String {
    if rom_name == "$c" {
        format!("{}.cue", game_name)
    } else if rom_name == "$i" {
        format!("{}.iso", game_name)
    } else if rom_name == "$b" {
        format!("{}.bin", game_name)
    } else if rom_name.starts_with("$T") {
        format!("{} (Track {}).bin", game_name, rom_name[2..].to_string())
    } else {
        rom_name.replace("#", game_name)
    }
}
fn compress_rom_name(rom_name: &str, game_name: &str) -> String {
    let first_step = rom_name.replace(game_name, "#");
    if first_step.starts_with("# (Track ") && first_step.ends_with(").bin") {
        return format!("$T{}", first_step[9..(first_step.len() - 5)].to_string());
    } else if first_step == "#.cue" {
        return String::from("$c");
    } else if first_step == "#.iso" {
        return String::from("$i");
    } else if first_step == "#.bin" {
        return String::from("$b");
    }
    first_step
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Category {
    Games,
    Demos,
    Coverdiscs,
    Applications,
    Preproduction,
    Educational,
    BonusDiscs,
    Multimedia,
    Addons,
    Audio,
    Video,
    Unknown,
}
impl From<&str> for Category {
    fn from(value: &str) -> Self {
        match value {
            "Games" => Category::Games,
            "Demos" => Category::Demos,
            "Coverdiscs" => Category::Coverdiscs,
            "Applications" => Category::Applications,
            "Preproduction" => Category::Preproduction,
            "Educational" => Category::Educational,
            "Bonus Discs" => Category::BonusDiscs,
            "Multimedia" => Category::Multimedia,
            "Add-Ons" => Category::Addons,
            "Audio" => Category::Audio,
            "Video" => Category::Video,
            _ => Category::Unknown,
        }
    }
}
impl FromSql for Category {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value.as_i64()? {
            0 => Ok(Category::Games),
            1 => Ok(Category::Demos),
            2 => Ok(Category::Coverdiscs),
            3 => Ok(Category::Applications),
            4 => Ok(Category::Preproduction),
            5 => Ok(Category::Educational),
            6 => Ok(Category::BonusDiscs),
            7 => Ok(Category::Multimedia),
            8 => Ok(Category::Addons),
            9 => Ok(Category::Audio),
            10 => Ok(Category::Video),
            127 => Ok(Category::Unknown),
            n => Err(FromSqlError::OutOfRange(n)),
        }
    }
}
impl ToSql for Category {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(
            match self {
                Self::Games => 0,
                Self::Demos => 1,
                Self::Coverdiscs => 2,
                Self::Applications => 3,
                Self::Preproduction => 4,
                Self::Educational => 5,
                Self::BonusDiscs => 6,
                Self::Multimedia => 7,
                Self::Addons => 8,
                Self::Audio => 9,
                Self::Video => 10,
                Self::Unknown => 127,
            },
        )))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Status {
    Verified,
    BadDump,
    Unknown,
}
impl From<&str> for Status {
    fn from(value: &str) -> Self {
        match value {
            "verified" => Self::Verified,
            "baddump" => Self::BadDump,
            _ => Self::Unknown,
        }
    }
}
impl FromSql for Status {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value.as_i64()? {
            0 => Ok(Self::Verified),
            1 => Ok(Self::BadDump),
            127 => Ok(Self::Unknown),
            n => Err(FromSqlError::OutOfRange(n)),
        }
    }
}
impl ToSql for Status {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(
            match self {
                Self::Verified => 0,
                Self::BadDump => 1,
                Self::Unknown => 127,
            },
        )))
    }
}

enum Author {
    Redump,
    NoIntro,
    Other(String),
}
impl FromSql for Author {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(match value.as_str()? {
            "Redump" => Self::Redump,
            "No-Intro" => Self::NoIntro,
            v => Self::Other(v.to_string()),
        })
    }
}
impl ToSql for Author {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(rusqlite::types::Value::Text(
            match self {
                Self::Redump => "Redump".to_string(),
                Self::NoIntro => "No-Intro".to_string(),
                Self::Other(str) => str.clone(),
            },
        )))
    }
}

#[derive(PartialEq, Eq)]
pub struct ROM {
    pub name: String,
    pub status: Option<Status>,
    pub size: usize,
    pub crc32: i32,
    pub md5: [u8; 16],
    pub sha1: [u8; 20],
    pub sha256: Option<[u8; 32]>,
}
impl Hash for ROM {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.size.hash(state);
        self.crc32.hash(state);
        self.md5.hash(state);
        self.sha1.hash(state);
    }
}

pub struct Game {
    dfid: i64,
    gid: Option<i64>,
    pub name: String,
    pub categories: HashSet<Category>,
    pub roms: HashSet<ROM>,
    pub revision: i64,
    loaded: bool,
}
impl GameElement for Game {
    type ROM = ROM;

    fn add_rom(&mut self, rom: Self::ROM) -> Result<()> {
        self.roms.insert(rom);
        Ok(())
    }
    fn parse_game(node: &roxmltree::Node) -> Result<Self> {
        let name: &str = node.attr("name")?;
        let mut game = Game {
            dfid: -1,
            gid: None,
            name: name.to_string(),
            categories: HashSet::new(),
            roms: HashSet::new(),
            revision: 0,
            loaded: true,
        };
        for node in node.get_tagged_children("category") {
            game.categories.insert(node.text().unwrap_or("").into());
        }
        Ok(game)
    }
    fn parse_game_rom(node: &roxmltree::Node) -> Result<Self::ROM> {
        let name: &str = node.attr("name")?;
        Ok(ROM {
            name: name.to_string(),
            status: if node.has_attribute("status") {
                Some({
                    let str: &str = node.attr("status")?;
                    str.into()
                })
            } else {
                None
            },
            size: node.attr("size")?,
            crc32: node.attr_hex("crc")?,
            md5: node.attr_hex("md5")?,
            sha1: node.attr_hex("sha1")?,
            sha256: if node.has_attribute("sha256") {
                Some(node.attr_hex("sha256")?)
            } else {
                None
            },
        })
    }
}
impl Game {
    fn delete(&self, connection: &impl CanPrepare) -> Result<()> {
        let mut statement = connection
            .prepare_cached_common("DELETE FROM games WHERE gid = ?")
            .ndl("Failed to update games in catalog DB")?;
        statement
            .execute((self.gid.unwrap(),))
            .ndl("Failed to update games in catalog DB")?;
        let mut statement = connection
            .prepare_cached_common("DELETE FROM game_categories WHERE gid = ?")
            .ndl("Failed to update game categories in catalog DB")?;
        statement
            .execute((self.gid.unwrap(),))
            .ndl("Failed to update game ROMs in catalog DB")?;
        let mut statement = connection
            .prepare_cached_common("DELETE FROM roms WHERE gid = ?")
            .ndl("Failed to update game ROMs in catalog DB")?;
        statement
            .execute((self.gid.unwrap(),))
            .ndl("Failed to update game ROMs in catalog DB")?;
        Ok(())
    }
    fn insert_categories(&self, connection: &impl CanPrepare) -> Result<()> {
        let mut statement = connection
            .prepare_cached_common("INSERT INTO game_categories (gid, category) VALUES (?, ?)")
            .ndl("Failed to add game category to catalog DB")?;
        for category in &self.categories {
            statement
                .execute((self.gid.unwrap(), category))
                .ndl("Failed to add game category to catalog DB")?;
        }
        Ok(())
    }
    fn insert_roms(&self, connection: &impl CanPrepare) -> Result<()> {
        let mut statement = connection
            .prepare_cached_common("INSERT INTO roms (gid, name, status, size, crc32, md5, sha1, sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .ndl("Failed to add ROMs to catalog DB")?;
        for rom in &self.roms {
            let name = compress_rom_name(&rom.name, &self.name);
            statement
                .execute((
                    self.gid, &name, rom.status, rom.size, rom.crc32, rom.md5, rom.sha1, rom.sha256,
                ))
                .ndl("Failed to add ROMs to catalog DB")?;
        }
        Ok(())
    }
    fn insert(&mut self, connection: &impl CanPrepare) -> Result<()> {
        let mut insert_game_stmt = connection
            .prepare_cached_common("INSERT INTO games (dfid, name) VALUES (?, ?) RETURNING gid")
            .ndl("Failed to add game to catalog DB")?;
        let gid: i64 = insert_game_stmt
            .query_one((self.dfid, &self.name), |row| Ok(row.get(0).unwrap()))
            .ndl("Failed to add game to catalog DB")?;
        self.gid = Some(gid);
        self.revision = 0;
        self.insert_categories(connection)?;
        self.insert_roms(connection)?;
        Ok(())
    }
    fn load(&mut self, connection: &impl CanPrepare) -> Result<()> {
        if self.loaded {
            return Ok(());
        }
        let mut get_categories_stmt = connection
            .prepare_cached_common("SELECT category FROM game_categories WHERE gid = ?")
            .ndl("Failed to retreive game categories from catalog DB")?;
        let categories = get_categories_stmt
            .query_map((self.gid.unwrap(),), |row| Ok(row.get(0).unwrap()))
            .ndl("Failed to retrieve game categories from catalog DB")?;
        for category in categories {
            self.categories
                .insert(category.ndl("Failed to retrieve game categories from catalog DB")?);
        }
        let mut get_roms_stmt = connection
            .prepare_cached_common(
                "SELECT name, status, size, crc32, md5, sha1, sha256 FROM roms WHERE gid = ?",
            )
            .ndl("Failed to retrieve ROMs from catalog DB")?;
        let roms = get_roms_stmt
            .query_map((self.gid.unwrap(),), |row| {
                let name: String = row.get(0).unwrap();
                Ok(ROM {
                    name: decompress_rom_name(&name, &self.name),
                    status: row.get(1).unwrap(),
                    size: row.get(2).unwrap(),
                    crc32: row.get(3).unwrap(),
                    md5: row.get(4).unwrap(),
                    sha1: row.get(5).unwrap(),
                    sha256: row.get(6).unwrap(),
                })
            })
            .ndl("Failed to retrieve ROMs from catalog DB")?;
        for rom in roms {
            self.roms
                .insert(rom.ndl("Failed to retrieve ROMs from catalog DB")?);
        }
        self.loaded = true;
        Ok(())
    }
    fn update(&mut self, connection: &impl CanPrepare, game: Game) -> Result<bool> {
        if !self.loaded {
            panic!("Attempted to update unloaded game");
        }
        let gid = match self.gid {
            Some(v) => v,
            None => {
                return Err(Error::new_original(
                    "Failed to update game in catalog DB\nMissing gid",
                ));
            }
        };
        let mut changed = false;
        if self.categories != game.categories {
            if self.categories.len() != 0 {
                let mut statement = connection
                    .prepare_cached_common("DELETE FROM game_categories WHERE gid = ?")
                    .ndl("Failed to remove game categories from catalog DB")?;
                statement
                    .execute((gid,))
                    .ndl("Failed to remove game categories from catalog DB")?;
                changed = true;
            }
            self.categories = game.categories;
            if self.categories.len() != 0 {
                self.insert_categories(connection)?;
            }
        }
        if self.roms != game.roms {
            let roms_equal = 'roms_equal: {
                if self.roms.len() != game.roms.len() {
                    break 'roms_equal false;
                }
                let mut self_hashes = Vec::with_capacity(self.roms.len());
                let mut game_hashes = Vec::with_capacity(game.roms.len());
                let random_state = RandomState::new();
                for rom in &self.roms {
                    self_hashes.push(random_state.hash_one(rom));
                }
                for rom in &game.roms {
                    game_hashes.push(random_state.hash_one(rom));
                }
                self_hashes.sort();
                game_hashes.sort();
                self_hashes == game_hashes
            };
            if !roms_equal {
                let mut statement = connection
                    .prepare_cached_common("UPDATE games SET revision = revision + 1 WHERE gid = ?")
                    .ndl("Failed to update games in catalog DB")?;
                let row_count = statement
                    .execute((gid,))
                    .ndl("Failed to update games in catalog DB")?;
                if row_count == 0 {
                    return Err(Error::new_original(format!(
                        "Failed to update games in catalog DB\nCan't bump revision of non-existant game (gid: {gid})"
                    )));
                }
                self.revision += 1;
            }
            if self.roms.len() != 0 {
                let mut statement = connection
                    .prepare_cached_common("DELETE FROM roms WHERE gid = ?")
                    .ndl("Failed to remove ROMs from catalog DB")?;
                statement
                    .execute((gid,))
                    .ndl("Failed to remove ROMs from catalog DB")?;
            }
            self.roms = game.roms;
            if self.roms.len() != 0 {
                self.insert_roms(connection)?;
            }
            changed = true;
        }
        Ok(changed)
    }
}

#[allow(unused)]
struct Datafile {
    pub dfid: i64,
    pub name: String,
    pub author: Author,
    pub version: String,
    pub last_updated: DateTime<Utc>,
}
impl Datafile {
    fn get(connection: &impl CanPrepare, name: &str, author: &Author) -> Result<Datafile> {
        let mut statement = connection
            .prepare_cached_common("SELECT * FROM datafiles WHERE name = ?")
            .ndl("Failed to retrieve datafile meta from catalog DB")?;
        let datafile = statement
            .query_one((name,), |row| {
                Ok(Datafile {
                    dfid: row.get("dfid").unwrap(),
                    name: row.get("name").unwrap(),
                    author: row.get("author").unwrap(),
                    version: row.get("version").unwrap(),
                    last_updated: DateTime::from_timestamp_millis(row.get("last_updated").unwrap())
                        .unwrap(),
                })
            })
            .optional()
            .ndl("Failed to retrieve datafile meta from catalog DB")?;
        drop(statement);
        match datafile {
            Some(datafile) => Ok(datafile),
            None => {
                let mut statement = connection
                    .prepare_cached_common(
                        "INSERT INTO datafiles (name, author, version, last_updated) VALUES (?, ?, ?, ?)",
                    )
                    .ndl("Failed to update datafile meta in catalog DB")?;
                statement
                    .execute((name, author, "", 0))
                    .ndl("Failed to update datafile meta in catalog DB")?;
                // unless some SQLite tomfoolery happens, there will at most be 1 recursive call
                drop(statement);
                Datafile::get(connection, name, author)
            }
        }
    }
    fn get_all_games_unloaded(
        &self,
        connection: &impl CanPrepare,
    ) -> Result<HashMap<String, Game>> {
        let mut games: HashMap<String, Game> = HashMap::new();
        let mut get_games_stmt = connection
            .prepare_cached_common("SELECT gid, name, revision FROM games WHERE dfid = ?")
            .ndl("Failed to retrieve games from catalog DB")?;
        let game_rows = get_games_stmt
            .query_map((self.dfid,), |row| {
                Ok(Game {
                    dfid: self.dfid,
                    gid: Some(row.get(0).unwrap()),
                    name: row.get(1).unwrap(),
                    categories: HashSet::new(),
                    roms: HashSet::new(),
                    revision: row.get(2).unwrap(),
                    loaded: false,
                })
            })
            .ndl("Failed to retrieve games from catalog DB")?;
        for game_row in game_rows {
            let game = game_row.ndl("Failed to retrieve games from catalog DB")?;
            games.insert(game.name.clone(), game);
        }
        Ok(games)
    }
    fn update(&self, connection: &impl CanPrepare) -> Result<()> {
        let mut statement = connection
            .prepare_cached_common(
                "UPDATE datafiles SET version = ?, last_updated = ? WHERE dfid = ?",
            )
            .ndl("Failed to update datafile in catalog DB")?;
        let rows_changed = statement
            .execute((
                &self.version,
                self.last_updated.timestamp_millis(),
                self.dfid,
            ))
            .ndl("Failed to update datafile in catalog DB")?;
        if rows_changed == 1 {
            Ok(())
        } else {
            Err(Error::new_original(format!(
                "Failed to update datafile in catalog DB\nAttempted to update non-existant datafile in DB (dfid: {})",
                self.dfid
            )))
        }
    }
}

pub struct Catalog {
    connection: Connection,
    min_update_delay: TimeDelta,
}

impl Drop for Catalog {
    fn drop(&mut self) {
        self.connection.execute("VACUUM", ()).unwrap();
        self.connection.execute("PRAGMA optimize;", ()).unwrap();
    }
}

impl Catalog {
    pub fn init(path: &impl AsRef<Path>) -> Result<Catalog> {
        let connection = Connection::open(path).ndl("Failed to open catalog DB")?;
        connection.set_prepared_statement_cache_capacity(32);
        debug!(
            r#"Opened Catalog database at "{}""#,
            path.as_ref().to_str().unwrap()
        );
        // configure the database
        connection
            .pragma_update(None, "page_size", 16384)
            .ndl("Failed to configure catalog DB")?;
        connection
            .pragma_update(None, "cache_size", 2000)
            .ndl("Failed to configure catalog DB")?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .ndl("Failed to configure catalog DB")?;
        connection
            .pragma_update(None, "synchronous", "normal")
            .ndl("Failed to configure catalog DB")?;
        // get a list of the database's tables and indexes
        let things = {
            let mut statement = connection
                .prepare("SELECT * FROM sqlite_master WHERE type = 'table' OR type = 'index'")
                .ndl("Failed to retrieve created tables from catalog DB")?;
            let mut tables: HashSet<String> = HashSet::new();
            let mut rows = statement
                .query(())
                .ndl("Failed to retrieve created tables from catalog DB")?;
            while let Some(row) = rows
                .next()
                .ndl("Failed to retrieve created tables from catalog DB")?
            {
                tables.insert(
                    row.get("name")
                        .ndl("Failed to retrieve created tables from catalog DB")?,
                );
            }
            tables
        };
        // create missing tables and indexes
        let mut changed = false;
        if !things.contains("datafiles") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "datafiles" (
                            "dfid"	INTEGER NOT NULL UNIQUE,
                            "name"	TEXT NOT NULL UNIQUE,
                            "author"    TEXT NOT NULL,
                            "version"	TEXT NOT NULL,
                            "last_updated"	INTEGER NOT NULL,
                            PRIMARY KEY("dfid")
                        )
                    "#,
                    (),
                )
                .ndl("Failed to create tables in catalog DB")?;
            debug!("Created \"datafiles\" table");
            changed = true;
        }
        if !things.contains("games") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "games" (
                            "dfid"	INTEGER NOT NULL,
                            "gid"	INTEGER NOT NULL UNIQUE,
                            "name"	TEXT NOT NULL,
                            "revision"	INTEGER NOT NULL DEFAULT 0,
                            PRIMARY KEY("gid")
                        )
                    "#,
                    (),
                )
                .ndl("Failed to create tables in catalog DB")?;
            debug!("Created \"games\" table");
            changed = true;
        }
        if !things.contains("game_categories") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "game_categories" (
                            "gid"	INTEGER NOT NULL,
                            "category"	INTEGER NOT NULL
                        )
                    "#,
                    (),
                )
                .ndl("Failed to create tables in catalog DB")?;
            debug!("Created \"game_categories\" table");
            changed = true;
        }
        if !things.contains("roms") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "roms" (
                            "gid"	INTEGER NOT NULL,
                            "name"	TEXT NOT NULL,
                            "status"	INTEGER,
                            "size"	INTEGER NOT NULL,
                            "crc32"	INTEGER NOT NULL,
                            "md5"	BLOB NOT NULL,
                            "sha1"	BLOB NOT NULL,
                            "sha256"	BLOB
                        )
                    "#,
                    (),
                )
                .ndl("Failed to create tables in catalog DB")?;
            debug!("Created \"roms\" table");
            changed = true;
        }
        if !things.contains("game_category_index") {
            connection
                .execute(
                    r#"
                        CREATE INDEX "game_category_index" ON "game_categories" (
                            "gid"	DESC
                        )
                    "#,
                    (),
                )
                .ndl("Failed to create tables in catalog DB")?;
            debug!("Created \"game_category_index\" index");
            changed = true;
        }
        if !things.contains("game_roms") {
            connection
                .execute(
                    r#"
                        CREATE INDEX "game_roms" ON "roms" (
                            "gid"	DESC
                        )
                    "#,
                    (),
                )
                .ndl("Failed to create tables in catalog DB")?;
            debug!("Created \"game_roms\" index");
            changed = true;
        }
        // optimize the database if the tables were changed
        if changed {
            connection
                .execute("PRAGMA optimize;", ())
                .ndl("Failed to optimize catalog DB")?;
            debug!("Optimized");
        }
        // return the database
        Ok(Catalog {
            connection,
            min_update_delay: TimeDelta::days(2),
        })
    }

    fn import_datafile_games<'a>(
        &mut self,
        datafile: &Datafile,
        xml: XMLDatafile<'a>,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction()
            .ndl("Failed to start transaction in catalog DB")?;
        let mut stored_games: HashMap<String, Game> =
            datafile.get_all_games_unloaded(&transaction)?;
        debug!("Previously stored games: {}", stored_games.len());
        let mut unchanged_entries: usize = 0;
        let mut changed_entries: usize = 0;
        let mut new_entries: usize = 0;
        let mut processed_games: HashSet<String> = HashSet::new();
        for mut game_element in xml.parse_games::<Game>()? {
            if processed_games.contains(&game_element.name) {
                return Err(Error::new_original(format!(
                    "Failed to parse datafile\nDuplicate games were found: \"{}\"",
                    game_element.name
                )));
            }
            let name = game_element.name.clone();
            if let Some(game) = stored_games.get_mut(&game_element.name) {
                game.load(&transaction)?;
                if game.update(&transaction, game_element)? {
                    changed_entries += 1;
                } else {
                    unchanged_entries += 1;
                }
                stored_games.remove(&name);
            } else {
                game_element.dfid = datafile.dfid;
                game_element.insert(&transaction)?;
                new_entries += 1;
            }
            processed_games.insert(name);
        }
        drop(processed_games);
        // by this point, only games which exist in the database but not in this datafile will remain
        let removed_games = stored_games.len();
        for (_, game) in stored_games {
            game.delete(&transaction)?;
        }
        transaction
            .commit()
            .ndl("Failed to commit changes to catalog DB")?;
        debug!(
            "Changed entries: {}\nUnchanged entries: {}\nAdded entries: {}\nRemoved entries: {}",
            changed_entries, unchanged_entries, new_entries, removed_games
        );
        Ok(())
    }

    fn oldest_nointro_datafile_time(&self) -> Result<DateTime<Utc>> {
        let mut statement = self
            .connection
            .prepare_cached("SELECT MIN(last_updated) FROM datafiles WHERE author = ?")
            .ndl("Failed to optimize No-Intro requests")?;
        match statement
            .query_one(("No-Intro",), |row| Ok(row.get(0).unwrap()))
            .ndl("Failed to optimize No-Intro requests")?
        {
            Some(timestamp) => Ok(DateTime::from_timestamp_millis(timestamp).unwrap()),
            None => Ok(DateTime::from_timestamp_millis(0).unwrap()),
        }
    }

    fn update_nointro_console(
        &mut self,
        console: GameConsole,
        agent: &Agent,
        links: &HashMap<String, nointro::DatafileLink>,
    ) -> Result<()> {
        let datafile_name = console.nointro_datafile_name().unwrap();
        let mut datafile = Datafile::get(&self.connection, datafile_name, &Author::NoIntro)?;
        if Utc::now()
            < datafile
                .last_updated
                .checked_add_signed(self.min_update_delay)
                .unwrap()
        {
            return Ok(());
        }
        let link = match links.get(datafile_name) {
            Some(link) => link,
            None => return Ok(()),
        };
        if link.last_updated <= datafile.last_updated {
            datafile.last_updated = Utc::now();
            datafile.update(&self.connection)?;
            debug!("Datafile \"{datafile_name}\" is already up-to-date. Skipping...");
            return Ok(());
        }
        let url = match &link.link {
            Some(url) => url,
            None => return Ok(()),
        };
        let content = nointro::download_datafile(agent, url)?;
        let xml = logiqx::XMLDatafile::open(&content)?;
        let header = xml.parse_header()?;
        datafile.version = header.version.to_string();
        self.import_datafile_games(&datafile, xml)?;
        datafile.last_updated = Utc::now();
        datafile.update(&self.connection)?;
        info!("Updated {} games", console.formal_name());
        Ok(())
    }

    fn update_redump_console(&mut self, console: GameConsole) -> Result<()> {
        let datafile_name = console.redump_datafile_name().unwrap();
        let mut datafile = Datafile::get(&self.connection, datafile_name, &Author::Redump)?;
        if Utc::now()
            < datafile
                .last_updated
                .checked_add_signed(self.min_update_delay)
                .unwrap()
        {
            return Ok(());
        }
        let content = redump::download_datafile(console.redump_slug().unwrap())?;
        let xml = logiqx::XMLDatafile::open(&content)?;
        let header = xml.parse_header()?;
        if datafile.version == header.version {
            datafile.last_updated = Utc::now();
            datafile.update(&self.connection)?;
            debug!("Datafile \"{datafile_name}\" is already up-to-date. Skipping...");
            return Ok(());
        }
        datafile.version = header.version.to_string();
        self.import_datafile_games(&datafile, xml)?;
        datafile.last_updated = Utc::now();
        datafile.update(&self.connection)?;
        info!("Updated {} games", console.formal_name());
        Ok(())
    }

    pub fn update_all_consoles(&mut self) -> Result<()> {
        if Utc::now()
            >= self
                .oldest_nointro_datafile_time()?
                .checked_add_signed(self.min_update_delay)
                .unwrap()
        {
            let agent = agent();
            let no_intro_links = nointro::load_datafile_links(&agent)?;
            self.update_nointro_console(GameConsole::GB, &agent, &no_intro_links)?;
            self.update_redump_console(GameConsole::Dreamcast)?;
            self.update_redump_console(GameConsole::GameCube)?;
            self.update_nointro_console(GameConsole::GBC, &agent, &no_intro_links)?;
            self.update_redump_console(GameConsole::PSX)?;
            self.update_redump_console(GameConsole::PS2)?;
            self.update_nointro_console(GameConsole::GBA, &agent, &no_intro_links)?;
            self.update_redump_console(GameConsole::PS3)?;
            self.update_redump_console(GameConsole::PSP)?;
            self.update_nointro_console(GameConsole::N64, &agent, &no_intro_links)?;
            self.update_redump_console(GameConsole::Wii)?;
            self.update_redump_console(GameConsole::Xbox)?;
            self.update_redump_console(GameConsole::Xbox360)?;
        } else {
            self.update_redump_console(GameConsole::Dreamcast)?;
            self.update_redump_console(GameConsole::GameCube)?;
            self.update_redump_console(GameConsole::PSX)?;
            self.update_redump_console(GameConsole::PS2)?;
            self.update_redump_console(GameConsole::PS3)?;
            self.update_redump_console(GameConsole::PSP)?;
            self.update_redump_console(GameConsole::Wii)?;
            self.update_redump_console(GameConsole::Xbox)?;
            self.update_redump_console(GameConsole::Xbox360)?;
        }
        Ok(())
    }
}
