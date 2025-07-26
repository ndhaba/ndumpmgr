use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, BufWriter, Read},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use compress_tools::{Ownership, uncompress_archive};
use log::{debug, info};
use rusqlite::{
    Connection, OptionalExtension, ToSql,
    types::{FromSql, FromSqlError, ToSqlOutput},
};
use tempfile::{NamedTempFile, tempdir};

use crate::{
    catalog::{
        Error, Result, logiqx,
        naming::{compress_rom_name, decompress_rom_name},
    },
    utils::*,
};

use super::GameConsole;

/**
 * Extensions for external structs/enums
 */
impl GameConsole {
    /// Attempts to find a slug to use for downloading a Redump datafile
    fn to_redump_slug(&self) -> Option<&str> {
        match self {
            Self::Dreamcast => Some("dc"),
            Self::GameCube => Some("gc"),
            Self::PSX => Some("psx"),
            Self::PS2 => Some("ps2"),
            Self::PS3 => Some("ps3"),
            Self::PSP => Some("psp"),
            Self::Wii => Some("wii"),
            Self::Xbox => Some("xbox"),
            Self::Xbox360 => Some("xbox360"),
            _ => None,
        }
    }
}

/**
 * Category
 */
enum Category {
    Games,
    Demos,
    Coverdiscs,
    Applications,
    Preproduction,
    Educational,
    BonusDiscs,
    Multimedia,
    Addons,
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
                Self::Unknown => 127,
            },
        )))
    }
}

/**
 * Internal Types
 */
#[allow(unused)]
struct Datafile {
    dfid: i64,
    console: String,
    version: String,
    last_updated: Duration,
}
#[allow(unused)]
#[derive(PartialEq, Eq, Hash)]
struct ROM {
    name: String,
    size: usize,
    crc32: i32,
    md5: [u8; 16],
    sha1: [u8; 20],
}
#[allow(unused)]
struct GameRow {
    dfid: i64,
    gid: i64,
    name: String,
    category: Category,
    revision: i64,
}
#[allow(unused)]
struct Game {
    name: String,
    category: Category,
    roms: HashSet<ROM>,
}

impl logiqx::Game for Game {
    type ROM = ROM;

    fn add_rom(&mut self, rom: Self::ROM) -> Result<()> {
        self.roms.insert(rom);
        Ok(())
    }
    fn parse_game(node: &roxmltree::Node) -> Result<Self> {
        let name: &str = node.attr("name").catalog("Failed to parse datafile")?;
        let category: &str = node
            .get_tagged_child("category")
            .catalog("Failed to parse datafile\nMissing <category> in <game>")?
            .text()
            .unwrap_or("");
        Ok(Game {
            name: name.to_string(),
            category: Category::from(category),
            roms: HashSet::new(),
        })
    }
    fn parse_game_rom(node: &roxmltree::Node) -> Result<Self::ROM> {
        let name: &str = node.attr("name").catalog("Failed to parse datafile")?;
        Ok(ROM {
            name: name.to_string(),
            size: node.attr("size").catalog("Failed to parse datafile")?,
            crc32: node.attr_hex("crc").catalog("Failed to parse datafile")?,
            md5: node.attr_hex("md5").catalog("Failed to parse datafile")?,
            sha1: node.attr_hex("sha1").catalog("Failed to parse datafile")?,
        })
    }
}

/**
 * Redump Database
 */
pub struct RedumpDatabase {
    connection: Connection,
    min_update_delay: Duration,
}

impl Drop for RedumpDatabase {
    fn drop(&mut self) {
        self.connection
            .execute("PRAGMA optimize;", ())
            .catalog("Failed to optimize redump DB")
            .unwrap();
        debug!("Optimized redump DB before dropping");
    }
}

impl RedumpDatabase {
    /// Initializes a Redump database with the given file path.
    ///
    /// Panics if the given path is not valid UTF-8.
    ///
    pub fn init(path: &PathBuf) -> Result<RedumpDatabase> {
        // open the database connection
        let connection = Connection::open(path).catalog("Failed to open redump DB")?;
        connection.set_prepared_statement_cache_capacity(32);
        debug!(r#"Opened Redump database at "{}""#, path.to_str().unwrap());
        // configure the database
        connection
            .pragma_update(None, "page_size", 16384)
            .catalog("Failed to configure redump DB")?;
        connection
            .pragma_update(None, "cache_size", 2000)
            .catalog("Failed to configure redump DB")?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .catalog("Failed to configure redump DB")?;
        connection
            .pragma_update(None, "synchronous", "normal")
            .catalog("Failed to configure redump DB")?;
        // get a list of the database's tables and indexes
        let things = {
            let mut statement = connection
                .prepare("SELECT * FROM sqlite_master WHERE type = 'table' OR type = 'index'")
                .catalog("Failed to retrieve created tables from redump DB")?;
            let mut tables: HashSet<String> = HashSet::new();
            let mut rows = statement
                .query(())
                .catalog("Failed to retrieve created tables from redump DB")?;
            while let Some(row) = rows
                .next()
                .catalog("Failed to retrieve created tables from redump DB")?
            {
                tables.insert(
                    row.get("name")
                        .catalog("Failed to retrieve created tables from redump DB")?,
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
                            "console"	TEXT NOT NULL UNIQUE,
                            "version"	TEXT NOT NULL,
                            "last_updated"	INTEGER NOT NULL,
                            PRIMARY KEY("dfid")
                        )
                    "#,
                    (),
                )
                .catalog("Failed to create tables in redump DB")?;
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
                            "category"	INTEGER NOT NULL,
                            "revision"	INTEGER NOT NULL DEFAULT 0,
                            PRIMARY KEY("gid")
                        )
                    "#,
                    (),
                )
                .catalog("Failed to create tables in redump DB")?;
            debug!("Created \"games\" table");
            changed = true;
        }
        if !things.contains("roms") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "roms" (
                            "gid"	INTEGER NOT NULL,
                            "name"	TEXT NOT NULL,
                            "size"	INTEGER NOT NULL,
                            "crc32"	INTEGER NOT NULL,
                            "md5"	BLOB NOT NULL,
                            "sha1"	BLOB NOT NULL
                        )
                    "#,
                    (),
                )
                .catalog("Failed to create tables in redump DB")?;
            debug!("Created \"roms\" table");
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
                .catalog("Failed to create tables in redump DB")?;
            debug!("Created \"game_roms\" index");
            changed = true;
        }
        // optimize the database if the tables were changed
        if changed {
            connection
                .execute("PRAGMA optimize;", ())
                .catalog("Failed to optimize redump DB")?;
            debug!("Optimized");
        }
        // return the database
        Ok(RedumpDatabase {
            connection,
            min_update_delay: Duration::from_secs(60 * 60 * 24),
        })
    }

    /// Downloads a Redump datafile for the given console.
    /// Returns the contents of said datafile
    ///
    /// Panics if the given console is not indexed by Redump.
    ///
    fn download_datafile(&self, console: GameConsole) -> Result<String> {
        // get the datafile's url
        let url: String = format!(
            "http://redump.org/datfile/{}/",
            console
                .to_redump_slug()
                .expect("Attempted to download datafile from redump.org for non-Redump console")
        );
        // create temp zip file and directory
        let zip_file = NamedTempFile::with_suffix(".zip")
            .catalog("Failed to create temporary file to download datafile")?;
        let extracted_files =
            tempdir().catalog("Failed to create directory file to extract datafile")?;
        // download the datafile's zip archivve
        {
            // make the http request
            let mut response = ureq::get(url).call().catalog("Failed to start download")?;
            // clone the file object, because that's something we have to do 😒
            let file = zip_file
                .as_file()
                .try_clone()
                .catalog("Failed to save download")?;
            // write to the file
            let mut writer = BufWriter::new(file);
            std::io::copy(&mut response.body_mut().as_reader(), &mut writer)
                .catalog("Failed to save datafile")?;
            // done
            debug!(
                "Downloaded zipped datafile to \"{}\"",
                zip_file.path().to_str().unwrap()
            );
        }
        // extract it
        uncompress_archive(
            BufReader::new(zip_file),
            extracted_files.path(),
            Ownership::Ignore,
        )
        .catalog("Failed to extract zip")?;
        debug!(
            "Extracted zipped datafile to \"{}\"",
            extracted_files.path().to_str().unwrap()
        );
        // locate the datafile
        let mut file = 'file_find: {
            // iterate over every file
            for file in extracted_files
                .path()
                .read_dir()
                .catalog("Failed to find downloaded datafile")?
            {
                let path = file.catalog("Failed to find downloaded datafile")?.path();
                // if its extension is .dat, we found it
                if let Some(extension) = path.extension() {
                    if extension == "dat" {
                        break 'file_find File::open(path).catalog("Failed to open datafile")?;
                    }
                }
            }
            // if we can't find the datafile, there's nothing we can do
            return Err(Error::new_original(
                "Failed to find downloaded datafile.\nNot included in the download",
            ));
        };
        // read the datafile
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .catalog("Failed to read datafile")?;
        Ok(contents)
    }

    /// Bumps up a game's revision by one
    ///
    fn bump_game_revision(connection: &impl CanPrepare, game_id: i64) -> Result<()> {
        // prepare a statement for the bumping
        let mut statement = connection
            .prepare_cached_common("UPDATE games SET revision = revision + 1 WHERE gid = ?")
            .catalog("Failed to update games in redump DB")?;
        // BUMP IT BUMP IT BUMP IT BUMP IT
        let row_count = statement
            .execute((game_id,))
            .catalog("Failed to update games in redump DB")?;
        // make sure the bump happened
        if row_count == 1 {
            Ok(())
        } else {
            Err(Error::new_original(format!(
                "Failed to update games in redump DB\nCan't bump revision of non-existant game (gid: {game_id})"
            )))
        }
    }

    /// Get a datafile's metadata from the database
    ///
    /// If the given console is not found, a new entry is created.
    ///
    fn get_datafile_from_db(connection: &impl CanPrepare, console: &str) -> Result<Datafile> {
        // prepare a statement to find the datafile
        let mut statement = connection
            .prepare_cached_common("SELECT * FROM datafiles WHERE console = ?")
            .catalog("Failed to retrieve datafile meta from redump DB")?;
        // parse the result
        let datafile = statement
            .query_one((console,), |row| {
                Ok(Datafile {
                    dfid: row.get("dfid").unwrap(),
                    console: row.get("console").unwrap(),
                    version: row.get("version").unwrap(),
                    last_updated: Duration::from_millis(row.get("last_updated").unwrap()),
                })
            })
            .optional()
            .catalog("Failed to retrieve datafile meta from redump DB")?;
        // handle our parsed result
        drop(statement);
        match datafile {
            // if the datafile was found, return it
            Some(datafile) => Ok(datafile),
            // if the datafile was not found, create a new one
            None => {
                // prepare an insert statement
                let mut statement = connection
                    .prepare_cached_common(
                        "INSERT INTO datafiles (console, version, last_updated) VALUES (?, ?, ?)",
                    )
                    .catalog("Failed to update datafile meta in redump DB")?;
                // execute it
                statement
                    .execute((console, "", 0))
                    .catalog("Failed to update datafile meta in redump DB")?;
                // rerun this function
                // unless some SQLite tomfoolery happens, there will at most be 1 recursive call
                drop(statement);
                RedumpDatabase::get_datafile_from_db(connection, console)
            }
        }
    }

    /// Get all of the games included from a certain datafile
    ///
    fn get_games_of_datafile(
        connection: &impl CanPrepare,
        datafile_id: i64,
    ) -> Result<Vec<GameRow>> {
        // prepare a statement to find all of the games
        let mut statement = connection
            .prepare_cached_common("SELECT gid, category, name, revision FROM games WHERE dfid = ?")
            .catalog("Failed to retrieve games stored in redump DB")?;
        // make the query
        let mut rows = statement
            .query((datafile_id,))
            .catalog("Failed to retrieve games stored in redump DB")?;
        // collect the games
        let mut games = Vec::new();
        while let Some(row) = rows
            .next()
            .catalog("Failed to retrieve games stored in redump DB")?
        {
            games.push(GameRow {
                dfid: datafile_id,
                gid: row.get(0).unwrap(),
                category: row.get(1).unwrap(),
                name: row.get(2).unwrap(),
                revision: row.get(3).unwrap(),
            });
        }
        // return the games
        Ok(games)
    }

    /// Get all of the ROM files associated with a game
    ///
    fn get_redump_roms(
        connection: &impl CanPrepare,
        game_id: i64,
        game_name: &str,
    ) -> Result<Vec<ROM>> {
        // prepare a statement to find all of the ROMs
        let mut statement = connection
            .prepare_cached_common("SELECT name, size, crc32, md5, sha1 FROM roms WHERE gid = ?")
            .catalog("Failed to retrieve game ROMs from redump DB")?;
        // make the query
        let mut rows = statement
            .query((game_id,))
            .catalog("Failed to retrieve game ROMs from redump DB")?;
        // collect all of the ROMs
        let mut roms = Vec::new();
        while let Some(row) = rows
            .next()
            .catalog("Failed to retrieve game ROMs from redump DB")?
        {
            let name: String = row.get(0).unwrap();
            roms.push(ROM {
                name: decompress_rom_name(&name, &game_name),
                size: row.get(1).unwrap(),
                crc32: row.get(2).unwrap(),
                md5: row.get(3).unwrap(),
                sha1: row.get(4).unwrap(),
            });
        }
        // done :D
        Ok(roms)
    }

    /// Inserts a new game into the database
    ///
    /// Returns its game ID
    ///
    fn insert_new_game(connection: &impl CanPrepare, datafile_id: i64, game: &Game) -> Result<i64> {
        // prepare a statement to insert the game into the database
        let mut insert_game_stmt = connection
            .prepare_cached_common(
                "INSERT INTO games (dfid, name, category) VALUES (?, ?, ?) RETURNING gid",
            )
            .catalog("Failed to update games in redump DB")?;
        // add the game
        insert_game_stmt
            .query_one((datafile_id, &game.name, &game.category), |row| {
                Ok(row.get(0).unwrap())
            })
            .catalog("Failed to retrieve games from redump DB")
    }

    /// Inserts new ROMs for a certain game in the database
    ///
    fn insert_new_roms<'a>(
        connection: &impl CanPrepare,
        game_id: i64,
        game_name: &str,
        roms: impl Iterator<Item = &'a ROM>,
    ) -> Result<()> {
        // prepare a statement for adding ROMs
        let mut statement = connection
            .prepare_cached_common(
                "INSERT INTO roms (gid, name, size, crc32, md5, sha1) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .catalog("Failed to update game ROMs in redump DB")?;
        // time to add each ROM
        for rom in roms {
            let name = compress_rom_name(&rom.name, game_name);
            statement
                .execute((game_id, name, rom.size, rom.crc32, rom.md5, rom.sha1))
                .catalog("Failed to update game ROMs in redump DB")?;
        }
        // done :D
        Ok(())
    }

    /// Removes a game from the database, along with its associated games
    ///
    fn remove_game(connection: &impl CanPrepare, game_id: i64) -> Result<()> {
        // prepare a statement for deleting the game
        let mut statement = connection
            .prepare_cached_common("DELETE FROM games WHERE gid = ?")
            .catalog("Failed to update games in redump DB")?;
        // delete it
        statement
            .execute((game_id,))
            .catalog("Failed to update games in redump DB")?;
        // delete the roms too
        drop(statement);
        RedumpDatabase::remove_game_roms(connection, game_id)
    }

    /// Removes a game's ROMs
    ///
    fn remove_game_roms(connection: &impl CanPrepare, game_id: i64) -> Result<()> {
        // prepare a statement for deleting the game ROMs
        let mut statement = connection
            .prepare_cached_common("DELETE FROM roms WHERE gid = ?")
            .catalog("Failed to update game ROMs in redump DB")?;
        // delete them
        statement
            .execute((game_id,))
            .catalog("Failed to update game ROMs in redump DB")?;
        Ok(())
    }

    /// Updates a datafile's metadata in the database
    ///
    fn update_datafile(connection: &impl CanPrepare, datafile: &Datafile) -> Result<()> {
        // prepare a statement to update the datafile
        let mut statement = connection
            .prepare_cached_common(
                "UPDATE datafiles SET version = ?, last_updated = ? WHERE dfid = ?",
            )
            .catalog("Failed to update datafile in redump DB")?;
        // update the datafile
        let rows_changed = statement
            .execute((
                &datafile.version,
                datafile.last_updated.as_millis() as i64,
                datafile.dfid,
            ))
            .catalog("Failed to update datafile in redump DB")?;
        // make sure only one row was changed
        if rows_changed == 1 {
            Ok(())
        } else {
            Err(Error::new_original(format!(
                "Failed to update datafile in redump DB\nAttempted to update non-existant datafile in DB (dfid: {})",
                datafile.dfid
            )))
        }
    }

    /// Shrink the database
    ///
    fn vacuum(&self) -> Result<()> {
        self.connection
            .execute("VACUUM", ())
            .catalog("Failed to shrink redump DB")?;
        Ok(())
    }

    /// Imports a datafile
    ///
    /// This does not check if the provided datafile contents match the console.
    /// A mismatch will almost certainly break the database.
    ///
    /// Panics if the given console is not indexed by Redump.
    ///
    fn import_datafile(&mut self, console: GameConsole, contents: &String) -> Result<()> {
        let timer = SystemTime::now();
        let datafile = logiqx::Datafile::open(contents.as_str())?;
        let header = datafile.parse_header()?;
        let mut datafile_row = RedumpDatabase::get_datafile_from_db(
            &self.connection,
            console.to_redump_slug().unwrap(),
        )?;
        if datafile_row.version == header.version {
            return Ok(());
        }
        let transaction = self
            .connection
            .transaction()
            .catalog("Failed to start transaction in redump DB")?;
        let mut stored_games: HashMap<String, i64> = {
            let games = RedumpDatabase::get_games_of_datafile(&transaction, datafile_row.dfid)?;
            let mut map: HashMap<String, i64> = HashMap::with_capacity(games.len());
            for game in games {
                map.insert(game.name, game.gid);
            }
            map
        };
        debug!("Previously stored games: {}", stored_games.len());
        let mut unchanged_entries: usize = 0;
        let mut changed_entries: usize = 0;
        let mut new_entries: usize = 0;
        let mut processed_games: HashSet<String> = HashSet::new();
        for game in datafile.parse_games::<Game>()? {
            if processed_games.contains(&game.name) {
                return Err(Error::new_original(format!(
                    "Failed to parse datafile\nDuplicate games were found: \"{}\"",
                    game.name
                )));
            }
            if let Some(gid) = stored_games.get(&game.name) {
                let gid = gid.clone();
                let roms_equal = 'are_roms_equal: {
                    let stored_roms =
                        RedumpDatabase::get_redump_roms(&transaction, gid, &game.name)?;
                    if stored_roms.len() != game.roms.len() {
                        break 'are_roms_equal false;
                    }
                    for rom in stored_roms {
                        if !game.roms.contains(&rom) {
                            break 'are_roms_equal false;
                        }
                    }
                    true
                };
                if roms_equal {
                    unchanged_entries += 1;
                } else {
                    changed_entries += 1;
                    RedumpDatabase::remove_game_roms(&transaction, gid)?;
                    RedumpDatabase::insert_new_roms(
                        &transaction,
                        gid,
                        &game.name,
                        game.roms.iter(),
                    )?;
                    RedumpDatabase::bump_game_revision(&transaction, gid)?;
                }
                stored_games.remove(&game.name);
            } else {
                new_entries += 1;
                RedumpDatabase::insert_new_roms(
                    &transaction,
                    RedumpDatabase::insert_new_game(&transaction, datafile_row.dfid, &game)?,
                    &game.name,
                    game.roms.iter(),
                )?;
            }
            processed_games.insert(game.name);
        }
        drop(processed_games);
        // by this point, only games which exist in the database but not in this datafile will remain
        let removed_games = stored_games.len();
        for (_, gid) in stored_games {
            RedumpDatabase::remove_game(&transaction, gid)?;
        }
        // update the version and last updated field within the database
        datafile_row.version = header.version.to_string();
        datafile_row.last_updated = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        RedumpDatabase::update_datafile(&transaction, &datafile_row)?;
        transaction
            .commit()
            .catalog("Failed to commit changes to redump DB")?;
        // post our stats :D
        let runtime = timer.elapsed().unwrap();
        debug!(
            "Changed entries: {}\nUnchanged entries: {}\nAdded entries: {}\nRemoved entries: {}",
            changed_entries, unchanged_entries, new_entries, removed_games
        );
        debug!(
            "Time to import: {}s {}ms",
            runtime.as_secs(),
            runtime.subsec_millis()
        );
        // we're done... finally
        Ok(())
    }

    /// Changes the minimum update delay
    ///
    /// Datfiles are updated infrequently, so this time should be in the scale of days.
    /// The higher the value, the less strain ndumplib puts on Redump servers.
    ///
    pub fn set_minimum_update_delay(&mut self, time: Duration) {
        self.min_update_delay = time;
    }

    /// Downloads the Redump datafile for the given console and imports it.
    ///
    /// If the last time this console was updated is within the minimum update delay,
    /// nothing will happen.
    ///
    fn update_console(&mut self, console: GameConsole) -> Result<()> {
        let datafile = RedumpDatabase::get_datafile_from_db(
            &self.connection,
            console.to_redump_slug().unwrap(),
        )?;
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        if current_time - datafile.last_updated >= self.min_update_delay {
            self.import_datafile(console, &self.download_datafile(console)?)?;
            info!("Updated {} games", console.to_formal_name());
        }
        Ok(())
    }

    /// Downloads all necessary Redump datafiles and imports them into the database.
    ///
    pub fn update(&mut self) -> Result<()> {
        self.update_console(GameConsole::Dreamcast)?;
        self.update_console(GameConsole::GameCube)?;
        self.update_console(GameConsole::PSX)?;
        self.update_console(GameConsole::PS2)?;
        self.update_console(GameConsole::PS3)?;
        self.update_console(GameConsole::PSP)?;
        self.update_console(GameConsole::Wii)?;
        self.update_console(GameConsole::Xbox)?;
        self.update_console(GameConsole::Xbox360)?;
        self.vacuum()?;
        Ok(())
    }
}
