use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, BufWriter, Read},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use compress_tools::{Ownership, uncompress_archive};
use log::debug;
use roxmltree::{Document, ParsingOptions};
use rusqlite::{Connection, OptionalExtension};
use tempfile::{NamedTempFile, tempdir};

use crate::utils::*;

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
 * Error Types
 */
#[derive(Debug)]
enum InnerError {
    IOError(std::io::Error),
    NetError(ureq::Error),
    ArchiveError(compress_tools::Error),
    XMLError(roxmltree::Error),
    XMLUtilsError(XMLUtilsError),
    SQLiteError(rusqlite::Error),
}

impl std::fmt::Display for InnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IOError(e) => write!(f, "I/O Error: {e}"),
            Self::NetError(e) => write!(f, "Network Error: {e}"),
            Self::ArchiveError(e) => write!(f, "Archive Error: {e}"),
            Self::XMLError(e) => write!(f, "XML Error: {e}"),
            Self::XMLUtilsError(e) => write!(f, "{e}"),
            Self::SQLiteError(e) => write!(f, "SQLite Error: {e}"),
        }
    }
}

impl From<std::io::Error> for InnerError {
    fn from(error: std::io::Error) -> Self {
        Self::IOError(error)
    }
}
impl From<ureq::Error> for InnerError {
    fn from(error: ureq::Error) -> Self {
        Self::NetError(error)
    }
}
impl From<compress_tools::Error> for InnerError {
    fn from(error: compress_tools::Error) -> Self {
        Self::ArchiveError(error)
    }
}
impl From<roxmltree::Error> for InnerError {
    fn from(error: roxmltree::Error) -> Self {
        Self::XMLError(error)
    }
}
impl From<XMLUtilsError> for InnerError {
    fn from(value: XMLUtilsError) -> Self {
        Self::XMLUtilsError(value)
    }
}
impl From<rusqlite::Error> for InnerError {
    fn from(error: rusqlite::Error) -> Self {
        Self::SQLiteError(error)
    }
}

#[derive(Debug)]
pub struct Error(String, Option<InnerError>);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error(str, Some(err)) => write!(f, "{str}\n{err}"),
            Error(str, None) => write!(f, "{str}"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// Creates a new [Error] with the given message and internal error
    ///
    fn new<S: AsRef<str>, E: Into<InnerError>>(message: S, error: E) -> Error {
        Error(message.as_ref().to_string(), Some(error.into()))
    }
    /// Creates a new [Error] without a separate internal error
    ///
    fn new_original<S: AsRef<str>>(message: S) -> Error {
        Error(message.as_ref().to_string(), None)
    }
}

type Result<T> = std::result::Result<T, Error>;

#[doc(hidden)]
trait __ResultUtils<T> {
    fn redump<S: AsRef<str>>(self, message: S) -> Result<T>;
}
impl<T, E: Into<InnerError>> __ResultUtils<T> for std::result::Result<T, E> {
    fn redump<S: AsRef<str>>(self, message: S) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::new(message, e)),
        }
    }
}
impl<T> __ResultUtils<T> for std::option::Option<T> {
    fn redump<S: AsRef<str>>(self, message: S) -> Result<T> {
        match self {
            Some(v) => Ok(v),
            None => Err(Error::new_original(message)),
        }
    }
}

/**
 * Internal Types
 */
struct RedumpDatafile {
    dfid: i64,
    console: String,
    version: String,
    last_updated: Duration,
}
struct RedumpGame {
    dfid: i64,
    gid: i64,
    name: String,
    revision: i64,
}
#[derive(PartialEq, Eq, Hash)]
struct RedumpRom {
    name: String,
    size: usize,
    crc: u32,
    sha1: [u8; 20],
}

/**
 * Redump Database
 */
pub struct RedumpDatabase {
    connection: Connection,
    min_update_delay: Duration,
}

impl RedumpDatabase {
    /// Initializes a Redump database with the given file path.
    ///
    /// Panics if the given path is not valid UTF-8.
    ///
    pub fn init(path: &PathBuf) -> Result<RedumpDatabase> {
        // open the database connection
        let connection = Connection::open(path).redump("Failed to open Redump database")?;
        connection.set_prepared_statement_cache_capacity(16);
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .redump("Failed to open Redump database")?;
        debug!(r#"Opened Redump database at "{}""#, path.to_str().unwrap());
        // get a list of the database's tables
        let tables = {
            let mut statement = connection
                .prepare("SELECT * FROM sqlite_master WHERE type = ?")
                .redump("Failed to retrieve created tables from redump DB")?;
            let mut tables: HashSet<String> = HashSet::new();
            let mut rows = statement
                .query(("table",))
                .redump("Failed to retrieve created tables from redump DB")?;
            while let Some(row) = rows
                .next()
                .redump("Failed to retrieve created tables from redump DB")?
            {
                tables.insert(
                    row.get("tbl_name")
                        .redump("Failed to retrieve created tables from redump DB")?,
                );
            }
            tables
        };
        // create missing tables
        if !tables.contains("datafiles") {
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
                .redump("Failed to create tables in redump DB")?;
            debug!("Created \"datafiles\" table");
        }
        if !tables.contains("games") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "games" (
                            "dfid"	INTEGER NOT NULL,
                            "gid"	INTEGER NOT NULL UNIQUE,
                            "name"	TEXT NOT NULL UNIQUE,
                            "revision"	INTEGER NOT NULL DEFAULT 0,
                            PRIMARY KEY("gid")
                        )
                    "#,
                    (),
                )
                .redump("Failed to create tables in redump DB")?;
            debug!("Created \"games\" table");
        }
        if !tables.contains("roms") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "roms" (
                            "gid"	INTEGER NOT NULL,
                            "name"	TEXT NOT NULL UNIQUE,
                            "size"	INTEGER NOT NULL,
                            "crc"	INTEGER NOT NULL,
                            "sha1"	BLOB NOT NULL
                        )
                    "#,
                    (),
                )
                .redump("Failed to create tables in redump DB")?;
            debug!("Created \"roms\" table");
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
                .expect("Attempted to download datafile for non-Redump console")
        );
        // create temp zip file and directory
        let zip_file = NamedTempFile::with_suffix(".zip")
            .redump("Failed to create temporary file to download datafile")?;
        let extracted_files =
            tempdir().redump("Failed to create directory file to extract datafile")?;
        // download the datafile's zip archivve
        {
            // make the http request
            let mut response = ureq::get(url).call().redump("Failed to start download")?;
            // clone the file object, because that's something we have to do 😒
            let file = zip_file
                .as_file()
                .try_clone()
                .redump("Failed to save download")?;
            // write to the file
            let mut writer = BufWriter::new(file);
            std::io::copy(&mut response.body_mut().as_reader(), &mut writer)
                .redump("Failed to save datafile")?;
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
        .redump("Failed to extract zip")?;
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
                .redump("Failed to find downloaded datafile")?
            {
                let path = file.redump("Failed to find downloaded datafile")?.path();
                // if its extension is .dat, we found it
                if let Some(extension) = path.extension() {
                    if extension == "dat" {
                        break 'file_find File::open(path).redump("Failed to open datafile")?;
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
            .redump("Failed to read datafile")?;
        Ok(contents)
    }

    /// Bumps up a game's revision by one
    ///
    fn bump_game_revision(connection: &impl CanPrepare, game_id: i64) -> Result<()> {
        // prepare a statement for the bumping
        let mut statement = connection
            .prepare_cached_common("UPDATE games SET revision = revision + 1 WHERE game_id = ?")
            .redump("Failed to update games in redump DB")?;
        // BUMP IT BUMP IT BUMP IT BUMP IT
        let row_count = statement
            .execute((game_id,))
            .redump("Failed to update games in redump DB")?;
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
    fn get_datafile_from_db(connection: &impl CanPrepare, console: &str) -> Result<RedumpDatafile> {
        // prepare a statement to find the datafile
        let mut statement = connection
            .prepare_cached_common("SELECT * FROM datafiles WHERE console = ?")
            .redump("Failed to retrieve datafile meta from redump DB")?;
        // parse the result
        let datafile = statement
            .query_one((console,), |row| {
                Ok(RedumpDatafile {
                    dfid: row.get("dfid").unwrap(),
                    console: row.get("console").unwrap(),
                    version: row.get("version").unwrap(),
                    last_updated: Duration::from_millis(row.get("last_updated").unwrap()),
                })
            })
            .optional()
            .redump("Failed to retrieve datafile meta from redump DB")?;
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
                    .redump("Failed to update datafile meta in redump DB")?;
                // execute it
                statement
                    .execute((console, "", 0))
                    .redump("Failed to update datafile meta in redump DB")?;
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
    ) -> Result<Vec<RedumpGame>> {
        // prepare a statement to find all of the games
        let mut statement = connection
            .prepare_cached_common("SELECT gid, name, revision FROM games WHERE dfid = ?")
            .redump("Failed to retrieve games stored in redump DB")?;
        // make the query
        let mut rows = statement
            .query((datafile_id,))
            .redump("Failed to retrieve games stored in redump DB")?;
        // collect the games
        let mut games = Vec::new();
        while let Some(row) = rows
            .next()
            .redump("Failed to retrieve games stored in redump DB")?
        {
            games.push(RedumpGame {
                dfid: datafile_id,
                gid: row.get("gid").unwrap(),
                name: row.get("name").unwrap(),
                revision: row.get("revision").unwrap(),
            });
        }
        // return the games
        Ok(games)
    }

    /// Get all of the ROM files associated with a game
    ///
    fn get_redump_roms(connection: &impl CanPrepare, game_id: i64) -> Result<Vec<RedumpRom>> {
        // prepare a statement to find all of the ROMs
        let mut statement = connection
            .prepare_cached_common("SELECT name, size, crc, sha1 FROM roms WHERE gid = ?")
            .redump("Failed to retrieve game ROMs from redump DB")?;
        // make the query
        let mut rows = statement
            .query((game_id,))
            .redump("Failed to retrieve game ROMs from redump DB")?;
        // collect all of the ROMs
        let mut roms = Vec::new();
        while let Some(row) = rows
            .next()
            .redump("Failed to retrieve game ROMs from redump DB")?
        {
            roms.push(RedumpRom {
                name: row.get("name").unwrap(),
                size: row.get("size").unwrap(),
                crc: row.get("crc").unwrap(),
                sha1: row.get("sha1").unwrap(),
            });
        }
        // done :D
        Ok(roms)
    }

    /// Inserts a new game into the database
    ///
    /// Returns its game ID
    ///
    fn insert_new_game(
        connection: &impl CanPrepare,
        datafile_id: i64,
        name: &String,
    ) -> Result<i64> {
        // prepare a statement to insert the game into the database
        let mut insert_game_stmt = connection
            .prepare_cached_common("INSERT INTO games (dfid, name) VALUES (?, ?)")
            .redump("Failed to update games in redump DB")?;
        // add the game
        insert_game_stmt
            .execute((datafile_id, name))
            .redump("Failed to update games in redump DB")?;
        // prepare a statement to get the game ID
        let mut get_game_id_stmt = connection
            .prepare_cached_common("SELECT gid FROM games WHERE dfid = ? AND name = ?")
            .redump("Failed to retrieve games from redump DB")?;
        // get the game ID
        get_game_id_stmt
            .query_one((datafile_id, name), |row| Ok(row.get("gid").unwrap()))
            .redump("Failed to retrieve games from redump DB")
    }

    /// Inserts new ROMs for a certain game in the database
    ///
    fn insert_new_roms<'a>(
        connection: &impl CanPrepare,
        game_id: i64,
        roms: impl Iterator<Item = &'a RedumpRom>,
    ) -> Result<()> {
        // prepare a statement for adding ROMs
        let mut statement = connection
            .prepare_cached_common(
                "INSERT INTO roms (gid, name, size, crc, sha1) VALUES (?, ?, ?, ?, ?)",
            )
            .redump("Failed to update game ROMs in redump DB")?;
        // time to add each ROM
        for rom in roms {
            statement
                .execute((game_id, &rom.name, rom.size, rom.crc, rom.sha1))
                .redump("Failed to update game ROMs in redump DB")?;
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
            .redump("Failed to update games in redump DB")?;
        // delete it
        statement
            .execute((game_id,))
            .redump("Failed to update games in redump DB")?;
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
            .redump("Failed to update game ROMs in redump DB")?;
        // delete them
        statement
            .execute((game_id,))
            .redump("Failed to update game ROMs in redump DB")?;
        Ok(())
    }

    /// Updates a datafile's metadata in the database
    ///
    fn update_datafile(connection: &impl CanPrepare, datafile: &RedumpDatafile) -> Result<()> {
        // prepare a statement to update the datafile
        let mut statement = connection
            .prepare_cached_common(
                "UPDATE datafiles SET version = ?, last_updated = ? WHERE dfid = ?",
            )
            .redump("Failed to update datafile in redump DB")?;
        // update the datafile
        let rows_changed = statement
            .execute((
                &datafile.version,
                datafile.last_updated.as_millis() as i64,
                datafile.dfid,
            ))
            .redump("Failed to update datafile in redump DB")?;
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

    /// Imports a datafile
    ///
    /// This does not check if the provided datafile contents match the console.
    /// A mismatch will almost certainly break the database.
    ///
    /// Panics if the given console is not indexed by Redump.
    ///
    fn import_datafile(&mut self, console: GameConsole, contents: &String) -> Result<()> {
        // parse the xml-formatted datafile
        let timer = SystemTime::now();
        let document = Document::parse_with_options(
            contents.as_ref(),
            ParsingOptions {
                allow_dtd: true,
                nodes_limit: u32::MAX,
            },
        )
        .redump("Failed to parse datafile")?;
        // find the root element
        let datafile_node = document
            .root()
            .get_tagged_child("datafile")
            .redump("Failed to parse datafile\nMissing <datafile>")?;
        // find the version
        let version = datafile_node
            .get_tagged_child("header")
            .redump("Failed to parse datafile\nMissing <header>")?
            .get_tagged_child("version")
            .redump("Failed to parse datafile\n<header> missing <version>")?
            .text()
            .unwrap_or("");
        // get the in-database datafile metadata
        let transaction = self
            .connection
            .transaction()
            .redump("Failed to start transaction in redump DB")?;
        let mut datafile =
            RedumpDatabase::get_datafile_from_db(&transaction, console.to_redump_slug().unwrap())?;
        // get all of the currently stored games
        let mut stored_games: HashMap<String, i64> = HashMap::new();
        for game in RedumpDatabase::get_games_of_datafile(&transaction, datafile.dfid)? {
            stored_games.insert(game.name, game.gid);
        }
        debug!("Previously stored games: {}", stored_games.len());
        // iterate over every game
        let mut unchanged_entries: usize = 0;
        let mut changed_entries: usize = 0;
        let mut new_entries: usize = 0;
        let mut processed_games: HashSet<String> = HashSet::new();
        for game in datafile_node.get_tagged_children("game") {
            // get the game's name
            let game_name: String = game.attr("name").redump("Failed to parse datafile")?;
            // make sure this hasn't been processed already
            if processed_games.contains(&game_name) {
                return Err(Error::new_original(format!(
                    "Failed to parse datafile\nDuplicate games were found: \"{game_name}\"",
                )));
            }
            // get the game's ROMs
            let mut roms = HashSet::new();
            for rom_element in game.get_tagged_children("rom") {
                let error_message = format!("Failed to parse datafile (at game \"{game_name}\")");
                roms.insert(RedumpRom {
                    name: rom_element.attr("name").redump(&error_message)?,
                    size: rom_element.attr("size").redump(&error_message)?,
                    crc: rom_element.attr_hex("crc").redump(&error_message)?,
                    sha1: rom_element.attr_hex("sha1").redump(&error_message)?,
                });
            }
            // has this game already been added?
            if let Some(gid) = stored_games.get(&game_name) {
                let gid = gid.clone();
                // check that the ROMs are equal
                let roms_equal = 'are_roms_equal: {
                    let stored_roms = RedumpDatabase::get_redump_roms(&transaction, gid)?;
                    if stored_roms.len() != roms.len() {
                        break 'are_roms_equal false;
                    }
                    for rom in stored_roms {
                        if !roms.contains(&rom) {
                            break 'are_roms_equal false;
                        }
                    }
                    true
                };
                // if the ROMs are equal, we don't have to do anything
                // if it does, there are many things to do
                if roms_equal {
                    unchanged_entries += 1;
                } else {
                    changed_entries += 1;
                    RedumpDatabase::remove_game_roms(&transaction, gid)?;
                    RedumpDatabase::insert_new_roms(&transaction, gid, roms.iter())?;
                    RedumpDatabase::bump_game_revision(&transaction, gid)?;
                }
                // remove the name-id+rev mapping. this will be useful later
                stored_games.remove(&game_name);
            } else {
                new_entries += 1;
                RedumpDatabase::insert_new_roms(
                    &transaction,
                    RedumpDatabase::insert_new_game(&transaction, datafile.dfid, &game_name)?,
                    roms.iter(),
                )?;
            }
            // update sets/maps
            processed_games.insert(game_name.to_string());
        }
        // processed_games is no longer needed
        drop(processed_games);
        // in the loop, we were deleting entries from stored_games
        // by this point, only games which exist in the database but not in this datafile will remain
        // we must remove these
        let removed_games = stored_games.len();
        for (_, gid) in stored_games {
            RedumpDatabase::remove_game(&transaction, gid)?;
        }
        // update the version and last updated field within the database
        datafile.version = version.to_string();
        datafile.last_updated = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        RedumpDatabase::update_datafile(&transaction, &datafile)?;
        transaction
            .commit()
            .redump("Failed to commit changes to redump DB")?;
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
    pub fn update_console(&mut self, console: GameConsole) -> Result<()> {
        let datafile = RedumpDatabase::get_datafile_from_db(
            &self.connection,
            console.to_redump_slug().unwrap(),
        )?;
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        if current_time - datafile.last_updated >= self.min_update_delay {
            self.import_datafile(console, &self.download_datafile(console)?)
        } else {
            Ok(())
        }
    }
}
