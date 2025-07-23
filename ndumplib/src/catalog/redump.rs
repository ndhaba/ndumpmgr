use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, BufWriter, Read},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use compress_tools::{Ownership, uncompress_archive};
use log::debug;
use roxmltree::Document;
use rusqlite::Connection;
use tempfile::{NamedTempFile, tempdir};

use super::GameConsole;

/**
 * Extensions for external structs/enums
 */
impl GameConsole {
    /// Attempts to find a slug to use for downloading a Redump datfile
    fn to_redump_slug(&self) -> Option<&str> {
        match self {
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
    SQLiteError(rusqlite::Error),
}

impl std::fmt::Display for InnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IOError(e) => write!(f, "I/O Error: {e}"),
            Self::NetError(e) => write!(f, "Network Error: {e}"),
            Self::ArchiveError(e) => write!(f, "Archive Error: {e}"),
            Self::XMLError(e) => write!(f, "XML Error: {e}"),
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
    fn map_redump_err<S: AsRef<str>>(self, message: S) -> Result<T>;
}
impl<T, E: Into<InnerError>> __ResultUtils<T> for std::result::Result<T, E> {
    fn map_redump_err<S: AsRef<str>>(self, message: S) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::new(message, e)),
        }
    }
}
impl<T> __ResultUtils<T> for std::option::Option<T> {
    fn map_redump_err<S: AsRef<str>>(self, message: S) -> Result<T> {
        match self {
            Some(v) => Ok(v),
            None => Err(Error::new_original(message)),
        }
    }
}

/**
 * Internal Types
 */
#[derive(PartialEq, Eq, Hash)]
struct RedumpRom {
    name: String,
    size: usize,
    crc: String,
    sha1: String,
}

/**
 * Redump Database
 */
pub struct RedumpDatabase {
    connection: Connection,
}

impl RedumpDatabase {
    /// Initializes a Redump database with the given file path.
    ///
    /// Panics if the given path is not valid UTF-8.
    ///
    pub fn init(path: &PathBuf) -> Result<RedumpDatabase> {
        // open the database connection
        let connection = Connection::open(path).map_redump_err("Failed to open Redump database")?;

        debug!(r#"Opened Redump database at "{}""#, path.to_str().unwrap());
        // get a list of the database's tables
        let tables = {
            let mut statement = connection
                .prepare("SELECT * FROM sqlite_master WHERE type = ?")
                .map_redump_err("Failed to retrieve created tables from Redump Database")?;
            let mut tables: HashSet<String> = HashSet::new();
            let mut rows = statement
                .query(("table",))
                .map_redump_err("Failed to retrieve created tables from Redump Database")?;
            while let Some(row) = rows
                .next()
                .map_redump_err("Failed to retrieve created tables from Redump Database")?
            {
                tables
                    .insert(row.get("tbl_name").map_redump_err(
                        "Failed to retrieve created tables from Redump Database",
                    )?);
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
                .map_redump_err("Failed to create tables in Redump Database")?;
            debug!("Created \"datfiles\" table");
        }
        if !tables.contains("games") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "games" (
                            "file_id"	INTEGER NOT NULL,
                            "game_id"	INTEGER NOT NULL UNIQUE,
                            "name"	TEXT NOT NULL UNIQUE,
                            "rom_revision"	INTEGER NOT NULL DEFAULT 0,
                            PRIMARY KEY("game_id")
                        )
                    "#,
                    (),
                )
                .map_redump_err("Failed to create tables in Redump Database")?;
            debug!("Created \"games\" table");
        }
        if !tables.contains("roms") {
            connection
                .execute(
                    r#"
                        CREATE TABLE "roms" (
                            "game_id"	INTEGER NOT NULL,
                            "name"	TEXT NOT NULL UNIQUE,
                            "size"	INTEGER NOT NULL,
                            "crc"	TEXT NOT NULL,
                            "sha1"	TEXT NOT NULL UNIQUE
                        )
                    "#,
                    (),
                )
                .map_redump_err("Failed to create tables in Redump Database")?;
            debug!("Created \"roms\" table");
        }
        // return the database
        Ok(RedumpDatabase { connection })
    }

    /// Downloads a Redump .DAT file for the given console.
    /// Returns the contents of said .DAT file
    ///
    /// Panics if the given console is not indexed by Redump.
    ///
    fn download_dat(&self, console: GameConsole) -> Result<String> {
        // get the DAT's url
        let url: String = format!(
            "http://redump.org/datfile/{}/",
            console
                .to_redump_slug()
                .expect("Attempted to download Redump DAT for non-Redump console")
        );
        // create temp zip file and directory
        let zip_file = NamedTempFile::with_suffix(".zip")
            .map_redump_err("Failed to create temporary file to download Redump DAT")?;
        let extracted_files =
            tempdir().map_redump_err("Failed to create directory file to extract Redump DAT")?;
        // download the DAT's zip archivve
        {
            // make the http request
            let mut response = ureq::get(url)
                .call()
                .map_redump_err("Failed to start download")?;
            // clone the file object, because that's something we have to do 😒
            let file = zip_file
                .as_file()
                .try_clone()
                .map_redump_err("Failed to save download")?;
            // write to the file
            let mut writer = BufWriter::new(file);
            std::io::copy(&mut response.body_mut().as_reader(), &mut writer)
                .map_redump_err("Failed to save Redump DAT")?;
            // done
            debug!(
                "Downloaded zipped Redump DAT to \"{}\"",
                zip_file.path().to_str().unwrap()
            );
        }
        // extract it
        uncompress_archive(
            BufReader::new(zip_file),
            extracted_files.path(),
            Ownership::Ignore,
        )
        .map_redump_err("Failed to extract zip")?;
        debug!(
            "Extracted zipped Redump DAT to \"{}\"",
            extracted_files.path().to_str().unwrap()
        );
        // locate the DAT
        let mut file = 'file_find: {
            // iterate over every file
            for file in extracted_files
                .path()
                .read_dir()
                .map_redump_err("Failed to find downloaded Redump DAT")?
            {
                let path = file
                    .map_redump_err("Failed to find downloaded Redump DAT")?
                    .path();
                // if its extension is .dat, we found it
                if let Some(extension) = path.extension() {
                    if extension == "dat" {
                        break 'file_find File::open(path)
                            .map_redump_err("Failed to open Redump DAT")?;
                    }
                }
            }
            // if we can't find the datfile, there's nothing we can do
            return Err(Error::new_original(
                "Failed to find downloaded Redump DAT.\nNot included in the download",
            ));
        };
        // read the datfile
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_redump_err("Failed to read Redump DAT")?;
        Ok(contents)
    }

    /// Imports a .DAT file
    ///
    /// This does not check if the provided DAT file contents match the console.
    /// A mismatch will almost certainly break the database.
    ///
    /// Panics if the given console is not indexed by Redump.
    ///
    fn import_dat(&mut self, console: GameConsole, contents: &String) -> Result<()> {
        // parse the xml-formatted DAT
        let document =
            Document::parse(contents.as_ref()).map_redump_err("Failed to parse Redump DAT")?;
        // find the root element
        let datafile = {
            let element = document
                .root()
                .first_element_child()
                .map_redump_err("Failed to parse Redump DAT\nMissing <datafile>")?;
            if !element.has_tag_name("datafile") {
                return Err(Error::new_original(
                    "Failed to parse Redump DAT\nMissing <datafile>",
                ));
            }
            element
        };
        // find the version
        let version = 'get_version: {
            // get the <header> element
            let header_element = datafile
                .children()
                .next()
                .map_redump_err("Failed to parse Redump DAT\nMissing <header>")?;
            if !header_element.has_tag_name("header") {
                return Err(Error::new_original(
                    "Failed to parse Redump DAT\nMissing <header>",
                ));
            }
            // painstakingly find the <version> element
            for element in header_element.children() {
                if element.has_tag_name("version") {
                    break 'get_version element.text().unwrap_or("");
                }
            }
            // we failed :(
            return Err(Error::new_original(
                "Failed to parse Redump DAT\nMissing <version>",
            ));
        };
        // get the in-database file ID
        let file_id: i64 = 'get_file_id: {
            // get the console slug
            let console_slug = console.to_redump_slug().unwrap();
            // retrieve the row corresponding to this console's DAT
            let mut statement = self
                .connection
                .prepare("SELECT file_id, version FROM datfiles WHERE console = ?")
                .map_redump_err("Failed to identify DAT")?;
            let mut result = statement
                .query((console_slug,))
                .map_redump_err("Failed to identify DAT")?;
            // if the row exists, we have the file ID
            if let Some(row) = result.next().map_redump_err("Failed to identify DAT")? {
                // GET IT GET THE THING
                let file_id = row.get("file_id").unwrap();
                debug!("Found file ID for {}: {}", console_slug, file_id);
                // return early if the version of the DAT in database is equal to the one we're processing
                let old_version: String = row.get("version").unwrap();
                if version != "" && version == old_version {
                    debug!(
                        "This version \"{}\" of the DAT is the current version stored",
                        version
                    );
                    return Ok(());
                }
                // done :D
                break 'get_file_id file_id;
            }
            // if it doesn't, we have to create a row and pull the file ID from that
            // first, create the row
            let mut statement = self
                .connection
                .prepare("INSERT INTO datfiles (console, version, last_updated) VALUES (?, ?, ?)")
                .map_redump_err("Failed to identify DAT")?;
            statement
                .execute((console_slug, "", 0))
                .map_redump_err("Failed to identify DAT")?;
            // then pull the file ID from it
            let mut statement = self
                .connection
                .prepare("SELECT file_id FROM datfiles WHERE console = ?")
                .map_redump_err("Failed to identify DAT")?;
            let file_id = statement
                .query((console_slug,))
                .map_redump_err("Failed to identify DAT")?
                .next()
                .map_redump_err("Failed to identify DAT")?
                .unwrap()
                .get("file_id")
                .unwrap();
            debug!(
                "Added new console \"{}\" with file ID: {}",
                console_slug, file_id
            );
            file_id
        };
        // get all of the currently stored games
        let mut stored_games = {
            let mut map: HashMap<String, (i64, i64)> = HashMap::new();
            // prepare the SQL statement
            let mut statement = self
                .connection
                .prepare("SELECT game_id, name, rom_revision FROM games WHERE file_id = ?")
                .map_redump_err("Failed to query current DAT contents")?;
            // make the query
            let mut result = statement
                .query((file_id,))
                .map_redump_err("Failed to query current DAT contents")?;
            // iterate over every row and add them
            while let Some(row) = result
                .next()
                .map_redump_err("Failed to query current DAT contents")?
            {
                map.insert(
                    row.get("name").unwrap(),
                    (
                        row.get("game_id").unwrap(),
                        row.get("rom_revision").unwrap(),
                    ),
                );
            }
            // done :D
            debug!("Previously stored games: {}", map.len());
            map
        };
        // pre-prepare some statements that we'll use later
        let mut get_roms_stmt = self
            .connection
            .prepare("SELECT name, size, crc, sha1 FROM roms WHERE game_id = ?")
            .map_redump_err("Failed to retrieve game ROMs from Redump Database")?;
        let mut get_game_id_stmt = self
            .connection
            .prepare("SELECT game_id FROM games WHERE file_id = ? AND name = ?")
            .map_redump_err("Failed to retrieve games from Redump Database")?;
        let mut delete_game_stmt = self
            .connection
            .prepare("DELETE FROM games WHERE game_id = ?")
            .map_redump_err("Failed to update games in Redump Database")?;
        let mut delete_roms_stmt = self
            .connection
            .prepare("DELETE FROM roms WHERE game_id = ?")
            .map_redump_err("Failed to update game ROMs in Redump Database")?;
        let mut insert_game_stmt = self
            .connection
            .prepare("INSERT INTO games (file_id, name) VALUES (?, ?)")
            .map_redump_err("Failed to update games in Redump Database")?;
        let mut insert_rom_stmt = self
            .connection
            .prepare("INSERT INTO roms (game_id, name, size, crc, sha1) VALUES (?, ?, ?, ?, ?)")
            .map_redump_err("Failed to update game ROMs in Redump Database")?;
        let mut update_rom_revision_stmt = self
            .connection
            .prepare("UPDATE games SET rom_revision = ? WHERE game_id = ?")
            .map_redump_err("Failed to update game ROMs in Redump Database")?;
        // iterate over every game
        let mut games = datafile.children();
        let mut unchanged_entries: usize = 0;
        let mut changed_entries: usize = 0;
        let mut new_entries: usize = 0;
        let mut processed_games: HashSet<String> = HashSet::new();
        while let Some(game) = games.next() {
            // make sure the tag is <game>
            if !game.has_tag_name("game") {
                continue;
            }
            // get the game's name
            let game_name = game
                .attribute("name")
                .map_redump_err("Failed to parse Redump DAT\n<game> missing \"name\" attribute")?;
            // make sure this hasn't been processed already
            if processed_games.contains(game_name) {
                return Err(Error::new_original(format!(
                    "Failed to parse Redump DAT\nDuplicate games were found: \"{}\"",
                    game_name
                )));
            }
            // get the game's ROMs
            let mut roms = HashSet::new();
            for rom_element in game.children() {
                // name
                let rom_name = rom_element.attribute("name").map_redump_err(format!(
                    "Failed to parse Redump DAT\n<rom> missing \"name\" attribute (game: \"{}\")",
                    game_name
                ))?;
                // size
                let size = {
                    let str = rom_element.attribute("size").map_redump_err(format!(
                        "Failed to parse Redump DAT\n<rom> missing \"size\" attribute (game: \"{}\")",
                        game_name
                    ))?;
                    usize::from_str_radix(str, 10).map_err(|_| {
                        Error::new_original(
                            format!("Failed to parse Redump DAT\n<rom> \"size\" attribute is invalid: \"{}\" (game: \"{}\")", str, game_name),
                        )
                    })?
                };
                // crc32
                let crc = rom_element.attribute("crc").map_redump_err(format!(
                    "Failed to parse Redump DAT\n<rom> missing \"crc\" attribute (game: \"{}\")",
                    game_name
                ))?;
                // sha1
                let sha1 = rom_element.attribute("sha1").map_redump_err(format!(
                    "Failed to parse Redump DAT\n<rom> missing \"sha1\" attribute (game: \"{}\")",
                    game_name
                ))?;
                // add the ROM
                roms.insert(RedumpRom {
                    name: rom_name.to_string(),
                    size,
                    crc: crc.to_string(),
                    sha1: sha1.to_string(),
                });
            }
            // has this game already been added?
            if let Some((game_id, rom_revision)) = stored_games.get(game_name) {
                // get a list of all of the ROMs stored in the database
                let stored_roms = {
                    let mut roms = Vec::new();
                    // get the rows of ROMs
                    let mut rows = get_roms_stmt
                        .query((game_id,))
                        .map_redump_err("Failed to retrieve game ROMs from Redump Database")?;
                    // iterate over every row and add them to the set
                    while let Some(row) = rows
                        .next()
                        .map_redump_err("Failed to retrieve game ROMs from Redump Database")?
                    {
                        roms.push(RedumpRom {
                            name: row.get("name").unwrap(),
                            size: row.get("size").unwrap(),
                            crc: row.get("crc").unwrap(),
                            sha1: row.get("sha1").unwrap(),
                        });
                    }
                    // done :D
                    roms
                };
                // check that the ROMs are equal
                let roms_equal = 'are_roms_equal: {
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
                // are the ROMs the exact same?
                if roms_equal {
                    unchanged_entries += 1;
                }
                // was some change made?
                else {
                    changed_entries += 1;
                    // delete the old ROMs
                    delete_roms_stmt
                        .execute((game_id,))
                        .map_redump_err("Failed to update game ROMs in Redump Database")?;
                    // add the new ROMs
                    for rom in roms {
                        insert_rom_stmt
                            .execute((game_id, rom.name, rom.size, rom.crc, rom.sha1))
                            .map_redump_err("Failed to update game ROMs in Redump Database")?;
                    }
                    // update the game's revision
                    update_rom_revision_stmt
                        .execute((rom_revision + 1, game_id))
                        .map_redump_err("Failed to update game ROMs in Redump Database")?;
                }
                // remove the name-id+rev mapping. this will be useful later
                stored_games.remove(game_name);
            } else {
                new_entries += 1;
                // add the game
                insert_game_stmt
                    .execute((file_id, game_name))
                    .map_redump_err("Failed to update games in Redump Database")?;
                // get the game ID
                let game_id: i64 = get_game_id_stmt
                    .query_one((file_id, game_name), |row| Ok(row.get("game_id").unwrap()))
                    .map_redump_err("Failed to retrieve games from Redump Database")?;
                // add the ROMs
                for rom in roms {
                    insert_rom_stmt
                        .execute((game_id, rom.name, rom.size, rom.crc, rom.sha1))
                        .map_redump_err("Failed to update game ROMs in Redump Database")?;
                }
            }
            // update sets/maps
            processed_games.insert(game_name.to_string());
        }
        // processed_games is no longer needed
        drop(processed_games);
        // in the loop, we were deleting entries from stored_games
        // by this point, only games which exist in the database but not in this datfile will remain
        // we must remove these
        let removed_games = stored_games.len();
        for (_, (game_id, _)) in stored_games {
            delete_game_stmt
                .execute((game_id,))
                .map_redump_err("Failed to update games in Redump Database")?;
            delete_roms_stmt
                .execute((game_id,))
                .map_redump_err("Failed to update games in Redump Database")?;
        }
        // update the version and last updated field within the database
        let mut update_datfile_stmt = self
            .connection
            .prepare("UPDATE datfiles SET version = ?, last_updated = ? WHERE file_id = ?")
            .map_redump_err("Failed to update DATs in Redump Database")?;
        update_datfile_stmt
            .execute((
                version,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                file_id,
            ))
            .map_redump_err("Failed to update DATs in Redump Database")?;
        // post our stats :D
        debug!(
            r#"Changed entries: {}\nUnchanged entries: {}\nAdded entries: {}\nRemoved entries: {}"#,
            changed_entries, unchanged_entries, new_entries, removed_games
        );
        // we're done... finally
        Ok(())
    }
}
