use std::{
    collections::HashSet,
    fs::File,
    io::{BufReader, BufWriter, Read},
    path::PathBuf,
};

use compress_tools::{Ownership, uncompress_archive};
use log::debug;
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
    fn new<S: AsRef<str>, E: Into<InnerError>>(message: S, error: E) -> Error {
        Error(message.as_ref().to_string(), Some(error.into()))
    }
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
                            "name"	TEXT NOT NULL,
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
                            "name"	TEXT NOT NULL,
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
}
