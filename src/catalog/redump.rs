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

use crate::{catalog::GameConsole, error_exit, settings::StorageLocations};

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
pub enum InnerError {
    IOError(std::io::Error),
    NetError(ureq::Error),
    ArchiveError(compress_tools::Error),
    XMLError(roxmltree::Error),
}

impl std::fmt::Display for InnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IOError(e) => write!(f, "I/O Error: {e}"),
            Self::NetError(e) => write!(f, "Network Error: {e}"),
            Self::ArchiveError(e) => write!(f, "Archive Error: {e}"),
            Self::XMLError(e) => write!(f, "XML Error: {e}"),
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
    fn convert<S: AsRef<str>, E: Into<InnerError>>(message: S) -> impl FnOnce(E) -> Error {
        let message = message.as_ref().to_string();
        |err| Error(message, Some(err.into()))
    }
    fn new<S: AsRef<str>, E: Into<InnerError>>(message: S, error: E) -> Error {
        Error(message.as_ref().to_string(), Some(error.into()))
    }
    fn new_original<S: AsRef<str>>(message: S) -> Error {
        Error(message.as_ref().to_string(), None)
    }
}

type Result<T> = std::result::Result<T, Error>;

/**
 * Other Structs
 */
struct XMLDocument<'a> {
    content: String,
    document: roxmltree::Document<'a>,
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
    /// If an error occurs, it will be logged and the program will exit.
    pub fn init(path: &PathBuf) -> RedumpDatabase {
        // open the database connection
        let connection = Connection::open(path)
            .unwrap_or_else(|err| error_exit!("Failed to open Redump database\n{err}"));

        debug!(r#"Opened Redump database at "{}""#, path.to_str().unwrap());
        // get a list of the database's tables
        let tables = {
            fn failed_to_retrive<T>(err: rusqlite::Error) -> T {
                error_exit!("Failed to retrieve created tables from Redump Database\n{err}");
            }
            let mut statement = connection
                .prepare("SELECT * FROM sqlite_master WHERE type = ?")
                .unwrap_or_else(failed_to_retrive);
            let mut tables: HashSet<String> = HashSet::new();
            let mut rows = statement
                .query(("table",))
                .unwrap_or_else(failed_to_retrive);
            while let Some(row) = rows.next().unwrap_or_else(failed_to_retrive) {
                tables.insert(row.get("tbl_name").unwrap_or_else(failed_to_retrive));
            }
            tables
        };
        // create missing tables
        fn failed_to_create<T>(err: rusqlite::Error) -> T {
            error_exit!("Failed to create tables in Redump Database\n{err}")
        }
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
                .unwrap_or_else(failed_to_create);
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
                .unwrap_or_else(failed_to_create);
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
                .unwrap_or_else(failed_to_create);
            debug!("Created \"roms\" table");
        }
        // return the database
        RedumpDatabase { connection }
    }

    /// Initializes a Redump database at the default file location relative to a storage location
    ///
    /// If an error occurs, it will be logged and the program will exit.
    pub fn init_default(locations: &StorageLocations) -> RedumpDatabase {
        let database_path = locations.default_data_path.join("redump.sqlite3");
        Self::init(&database_path)
    }

    fn download_datfile(&mut self, console: GameConsole) -> Result<String> {
        // get the datfile's url
        let url: String = format!(
            "http://redump.org/datfile/{}/",
            console
                .to_redump_slug()
                .expect("Attempted to download Redump datfile for non-Redump console")
        );
        // create temp zip file and directory
        let zip_file = NamedTempFile::with_suffix(".zip").map_err(Error::convert(
            "Failed to create temporary file to download datfile",
        ))?;
        let extracted_files = tempdir().map_err(Error::convert(
            "Failed to create directory file to extract datfile",
        ))?;
        // download the datfile's zip archivve
        {
            // make the http request
            let mut response = ureq::get(url)
                .call()
                .map_err(Error::convert("Failed to start download"))?;
            // clone the file object, because that's something we have to do 😒
            let file = zip_file
                .as_file()
                .try_clone()
                .map_err(Error::convert("Failed to save download"))?;
            // write to the file
            let mut writer = BufWriter::new(file);
            if let Err(err) = std::io::copy(&mut response.body_mut().as_reader(), &mut writer) {
                return Err(Error::new("Failed to save redump datfile", err));
            }
            // done
            debug!(
                "Downloaded zipped Redump datfile to \"{}\"",
                zip_file.path().to_str().unwrap()
            );
        }
        // extract it
        if let Err(err) = uncompress_archive(
            BufReader::new(zip_file),
            extracted_files.path(),
            Ownership::Ignore,
        ) {
            return Err(Error::new("Failed to extract zip", err));
        }
        debug!(
            "Extracted zipped Redump datfile to \"{}\"",
            extracted_files.path().to_str().unwrap()
        );
        // locate the datfile
        let mut file = 'file_find: {
            fn failed_to_find(err: std::io::Error) -> Error {
                Error::new("Failed to find downloaded datfile", err)
            }
            // iterate over every file
            for file in extracted_files.path().read_dir().map_err(failed_to_find)? {
                let path = file.map_err(failed_to_find)?.path();
                // if its extension is .dat, we found it
                if let Some(extension) = path.extension() {
                    if extension == "dat" {
                        break 'file_find File::open(path)
                            .map_err(|err| Error::new("Failed to open datfile", err))?;
                    }
                }
            }
            // if we can't find the datfile, there's nothing we can do
            return Err(Error::new_original(
                "Failed to find downloaded datfile. It wasn't included",
            ));
        };
        // read the datfile
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(Error::convert("Failed to read datfile"))?;
        Ok(contents)
    }
}
