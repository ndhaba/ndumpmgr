use crate::utils::XMLUtilsError;

pub mod nointro;
pub mod redump;

#[derive(Clone, Copy)]
pub enum GameConsole {
    Dreamcast,
    GBA,
    GameCube,
    N64,
    PSX,
    PS2,
    PS3,
    PSP,
    Wii,
    WiiU,
    Xbox,
    Xbox360,
}

impl GameConsole {
    pub fn to_formal_name(&self) -> &str {
        match self {
            Self::Dreamcast => "Dreamcast",
            Self::GBA => "Game Boy Advance",
            Self::GameCube => "GameCube",
            Self::N64 => "Nintendo 64",
            Self::PSX => "PlayStation",
            Self::PS2 => "PlayStation 2",
            Self::PS3 => "PlayStation 3",
            Self::PSP => "PlayStation Portable",
            Self::Wii => "Wii",
            Self::WiiU => "Wii U",
            Self::Xbox => "Xbox",
            Self::Xbox360 => "Xbox 360",
        }
    }
}

#[derive(Debug)]
pub(crate) enum InnerError {
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
    pub(crate) fn new<S: AsRef<str>, E: Into<InnerError>>(message: S, error: E) -> Error {
        Error(message.as_ref().to_string(), Some(error.into()))
    }
    /// Creates a new [Error] without a separate internal error
    ///
    pub(crate) fn new_original<S: AsRef<str>>(message: S) -> Error {
        Error(message.as_ref().to_string(), None)
    }
}

pub(crate) type Result<T> = std::result::Result<T, Error>;
