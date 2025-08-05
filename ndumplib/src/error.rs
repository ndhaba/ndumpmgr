#[derive(Debug)]
pub(crate) enum InnerError {
    IOError(std::io::Error),
    NetError(ureq::Error),
    ArchiveError(compress_tools::Error),
    XMLError(roxmltree::Error),
    SQLiteError(rusqlite::Error),
    UnknownError(visdom::types::BoxDynError),
}

impl std::fmt::Display for InnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IOError(e) => write!(f, "I/O Error: {e}"),
            Self::NetError(e) => write!(f, "Network Error: {e}"),
            Self::ArchiveError(e) => write!(f, "Archive Error: {e}"),
            Self::XMLError(e) => write!(f, "XML Error: {e}"),
            Self::SQLiteError(e) => write!(f, "SQLite Error: {e}"),
            Self::UnknownError(e) => write!(f, "{e}"),
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
impl From<visdom::types::BoxDynError> for InnerError {
    fn from(value: visdom::types::BoxDynError) -> Self {
        Self::UnknownError(value)
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

#[doc(hidden)]
pub(crate) trait ResultUtils<T> {
    fn ndl<S: AsRef<str>>(self, message: S) -> Result<T>;
}
impl<T, E: Into<InnerError>> ResultUtils<T> for std::result::Result<T, E> {
    fn ndl<S: AsRef<str>>(self, message: S) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::new(message, e)),
        }
    }
}
impl<T> ResultUtils<T> for std::option::Option<T> {
    fn ndl<S: AsRef<str>>(self, message: S) -> Result<T> {
        match self {
            Some(v) => Ok(v),
            None => Err(Error::new_original(message)),
        }
    }
}
