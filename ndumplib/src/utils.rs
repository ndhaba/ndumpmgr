use roxmltree::Node;
use rusqlite::{CachedStatement, Connection, Transaction};
use ureq::{Body, http::Response};

#[derive(Debug)]
pub(crate) struct XMLUtilsError(String);

impl std::fmt::Display for XMLUtilsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for XMLUtilsError {}

pub(crate) trait XMLQueries {
    fn get_tagged_child(&self, tag_name: &str) -> Option<Self>
    where
        Self: Sized;

    fn get_tagged_children(&self, tag_name: &str) -> impl Iterator<Item = Self>
    where
        Self: Sized;
}

pub(crate) trait XMLPlainAttribute<T> {
    fn attr(&self, name: &str) -> Result<T, XMLUtilsError>;
}

pub(crate) trait XMLHexAttribute<T> {
    fn attr_hex(&self, name: &str) -> Result<T, XMLUtilsError>;
}

impl<'a, 'input> XMLQueries for Node<'a, 'input> {
    fn get_tagged_child(&self, tag_name: &str) -> Option<Self>
    where
        Self: Sized,
    {
        for element in self.children() {
            if element.has_tag_name(tag_name) {
                return Some(element);
            }
        }
        None
    }

    fn get_tagged_children(&self, tag_name: &str) -> impl Iterator<Item = Self>
    where
        Self: Sized,
    {
        let tag_name = tag_name.to_string();
        self.children()
            .filter(move |element| element.has_tag_name(tag_name.as_str()))
    }
}

impl<'a, 'input> XMLPlainAttribute<&'a str> for Node<'a, 'input> {
    fn attr(&self, name: &str) -> Result<&'a str, XMLUtilsError> {
        match self.attribute(name) {
            None => Err(XMLUtilsError(format!(
                "<{}> element missing attribute \"{}\"",
                self.tag_name().name(),
                name
            ))),
            Some(value) => Ok(value),
        }
    }
}

impl<'a, 'input> XMLPlainAttribute<usize> for Node<'a, 'input> {
    fn attr(&self, name: &str) -> Result<usize, XMLUtilsError> {
        let value: &'a str = self.attr(name)?;
        match usize::from_str_radix(value, 10) {
            Ok(value) => Ok(value),
            Err(_) => Err(XMLUtilsError(format!(
                "<{}> element has invalid \"{}\" attribute: \"{}\" (expected a usize)",
                self.tag_name().name(),
                name,
                value
            ))),
        }
    }
}

impl<'a, 'input> XMLHexAttribute<i32> for Node<'a, 'input> {
    fn attr_hex(&self, name: &str) -> Result<i32, XMLUtilsError> {
        let buffer: [u8; 4] = self.attr_hex(name)?;
        Ok(i32::from_be_bytes(buffer))
    }
}

impl<'a, 'input, const N: usize> XMLHexAttribute<[u8; N]> for Node<'a, 'input> {
    fn attr_hex(&self, name: &str) -> Result<[u8; N], XMLUtilsError> {
        let value: &'a str = self.attr(name)?;
        let mut slice: [u8; N] = [0; N];
        match hex::decode_to_slice(&value, &mut slice) {
            Ok(_) => Ok(slice),
            Err(_) => Err(XMLUtilsError(format!(
                "<{}> element has invalid \"{}\" attribute: \"{}\" (expected {}-bit hex)",
                self.tag_name().name(),
                name,
                value,
                N * 8
            ))),
        }
    }
}

pub(crate) trait CanPrepare {
    fn prepare_cached_common(&self, sql: &str) -> rusqlite::Result<CachedStatement>;
}

impl CanPrepare for Connection {
    #[inline(always)]
    fn prepare_cached_common(&self, sql: &str) -> rusqlite::Result<CachedStatement> {
        self.prepare_cached(sql)
    }
}

impl<'a> CanPrepare for Transaction<'a> {
    #[inline(always)]
    fn prepare_cached_common(&self, sql: &str) -> rusqlite::Result<CachedStatement> {
        self.prepare_cached(sql)
    }
}

#[doc(hidden)]
pub(crate) trait ResultUtils<T> {
    fn catalog<S: AsRef<str>>(self, message: S) -> crate::catalog::Result<T>;
}
impl<T, E: Into<crate::catalog::InnerError>> ResultUtils<T> for std::result::Result<T, E> {
    fn catalog<S: AsRef<str>>(self, message: S) -> crate::catalog::Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(crate::catalog::Error::new(message, e)),
        }
    }
}
impl<T> ResultUtils<T> for std::option::Option<T> {
    fn catalog<S: AsRef<str>>(self, message: S) -> crate::catalog::Result<T> {
        match self {
            Some(v) => Ok(v),
            None => Err(crate::catalog::Error::new_original(message)),
        }
    }
}

pub(crate) trait ResponseUtils {
    fn content_type(&self) -> String;
    fn content_length(&self) -> usize;
}

impl ResponseUtils for Response<Body> {
    fn content_type(&self) -> String {
        self.headers()
            .get("Content-Type")
            .unwrap()
            .to_str()
            .unwrap()
            .split(";")
            .next()
            .unwrap()
            .to_string()
    }
    fn content_length(&self) -> usize {
        self.headers()
            .get("Content-Length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap()
    }
}
