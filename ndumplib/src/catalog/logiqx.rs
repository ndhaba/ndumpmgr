use super::ResultUtils;
use roxmltree::{Document, Node, ParsingOptions};

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

#[allow(unused)]
pub(crate) struct Header<'a> {
    pub name: &'a str,
    pub description: &'a str,
    pub version: &'a str,
    pub homepage: &'a str,
}

pub(crate) trait GameElement
where
    Self: Sized,
{
    type ROM;

    fn add_rom(&mut self, rom: Self::ROM) -> super::Result<()>;
    fn parse_game(node: &Node) -> super::Result<Self>;
    fn parse_game_rom(node: &Node) -> super::Result<Self::ROM>;
}

pub(crate) struct XMLDatafile<'a> {
    document: Document<'a>,
}

impl<'a> XMLDatafile<'a> {
    pub fn open(content: &'a str) -> super::Result<XMLDatafile<'a>> {
        Ok(XMLDatafile {
            document: Document::parse_with_options(
                content,
                ParsingOptions {
                    allow_dtd: true,
                    nodes_limit: u32::MAX,
                },
            )
            .catalog("Failed to parse logiqx datafile")?,
        })
    }

    fn root(&'a self) -> super::Result<Node<'a, 'a>> {
        self.document
            .root()
            .get_tagged_child("datafile")
            .catalog("Failed to parse datafile\nMissing <datafile>")
    }

    pub fn parse_header(&'a self) -> super::Result<Header<'a>> {
        let header = self
            .root()?
            .get_tagged_child("header")
            .catalog("Failed to parse datafile\nMissing <header>")?;
        let name = header
            .get_tagged_child("name")
            .catalog("Failed to parse datafile\nMissing <name> in <header>")?
            .text()
            .unwrap_or("");
        let description = header
            .get_tagged_child("description")
            .catalog("Failed to parse datafile\nMissing <description> in <header>")?
            .text()
            .unwrap_or("");
        let version = header
            .get_tagged_child("version")
            .catalog("Failed to parse datafile\nMissing <version> in <header>")?
            .text()
            .unwrap_or("");
        let homepage = header
            .get_tagged_child("homepage")
            .catalog("Failed to parse datafile\nMissing <homepage> in <header>")?
            .text()
            .unwrap_or("");
        Ok(Header {
            name,
            description,
            version,
            homepage,
        })
    }

    pub fn parse_games<T>(&self) -> super::Result<Vec<T>>
    where
        T: GameElement,
    {
        let mut games = Vec::new();
        for game_element in self.root()?.get_tagged_children("game") {
            let mut game = T::parse_game(&game_element)?;
            for rom in game_element.get_tagged_children("rom") {
                game.add_rom(T::parse_game_rom(&rom)?)?;
            }
            games.push(game);
        }
        Ok(games)
    }
}
