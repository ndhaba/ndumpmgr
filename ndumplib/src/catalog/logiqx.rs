use roxmltree::{Document, Node, ParsingOptions};

use crate::{catalog::Result, utils::*};

#[allow(unused)]
pub(crate) struct Header<'a> {
    pub name: &'a str,
    pub description: &'a str,
    pub version: &'a str,
    pub homepage: &'a str,
}

pub(crate) trait Game
where
    Self: Sized,
{
    type ROM;

    fn add_rom(&mut self, rom: Self::ROM) -> Result<()>;
    fn parse_game(node: &Node) -> Result<Self>;
    fn parse_game_rom(node: &Node) -> Result<Self::ROM>;
}

pub(crate) struct Datafile<'a> {
    document: Document<'a>,
}

impl<'a> Datafile<'a> {
    pub fn open(content: &'a str) -> Result<Datafile<'a>> {
        Ok(Datafile {
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

    fn root(&'a self) -> Result<Node<'a, 'a>> {
        self.document
            .root()
            .get_tagged_child("datafile")
            .catalog("Failed to parse datafile\nMissing <datafile>")
    }

    pub fn parse_header(&'a self) -> Result<Header<'a>> {
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

    pub fn parse_games<T>(&self) -> Result<Vec<T>>
    where
        T: Game,
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
