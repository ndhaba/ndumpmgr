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
