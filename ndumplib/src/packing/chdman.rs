use std::{process::Command, str::Utf8Error};

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    UTF8(Utf8Error),
    CHDMAN(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IO(err) => write!(f, "{}", err),
            Self::UTF8(err) => write!(f, "{}", err),
            Self::CHDMAN(err) => write!(f, "{}", err),
        }
    }
}
impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IO(value)
    }
}
impl From<Utf8Error> for Error {
    fn from(value: Utf8Error) -> Self {
        Self::UTF8(value)
    }
}

#[derive(Clone, Copy)]
pub enum Codec {
    ZLIB,
    ZSTD,
    LZMA,
    HUFF,
    FLAC,
    CDZL,
    CDZS,
    CDLZ,
    CDFL,
    AVHU,
}

impl Codec {
    fn to_string(self) -> &'static str {
        match self {
            Self::ZLIB => "zlib",
            Self::ZSTD => "zstd",
            Self::LZMA => "lzma",
            Self::HUFF => "huff",
            Self::FLAC => "flac",
            Self::CDZL => "cdzl",
            Self::CDZS => "cdzs",
            Self::CDLZ => "cdlz",
            Self::CDFL => "cdfl",
            Self::AVHU => "avhu",
        }
    }
}

pub struct CreateOptions {
    pub compression: Option<Box<[Codec]>>,
    pub force: bool,
    pub hunk_size: Option<usize>,
    pub processor_count: Option<usize>,
}

pub fn create_cd(
    input: &impl AsRef<str>,
    output: &impl AsRef<str>,
    options: CreateOptions,
) -> Result<(), Error> {
    let mut command = Command::new("chdman");
    command
        .arg("createcd")
        .arg("-i")
        .arg(input.as_ref())
        .arg("-o")
        .arg(output.as_ref());
    if let Some(compression) = options.compression {
        command.arg("-c").arg(
            compression
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<&str>>()
                .join(","),
        );
    }
    if options.force {
        command.arg("-f");
    }
    if let Some(hunk_size) = options.hunk_size {
        command.arg("-hs").arg(hunk_size.to_string());
    }
    if let Some(processor_count) = options.processor_count {
        command.arg("-np").arg(processor_count.to_string());
    }
    let output = match command.output() {
        Ok(output) => output,
        Err(error) => return Err(error.into()),
    };
    let stderr = match std::str::from_utf8(&output.stderr) {
        Ok(str) => str,
        Err(error) => return Err(error.into()),
    };
    if stderr.contains("Compression complete") {
        Ok(())
    } else {
        match stderr.find("Error:") {
            Some(idx) => Err(Error::CHDMAN(stderr[idx..].trim().to_string())),
            None => Err(Error::CHDMAN("Unknown".to_string())),
        }
    }
}

pub struct ExtractOptions {
    force: bool,
    split_tracks: bool,
}

pub fn extract_cd(
    input: &impl AsRef<str>,
    output: &impl AsRef<str>,
    options: ExtractOptions,
) -> Result<(), Error> {
    let mut command = Command::new("chdman");
    command
        .arg("extractcd")
        .arg("-i")
        .arg(input.as_ref())
        .arg("-o")
        .arg(output.as_ref());
    if options.force {
        command.arg("-f");
    }
    if options.split_tracks {
        command.arg("-sb");
    }
    let output = match command.output() {
        Ok(output) => output,
        Err(error) => return Err(error.into()),
    };
    let stderr = match std::str::from_utf8(&output.stderr) {
        Ok(str) => str,
        Err(error) => return Err(error.into()),
    };
    if stderr.contains("Extraction complete") {
        Ok(())
    } else {
        match stderr.find("Error:") {
            Some(idx) => Err(Error::CHDMAN(stderr[idx..].trim().to_string())),
            None => Err(Error::CHDMAN("Unknown".to_string())),
        }
    }
}

pub fn verify(input: &impl AsRef<str>) -> Result<bool, Error> {
    let output = match Command::new("chdman")
        .arg("verify")
        .arg("-i")
        .arg(input.as_ref())
        .output()
    {
        Ok(output) => output,
        Err(error) => return Err(error.into()),
    };
    match std::str::from_utf8(&output.stdout) {
        Ok(str) => Ok(str.contains("verification successful")),
        Err(error) => return Err(error.into()),
    }
}
