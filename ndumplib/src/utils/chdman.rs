use std::process::Command;

use fancy_regex::Regex;

use super::{first_match, regex};
use crate::{Error, Result, ResultUtils};

#[derive(Clone, Copy, Debug)]
#[allow(unused)]
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
    fn from_string(str: &str) -> Self {
        match str {
            "zlib" => Self::ZLIB,
            "zstd" => Self::ZSTD,
            "lzma" => Self::LZMA,
            "huff" => Self::HUFF,
            "flac" => Self::FLAC,
            "cdzl" => Self::CDZL,
            "cdzs" => Self::CDZS,
            "cdlz" => Self::CDLZ,
            "cdfl" => Self::CDFL,
            "avhu" => Self::AVHU,
            _ => panic!(),
        }
    }
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
) -> Result<()> {
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
    let output = command.output().ndl("Failed to create CHD")?;
    let stderr = std::str::from_utf8(&output.stderr).unwrap();
    if stderr.contains("Compression complete") {
        Ok(())
    } else {
        match stderr.find("Error:") {
            Some(idx) => Err(Error::new_original(stderr[idx..].trim().to_string())),
            None => Err(Error::new_original("Unknown".to_string())),
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
) -> Result<()> {
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
    let output = command.output().ndl("Failed to extract CHD")?;
    let stderr = std::str::from_utf8(&output.stderr).unwrap();
    if stderr.contains("Extraction complete") {
        Ok(())
    } else {
        match stderr.find("Error:") {
            Some(idx) => Err(Error::new_original(stderr[idx..].trim().to_string())),
            None => Err(Error::new_original("Unknown".to_string())),
        }
    }
}

pub fn verify(input: &impl AsRef<str>) -> Result<bool> {
    let output = Command::new("chdman")
        .arg("verify")
        .arg("-i")
        .arg(input.as_ref())
        .output()
        .ndl("Failed to verify CHD")?;
    Ok(std::str::from_utf8(&output.stdout)
        .unwrap()
        .contains("verification successful"))
}

#[derive(Debug)]
pub enum TrackType {
    Mode1,
    Mode2Raw,
    Audio,
}

impl TrackType {
    fn from_str(str: &str) -> Option<TrackType> {
        match str {
            "MODE1" => Some(Self::Mode1),
            "MODE2_RAW" => Some(Self::Mode2Raw),
            "AUDIO" => Some(Self::Audio),
            _ => None,
        }
    }
}

#[allow(unused)]
#[derive(Debug)]
pub enum Tag {
    CHT2 { track: u8, track_type: TrackType },
    Other(String),
}

#[allow(unused)]
#[derive(Debug)]
pub struct InfoV5 {
    logical_size: usize,
    chd_size: usize,
    compression: Vec<Codec>,
    sha1: [u8; 20],
    data_sha1: [u8; 20],
    metadata: Vec<Tag>,
}

fn parse_usize(regex: &Regex, input: &str) -> Option<usize> {
    first_match(regex, input)
        .map(|v| usize::from_str_radix(&v.trim().replace(",", ""), 10).unwrap())
}

fn parse_sha1(regex: &Regex, input: &str) -> Option<[u8; 20]> {
    first_match(regex, input).map(|v| {
        let mut sha1 = [0u8; 20];
        hex::decode_to_slice(&v.trim(), &mut sha1).unwrap();
        sha1
    })
}

pub fn info(input: &impl AsRef<str>) -> Result<InfoV5> {
    let output = Command::new("chdman")
        .arg("info")
        .arg("-i")
        .arg(input.as_ref())
        .output()
        .ndl("Failed to get info on CHD")?;
    let content = std::str::from_utf8(&output.stdout).unwrap();
    let compression: Vec<Codec> = {
        let comp_str: String = first_match(regex!(r"(?<=Compression:)\s+\w[^\n]+"), content)
            .ndl("Failed to parse V5 CHD info")?;
        comp_str
            .trim()
            .split(", ")
            .map(|v| first_match(regex!(r"^\w+"), v).unwrap())
            .map(|v| Codec::from_string(&v))
            .collect()
    };
    let metadata: Vec<Tag> = {
        let total_metadata: String = first_match(regex!(r"(?<=Metadata:)[\s\S]+"), content)
            .ndl("Failed to parse V5 CHD info")?;
        let total_meta_lines: Vec<&str> = total_metadata
            .trim()
            .split("\n")
            .map(|v| v.trim())
            .collect();
        let mut metadata: Vec<Tag> = Vec::with_capacity(total_meta_lines.len() / 2);
        for i in (0..total_meta_lines.len()).step_by(2) {
            let line = total_meta_lines.get(i + 1).unwrap();
            if total_meta_lines.get(i).unwrap().contains("Tag='CHT2'") {
                metadata.push(Tag::CHT2 {
                    track: u8::from_str_radix(
                        &first_match(regex!(r"(?<=TRACK:)\d+"), line)
                            .ndl("Failed to parse V5 CHD info")?,
                        10,
                    )
                    .unwrap(),
                    track_type: TrackType::from_str(
                        &first_match(regex!(r"(?<= TYPE:)\w+"), line)
                            .ndl("Failed to parse V5 CHD info")?,
                    )
                    .unwrap(),
                })
            } else {
                metadata.push(Tag::Other(line.to_string()));
            }
        }
        metadata
    };
    Ok(InfoV5 {
        logical_size: parse_usize(regex!(r"(?<=Logical size:)\s+[\d,]+(?= bytes)"), content)
            .unwrap(),
        chd_size: parse_usize(regex!(r"(?<=CHD size:)\s+[\d,]+(?= bytes)"), content).unwrap(),
        sha1: parse_sha1(regex!(r"(?<=\nSHA1:)\s+[\da-fA-F]{40}"), content).unwrap(),
        data_sha1: parse_sha1(regex!(r"(?<=Data SHA1:)\s+[\da-fA-F]{40}"), content).unwrap(),
        compression,
        metadata,
    })
}
