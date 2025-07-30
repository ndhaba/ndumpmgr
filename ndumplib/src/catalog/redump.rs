use std::{
    fs::File,
    io::{BufReader, BufWriter, Read},
};

use compress_tools::{Ownership, uncompress_archive};
use log::debug;
use tempfile::{NamedTempFile, tempdir};

use super::{Error, Result};
use crate::{GameConsole, utils::*};

impl GameConsole {
    pub(super) fn redump_datafile_name(&self) -> Option<&str> {
        match self {
            Self::Dreamcast => Some("Sega - Dreamcast"),
            Self::GameCube => Some("Nintendo - GameCube"),
            Self::PSX => Some("Sony - PlayStation"),
            Self::PS2 => Some("Sony - PlayStation 2"),
            Self::PS3 => Some("Sony - PlayStation 3"),
            Self::PSP => Some("Sony - PlayStation Portable"),
            Self::Wii => Some("Nintendo - Wii"),
            Self::Xbox => Some("Microsoft - Xbox"),
            Self::Xbox360 => Some("Microsoft - Xbox 360"),
            _ => None,
        }
    }

    pub(super) fn redump_slug(&self) -> Option<&str> {
        match self {
            Self::Dreamcast => Some("dc"),
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

pub(super) fn download_datafile(slug: &str) -> Result<String> {
    let url: String = format!("http://redump.org/datfile/{slug}/");
    let zip_file = NamedTempFile::with_suffix(".zip")
        .catalog("Failed to create temporary file to download datafile")?;
    let extracted_files =
        tempdir().catalog("Failed to create directory file to extract datafile")?;
    {
        let mut response = ureq::get(url).call().catalog("Failed to start download")?;
        let file = zip_file
            .as_file()
            .try_clone()
            .catalog("Failed to save download")?;
        let mut writer = BufWriter::new(file);
        std::io::copy(&mut response.body_mut().as_reader(), &mut writer)
            .catalog("Failed to save datafile")?;
        debug!(
            "Downloaded zipped datafile to \"{}\"",
            zip_file.path().to_str().unwrap()
        );
    }
    uncompress_archive(
        BufReader::new(zip_file),
        extracted_files.path(),
        Ownership::Ignore,
    )
    .catalog("Failed to extract zip")?;
    debug!(
        "Extracted zipped datafile to \"{}\"",
        extracted_files.path().to_str().unwrap()
    );
    let mut file = 'file_find: {
        for file in extracted_files
            .path()
            .read_dir()
            .catalog("Failed to find downloaded datafile")?
        {
            let path = file.catalog("Failed to find downloaded datafile")?.path();
            if let Some(extension) = path.extension() {
                if extension == "dat" {
                    break 'file_find File::open(path).catalog("Failed to open datafile")?;
                }
            }
        }
        return Err(Error::new_original(
            "Failed to find downloaded datafile.\nNot included in the download",
        ));
    };
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .catalog("Failed to read datafile")?;
    Ok(contents)
}
