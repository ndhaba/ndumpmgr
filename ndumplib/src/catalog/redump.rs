use std::{
    fs::File,
    io::{BufReader, BufWriter, Read},
};

use compress_tools::{Ownership, uncompress_archive};
use log::debug;
use tempfile::{NamedTempFile, TempDir, tempdir};

use crate::{Error, GameConsole, Result, ResultUtils};

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

pub(super) fn download_cuesheets(slug: &str) -> Result<TempDir> {
    let url: String = format!("http://redump.org/cues/{slug}/");
    let zip_file = NamedTempFile::with_suffix(".zip")
        .ndl("Failed to create temporary file to download cuesheets")?;
    let extracted_files = tempdir().ndl("Failed to create directory file to extract cue files")?;
    {
        let mut response = ureq::get(url).call().ndl("Failed to start download")?;
        let file = zip_file
            .as_file()
            .try_clone()
            .ndl("Failed to save download")?;
        let mut writer = BufWriter::new(file);
        std::io::copy(&mut response.body_mut().as_reader(), &mut writer)
            .ndl("Failed to save cue files")?;
        debug!(
            "Downloaded zipped cuesheets to \"{}\"",
            zip_file.path().to_str().unwrap()
        );
    }
    uncompress_archive(
        BufReader::new(zip_file),
        extracted_files.path(),
        Ownership::Ignore,
    )
    .ndl("Failed to extract zip")?;
    debug!(
        "Extracted zipped cuesheets to \"{}\"",
        extracted_files.path().to_str().unwrap()
    );
    Ok(extracted_files)
}

pub(super) fn download_datafile(slug: &str) -> Result<String> {
    let url: String = format!("http://redump.org/datfile/{slug}/");
    let zip_file = NamedTempFile::with_suffix(".zip")
        .ndl("Failed to create temporary file to download datafile")?;
    let extracted_files = tempdir().ndl("Failed to create directory file to extract datafile")?;
    {
        let mut response = ureq::get(url).call().ndl("Failed to start download")?;
        let file = zip_file
            .as_file()
            .try_clone()
            .ndl("Failed to save download")?;
        let mut writer = BufWriter::new(file);
        std::io::copy(&mut response.body_mut().as_reader(), &mut writer)
            .ndl("Failed to save datafile")?;
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
    .ndl("Failed to extract zip")?;
    debug!(
        "Extracted zipped datafile to \"{}\"",
        extracted_files.path().to_str().unwrap()
    );
    let mut file = 'file_find: {
        for file in extracted_files
            .path()
            .read_dir()
            .ndl("Failed to find downloaded datafile")?
        {
            let path = file.ndl("Failed to find downloaded datafile")?.path();
            if let Some(extension) = path.extension() {
                if extension == "dat" {
                    break 'file_find File::open(path).ndl("Failed to open datafile")?;
                }
            }
        }
        return Err(Error::new_original(
            "Failed to find downloaded datafile.\nNot included in the download",
        ));
    };
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .ndl("Failed to read datafile")?;
    Ok(contents)
}
