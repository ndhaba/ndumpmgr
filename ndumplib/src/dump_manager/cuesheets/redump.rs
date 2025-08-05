use std::io::{BufReader, BufWriter};

use compress_tools::{Ownership, uncompress_archive};
use log::debug;
use tempfile::{NamedTempFile, TempDir, tempdir};

use crate::{GameConsole, Result, ResultUtils};

impl GameConsole {
    pub(super) fn redump_cue_slug(&self) -> Option<&str> {
        match self {
            Self::PSX => Some("psx"),
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
