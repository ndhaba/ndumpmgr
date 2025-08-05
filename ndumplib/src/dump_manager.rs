use std::{
    fs::File,
    path::{Path, PathBuf},
};

use sha1::{Digest, Sha1};
use tempfile::TempDir;

use self::{catalog::Catalog, cuesheets::Cuesheets};
use crate::{GameConsole, Result, ResultUtils};

mod catalog;
mod cuesheets;

pub struct ROMInfo {
    pub console: GameConsole,
    pub game_name: String,
    pub preferred_file_name: String,
}

#[derive(Clone, Copy)]
pub enum ROMStatus {
    Verified,
    Unverified,
    Broken,
}

pub struct DumpManager {
    catalog: Catalog,
    cuesheets: Cuesheets,
}

impl DumpManager {
    pub fn init(path: &impl AsRef<Path>) -> Result<DumpManager> {
        let base_folder_path = PathBuf::from(path.as_ref());
        Ok(DumpManager {
            catalog: Catalog::init(&base_folder_path.join("./catalog.sqlite"))?,
            cuesheets: Cuesheets::init(&base_folder_path.join("./cuesheets.sqlite"))?,
        })
    }

    pub fn can_convert(&self, path: &impl AsRef<Path>) -> bool {
        match path.as_ref().extension() {
            None => false,
            Some(extension) => {
                let extension = extension.to_str().unwrap();
                extension == "iso" || extension == "cue"
            }
        }
    }

    pub fn can_verify(&self, path: &impl AsRef<Path>) -> bool {
        match path.as_ref().extension() {
            None => false,
            Some(extension) => {
                let extension = extension.to_str().unwrap();
                extension == "iso" || extension == "cue" || extension == "chd"
            }
        }
    }

    fn convert_iso(&self, iso_path: &str, output_directory: &str, remove: bool) -> Result<String> {
        Ok("TODO".into())
    }

    fn convert_cue(&self, cue_path: &str, output_directory: &str, remove: bool) -> Result<String> {
        Ok("TODO".into())
    }

    pub fn convert_file(
        &self,
        path: &str,
        output_directory: &str,
        remove: bool,
    ) -> Result<Option<PathBuf>> {
        Ok(None)
    }

    pub fn get_rom_info(&self, path: &str) -> Result<Option<ROMInfo>> {
        Ok(None)
    }

    pub fn update(&mut self) -> Result<()> {
        self.catalog.update_all_consoles()?;
        self.cuesheets.update_all_consoles()
    }

    fn verify_standard_file(&self, path: &impl AsRef<Path>) -> Result<ROMStatus> {
        let mut file = File::open(path).ndl("Failed to verify file")?;
        let mut hasher = Sha1::new();
        let _bytes_written = std::io::copy(&mut file, &mut hasher).ndl("Failed to verify file")?;
        let hash = hasher.finalize();
        if self.catalog.is_rom(hash.into())? {
            Ok(ROMStatus::Verified)
        } else {
            Ok(ROMStatus::Unverified)
        }
    }

    fn verify_cue(&self, path: &impl AsRef<Path>) -> Result<ROMStatus> {
        let content = std::fs::read_to_string(path).ndl("Failed to verify cue")?;
        let path_buffer = path.as_ref().to_path_buf();
        for filename in self::cuesheets::get_track_filenames(&content) {
            if !path_buffer.with_file_name(filename).is_file() {
                return Ok(ROMStatus::Broken);
            }
        }
        match self.cuesheets.find_cue_hash(&content, path)? {
            None => Ok(ROMStatus::Unverified),
            Some(hash) => {
                if self.catalog.is_rom(hash)? {
                    Ok(ROMStatus::Verified)
                } else {
                    Ok(ROMStatus::Unverified)
                }
            }
        }
    }

    fn verify_chd(&self, path: &impl AsRef<Path>) -> Result<ROMStatus> {
        let _directory = TempDir::new().ndl("Failed to verify chd")?;
        Ok(ROMStatus::Broken)
    }

    pub fn verify_file(&self, path: &impl AsRef<Path>) -> Result<ROMStatus> {
        match path.as_ref().extension() {
            None => Ok(ROMStatus::Unverified),
            Some(extension) => {
                let extension = extension.to_str().unwrap();
                match extension {
                    "cue" => self.verify_cue(path),
                    "chd" => self.verify_chd(path),
                    "bin" | "iso" => self.verify_standard_file(path),
                    _ => Ok(ROMStatus::Unverified),
                }
            }
        }
    }
}
