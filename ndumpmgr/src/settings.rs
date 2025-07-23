use serde::{Deserialize, Serialize};
use std::{env, fs, path::PathBuf};

use log::debug;

use crate::error_exit;

macro_rules! no_home_directory {
    () => {
        error_exit!("Could not find home directory.");
    };
}

pub struct StorageLocations {
    pub config_path: PathBuf,
    pub default_data_path: PathBuf,
}

impl Default for StorageLocations {
    fn default() -> Self {
        #[allow(deprecated)] // home_dir is deprecated
        match (env::consts::OS, env::home_dir()) {
            // OS is linux, and the home directory is defined
            ("linux", Some(home_dir)) => {
                // .config and .local/share
                let config_dir = home_dir.join(".config");
                let mut share_dir = home_dir.join(".local/share");
                // if .config and .local/share exist, store our files in these places
                if config_dir.is_dir() && share_dir.is_dir() {
                    share_dir.push("ndumpmgr");
                    // try to create data directory
                    if !share_dir.exists() {
                        match fs::create_dir(&share_dir) {
                            Ok(_) => {}
                            Err(_) => error_exit!(
                                "Failed to create data directory \"{}\". Please grant ndumpmgr the needed permissions",
                                share_dir.to_str().unwrap()
                            ),
                        }
                    // if there's something other than a directory there, ask the user to remove it
                    } else if !share_dir.is_dir() {
                        error_exit!(
                            "\"{}\" is not a directory. Please move whatever is there",
                            share_dir.to_str().unwrap()
                        );
                    }
                    // return the storage locations
                    let config_path = config_dir.join("ndumpmgr.yml");
                    debug!("Config path: {}", config_path.to_str().unwrap());
                    debug!("Default data path: {}", share_dir.to_str().unwrap());
                    return StorageLocations {
                        config_path,
                        default_data_path: share_dir,
                    };
                // otherwise, store them together in a .ndumpmgr folder in home
                } else {
                    let base_dir = home_dir.join(".ndumpmgr");
                    let config_path = base_dir.join("config.yml");
                    let default_data_path = base_dir.join("data");
                    // try to create ndumpmgr directory
                    if !base_dir.exists() {
                        match fs::create_dir(&base_dir) {
                            Ok(_) => {}
                            Err(_) => error_exit!(
                                "Failed to create directory \"{}\". Please grant ndumpmgr the needed permissions",
                                base_dir.to_str().unwrap()
                            ),
                        }
                    // if there's something other than a directory there, ask the user to remove it
                    } else if !base_dir.is_dir() {
                        error_exit!(
                            "\"{}\" is not a directory. Please move whatever is there",
                            base_dir.to_str().unwrap()
                        );
                    }
                    // try to create data directory
                    if !default_data_path.exists() {
                        match fs::create_dir(&default_data_path) {
                            Ok(_) => {}
                            Err(_) => error_exit!(
                                "Failed to create data directory \"{}\". Please grant ndumpmgr the needed permissions",
                                default_data_path.to_str().unwrap()
                            ),
                        }
                    // if there's something other than a directory there, ask the user to remove it
                    } else if !default_data_path.is_dir() {
                        error_exit!(
                            "\"{}\" is not a directory. Please move whatever is there",
                            default_data_path.to_str().unwrap()
                        );
                    }
                    // return the storage locations
                    debug!("Config path: {}", config_path.to_str().unwrap());
                    debug!("Default data path: {}", default_data_path.to_str().unwrap());
                    return StorageLocations {
                        config_path,
                        default_data_path,
                    };
                }
            }
            // OS is linux, but there's no home directory
            ("linux", None) => {
                no_home_directory!();
            }
            // any other OS
            _ => error_exit!("Unsupported OS: {}", env::consts::OS),
        };
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Settings {
    game_location: PathBuf,
}

impl Default for Settings {
    fn default() -> Self {
        #[allow(deprecated)] // home_dir is deprecated
        // get the default game location
        let game_location = match env::home_dir() {
            Some(mut home_dir) => {
                home_dir.push("games");
                home_dir
            }
            None => {
                no_home_directory!();
            }
        };
        // return defaults
        return Settings { game_location };
    }
}

impl Settings {
    /// Loads a config file from the given storage location
    pub fn load(locations: &StorageLocations) -> Settings {
        // if the config file doesn't exist, return the default
        if !locations.config_path.exists() {
            debug!("Config file not found. Using defaults...");
            Default::default()
        // if the config file isn't a file, tell the user to fix it
        } else if !locations.config_path.is_file() {
            error_exit!(
                "Configuration file \"{}\" is not a file. Please move whatever is there",
                locations.config_path.to_str().unwrap()
            );
        // if the config file does exist, read it
        } else {
            debug!("Using config file");
            let file_contents = match fs::read_to_string(&locations.config_path) {
                Ok(content) => content,
                Err(err) => error_exit!("Failed to read configuration file: {}", err),
            };
            match serde_yaml::from_str(file_contents.as_str()) {
                Ok(settings) => settings,
                Err(_) => error_exit!("Malformed configuration."),
            }
        }
    }
    /// Saves a config file to the given storage location
    #[allow(unused)]
    pub fn save(&self, locations: &StorageLocations) {
        match fs::write(&locations.config_path, serde_yaml::to_string(self).unwrap()) {
            Ok(()) => debug!("Saved config file"),
            Err(err) => error_exit!("Failed to write configuration file: {}", err),
        }
    }
}
