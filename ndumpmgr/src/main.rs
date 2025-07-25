use clap::{Parser, Subcommand};
use log::{LevelFilter, info};
use ndumplib::catalog::redump::RedumpDatabase;
use simplelog::{ConfigBuilder, TermLogger};

mod settings;

macro_rules! error_exit {
    ($($values:expr),*) => {{
        log::error!($($values),*);
        std::process::exit(0);
    }};
}
pub(crate) use error_exit;

use crate::settings::StorageLocations;

#[derive(Parser)]
#[command(
    version("0.1.0"),
    about,
    long_about(Some("A utility for organizing game dumps/ISOs"))
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    /// Enables verbose logging - detailed info useful for debugging ndumpmgr
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Imports a game dump or folder of game dumps
    Import {
        /// The path to the dump or folder of dumps
        /// (defaults to the user's download folder)
        path: Option<String>,
    },
    /// Sorts the currently stored game dumps by console
    Sort {},
}

/// Imports a game dump or folder of game dumps
fn import(_path: Option<String>, _settings: settings::Settings) {}

/// Sorts the currently stored game dumps by console
fn sort(_settings: settings::Settings, locations: &StorageLocations) {
    // setup databases
    let mut redump_database =
        RedumpDatabase::init(&locations.default_data_path.join("redump.sqlite"))
            .unwrap_or_else(|err| error_exit!("{}", err));
    // check for updates
    info!("Checking for game updates");
    redump_database
        .update()
        .unwrap_or_else(|err| error_exit!("{err}"));
}

fn main() {
    // parse cli arguments
    let cli = Cli::parse();
    // initialize logger
    let mut logger_config = ConfigBuilder::new();
    logger_config.set_time_level(LevelFilter::Off);
    TermLogger::init(
        if cli.verbose {
            simplelog::LevelFilter::Debug
        } else {
            simplelog::LevelFilter::Info
        },
        logger_config.build(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )
    .unwrap();
    // load settings
    let locations = settings::StorageLocations::default();
    let settings = settings::Settings::load(&locations);
    // run command
    match cli.command {
        Some(Command::Import { path }) => import(path, settings),
        Some(Command::Sort {}) => sort(settings, &locations),
        None => {}
    }
}
