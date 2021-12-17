#![warn(clippy::all)]

use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version};
use relationaldb_rs::{error::Result, server::Server};

/// Service program entry
#[tokio::main]
async fn main() -> Result<()> {
    let _opts = app_from_crate!()
        .arg(
            clap::Arg::with_name("config")
                .short("c")
                .long("config")
                .help("Configuration file path")
                .takes_value(true)
                .default_value("/etc/relationaldb.yaml"),
        )
        .get_matches();

    Server::new().await?.listen().await?.serve().await
}
