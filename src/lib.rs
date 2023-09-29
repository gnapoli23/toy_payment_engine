use std::{error::Error, path::Path};
use tokio::fs::File;
use tokio::io::{self, BufReader};

use clap::Parser;
use log::info;

mod engine;

// Using a struct here for maintanaibility reasons, so that if the application/engine needs
// to handle other future command-line arguments, they can be easily added.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    // Input CSV file path
    #[arg(index = 1, value_parser = parse_filepath)]
    pub file_path: String,
}

fn parse_filepath(file_path: &str) -> Result<String, String> {
    let path = Path::new(file_path);

    // Check that the path exists
    if !path.exists() {
        return Err(String::from("File path doesn't exist"));
    }

    // Check that the file is a CSV
    if let Some(ext) = path.extension() {
        if let Some(ext_str) = ext.to_str() {
            if ext_str.to_lowercase() == "csv" {
                Ok(file_path.into())
            } else {
                Err(String::from("File is not in CSV format"))
            }
        } else {
            Err(String::from("Unable to convert file path to string"))
        }
    } else {
        Err(String::from("File path hasn't any extension"))
    }
}

pub async fn run() -> Result<(), Box<dyn Error>> {
    // Init
    env_logger::init();
    info!("Payment engine started.");
    let args = Args::parse();

    // Read CSV file containing transactions
    info!("Reading data from CSV file.");
    let file = File::open(args.file_path).await?;
    let rdr = BufReader::new(file);

    // Process transactions data
    info!("Processing transactions data");
    let accounts = engine::process_transactions(rdr).await?;

    // Output info on accounts
    let mut wrt = csv_async::AsyncSerializer::from_writer(io::stdout());

    for (_, acc) in accounts {
        wrt.serialize(acc).await?;
    }

    wrt.flush().await?;
    Ok(())
}
