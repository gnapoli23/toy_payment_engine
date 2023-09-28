use clap::Parser;
use log::{info, debug};

// Using a struct here for maintanaibility reasons, so that if the application/engine needs
// to handle other future command-line arguments, they can be easily added.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    // Input CSV file path
    #[arg(index = 1)]
    pub file_path: String,
}

pub fn run() {
    // Init
    env_logger::init();
    info!("Payment engine is starting...");
    let args = Args::parse();
    debug!("Input args: {args:?}");
    // 1) Init engine

    // 2) Read CSV file containing transactions

    // 3) Process transactions data

    // 4) Output info on accounts
    println!("All transactions have been processed");
}