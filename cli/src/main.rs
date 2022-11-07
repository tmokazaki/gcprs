mod bq;
mod gcs;

use anyhow;
use anyhow::Result;
use bq::{handle as handle_bq, BqArgs};
use clap::{Parser, Subcommand};
use gcs::{handle as handle_gcs, GcsArgs};

#[derive(Debug, Subcommand)]
enum SubCommand {
    /// Execute BigQuery APIs
    Bq(BqArgs),
    Gcs(GcsArgs),
}

#[derive(Debug, Parser)]
struct Arguments {
    #[clap(subcommand)]
    command: SubCommand,
}

#[tokio::main]
async fn main() -> Result<()> {
    let main_args = Arguments::parse();
    //println!("{:?}", args);
    match main_args.command {
        SubCommand::Bq(bqargs) => handle_bq(bqargs).await,
        SubCommand::Gcs(gcsargs) => handle_gcs(gcsargs).await,
    }
}
