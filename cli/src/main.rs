mod bq;
mod chart;
mod common;
mod df;
mod drive;
mod gcs;
mod ml;
mod text;

use anyhow::Result;
use bq::{handle as handle_bq, BqArgs};
use chart::{handle as handle_chart, ChartArgs};
use clap::{Parser, Subcommand};
use df::{handle as handle_datafusion, DataFusionArgs};
use drive::{handle as handle_drive, DriveArgs};
use gcs::{handle as handle_gcs, GcsArgs};
use ml::{handle as handle_ml, MlArgs};
use text::{handle as handle_text, TextArgs};

#[derive(Debug, Subcommand)]
enum SubCommand {
    /// Execute BigQuery APIs
    Bq(BqArgs),
    /// Execute GCS APIs
    Gcs(GcsArgs),
    /// Execute DataFusion
    DF(DataFusionArgs),
    /// Execute ML
    Ml(MlArgs),
    /// Execute Chart
    Chart(ChartArgs),
    /// Execute Drive APIs
    Drive(DriveArgs),
    /// Execute Text
    Text(TextArgs),
}

#[derive(Debug, Parser)]
struct Arguments {
    #[clap(subcommand)]
    command: SubCommand,
}

#[tokio::main]
async fn main() -> Result<()> {
    let main_args = Arguments::parse();
    //println!("{:?}", main_args);
    match main_args.command {
        SubCommand::Bq(bqargs) => handle_bq(bqargs).await,
        SubCommand::Gcs(gcsargs) => handle_gcs(gcsargs).await,
        SubCommand::DF(dfargs) => handle_datafusion(dfargs).await,
        SubCommand::Ml(mlargs) => handle_ml(mlargs).await,
        SubCommand::Chart(cargs) => handle_chart(cargs).await,
        SubCommand::Drive(dargs) => handle_drive(dargs).await,
        SubCommand::Text(targs) => handle_text(targs).await,
    }
}
