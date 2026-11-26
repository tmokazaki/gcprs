mod bq;
mod chart;
mod common;
mod df;
mod drive;
mod gcs;
mod ml;
#[cfg(feature = "text")]
mod text;
#[cfg(feature = "text")]
use text::{handle as handle_text, TextArgs};

use anyhow::Result;
use bq::{handle as handle_bq, BqArgs};
use chart::{handle as handle_chart, ChartArgs};
use clap::{Parser, Subcommand};
use df::{handle as handle_datafusion, DataFusionArgs};
use drive::{handle as handle_drive, DriveArgs};
use gcs::{handle as handle_gcs, GcsArgs};
use ml::{handle as handle_ml, MlArgs};

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
    #[cfg(feature = "text")]
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
        #[cfg(feature = "text")]
        SubCommand::Text(targs) => handle_text(targs).await,
    }
}

#[cfg(test)]
mod tests {
    /// The cli links both `ring` (via the Google API crates) and `aws-lc-rs` (via object_store),
    /// so rustls cannot pick a crypto provider by itself. Creating the gcprs clients must still
    /// work: they install `ring` as the process default before touching rustls.
    #[test]
    fn test_gcprs_clients_work_with_multiple_crypto_providers() {
        let _client = gcprs::auth::new_client();
        let _client = gcprs::metadata::new_client();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }
}
