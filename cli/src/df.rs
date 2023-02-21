use anyhow::Result;
use clap::{Args, Subcommand};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{
    CsvReadOptions, DataFrame, NdJsonReadOptions, ParquetReadOptions, SessionConfig, SessionContext,
};
use std::ffi::OsStr;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Args)]
pub struct DataFusionArgs {
    #[clap(subcommand)]
    pub datafusion_sub_command: DataFusionSubCommand,

    /// Input files
    #[clap(short = 'i', long = "inputs")]
    pub inputs: Vec<String>,

    /// Output file
    #[clap(short = 'o', long = "output", default_value = None)]
    pub output: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum DataFusionSubCommand {
    /// Query
    Query(QueryArgs),
}

#[derive(Default, Debug, Args)]
pub struct QueryArgs {
    #[clap(short = 'q', long = "query")]
    query: String,
}

#[derive(Error, Debug)]
pub enum DFError {
    #[error("file extension must be either `json`, `parquet`, `csv`")]
    UnsupportFileFormat,
}

pub async fn write_file(df: DataFrame, filename: String) -> Result<()> {
    if let Some(output_ex) = Path::new(&filename).extension().and_then(OsStr::to_str) {
        match output_ex {
            "json" => {
                df.write_json(&filename).await?;
            }
            "parquet" => {
                // TODO: set some options if necessary
                let builder = WriterProperties::builder();
                df.write_parquet(&filename, Some(builder.build())).await?;
            }
            "csv" => {
                df.write_csv(&filename).await?;
            }
            _ => anyhow::bail!(DFError::UnsupportFileFormat),
        };
        Ok(())
    } else {
        anyhow::bail!(DFError::UnsupportFileFormat)
    }
}

pub async fn handle(dfargs: DataFusionArgs) -> Result<()> {
    let cfg = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(cfg);
    for (i, input) in dfargs.inputs.iter().enumerate() {
        let table_id = format!("t{}", i);
        if let Some(input_ex) = Path::new(input).extension().and_then(OsStr::to_str) {
            match input_ex {
                "json" => {
                    ctx.register_json(&table_id, input, NdJsonReadOptions::default())
                        .await?
                }
                "parquet" => {
                    ctx.register_parquet(&table_id, input, ParquetReadOptions::default())
                        .await?
                }
                "csv" => {
                    ctx.register_csv(&table_id, input, CsvReadOptions::new())
                        .await?
                }
                _ => anyhow::bail!(DFError::UnsupportFileFormat),
            }
        } else {
            anyhow::bail!(DFError::UnsupportFileFormat)
        }
    }
    match dfargs.datafusion_sub_command {
        DataFusionSubCommand::Query(args) => {
            let df = ctx.sql(&args.query).await?;
            df.clone().show().await?;
            if let Some(output) = dfargs.output {
                write_file(df, output).await?;
            }
            Ok(())
        }
    }
}
