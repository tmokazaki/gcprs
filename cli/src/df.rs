mod func;

use anyhow::Result;
use clap::{Args, Subcommand};
use datafusion::arrow::csv::WriterBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion_common::config::{TableParquetOptions, JsonOptions, CsvOptions};
use datafusion::prelude::{
    CsvReadOptions, DataFrame, NdJsonReadOptions, ParquetReadOptions, SessionConfig, SessionContext,
};
use func::{udaf_string_agg, udf_pow};
use object_store::gcp::GoogleCloudStorageBuilder;
use std::ffi::OsStr;
use std::fs::remove_dir_all;
use std::io;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use url::Url;

#[derive(Debug, Args)]
pub struct DataFusionArgs {
    #[clap(subcommand)]
    pub datafusion_sub_command: DataFusionSubCommand,

    /// Input files.
    ///
    /// You can use glob format for a single table.
    /// Multiple tables are also supported. To use it, add `-i <filename>` arguments as you need.
    #[clap(short = 'i', long = "inputs")]
    pub inputs: Vec<String>,

    /// Output raw JSON
    #[clap(short = 'j', long = "json", default_value = "false")]
    pub json: bool,

    /// Output file. Optional.
    ///
    /// The result is always shown in stdout. This option write the result to the file.
    #[clap(short = 'o', long = "output", default_value = None)]
    pub output: Option<String>,

    /// If Output argument file exists, force to remove.
    #[clap(short = 'r', long = "remove", default_value = "false")]
    pub remove: bool,
}

#[derive(Debug, Subcommand)]
pub enum DataFusionSubCommand {
    /// Execute query
    Query(QueryArgs),

    /// Show schema
    Schema(SchemaArgs),
}

#[derive(Default, Debug, Args)]
pub struct QueryArgs {
    #[clap(short = 'q', long = "query")]
    query: String,
}

#[derive(Default, Debug, Args)]
pub struct SchemaArgs {}

#[derive(Error, Debug)]
pub enum DFError {
    #[error("file extension must be either `json` or `njson`(new line delimited json), `parquet`, `csv`")]
    UnsupportFileFormat,
}

pub async fn write_file(df: DataFrame, filename: String, remove: bool) -> Result<()> {
    let path = Path::new(&filename);
    if let Some(output_ex) = path.extension().and_then(OsStr::to_str) {
        if path.exists() && remove {
            remove_dir_all(&filename)?;
        }
        let write_options = DataFrameWriteOptions::default();
        match output_ex {
            "json" => {
                let options = JsonOptions::default();
                df.write_json(&filename, write_options, Some(options)).await?;
            }
            "parquet" => {
                // TODO: set some options if necessary
                let options = TableParquetOptions::default();
                df.write_parquet(&filename, write_options, Some(options))
                    .await?;
            }
            "csv" => {
                let mut options = CsvOptions::default();
                options.has_header = true;
                df.write_csv(&filename, write_options, Some(options)).await?;
            }
            _ => anyhow::bail!(DFError::UnsupportFileFormat),
        };
        Ok(())
    } else {
        anyhow::bail!(DFError::UnsupportFileFormat)
    }
}

pub fn session_context() -> SessionContext {
    let cfg = SessionConfig::new().with_information_schema(true);
    SessionContext::new_with_config(cfg)
}

pub async fn register_source(ctx: &SessionContext, inputs: Vec<String>) -> Result<()> {
    for (i, input) in inputs.iter().enumerate() {
        let table_id = format!("t{}", i);

        // GCS
        if let Ok(url) = Url::parse(input) {
            match url.scheme() {
                "gs" => {
                    if let Some(bucket_name) = url.host_str() {
                        let sa = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")?;
                        let gcs = GoogleCloudStorageBuilder::new()
                            .with_service_account_path(sa)
                            .with_bucket_name(bucket_name)
                            .build()?;
                        ctx.runtime_env().register_object_store(&url, Arc::new(gcs));
                    }
                }
                _ => {}
            }
        }

        let path = Path::new(input);
        if let Some(input_ex) = path.extension().and_then(OsStr::to_str) {
            match input_ex {
                "json" | "njson" => {
                    let mut options = NdJsonReadOptions::default();
                    options.file_extension = input_ex;
                    ctx.register_json(&table_id, input, options).await?
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
    Ok(())
}

pub async fn print_dataframe(df: DataFrame, as_json: bool) -> Result<()> {
    if as_json {
        let batches = df.collect().await?;
        let ref_batches: Vec<&RecordBatch> = batches.iter().collect();
        let mut writer = io::BufWriter::new(io::stdout());
        for d in datafusion::arrow::json::writer::record_batches_to_json_rows(&ref_batches)?
            .into_iter()
            .map(|val| serde_json::from_value(serde_json::Value::Object(val)))
            .take_while(|val| val.is_ok())
        {
            writer.write(serde_json::to_string::<serde_json::Value>(&d?)?.as_bytes())?;
            writer.write("\n".as_bytes())?;
        }
    } else {
        df.show().await?;
    }
    Ok(())
}

pub async fn handle(dfargs: DataFusionArgs) -> Result<()> {
    let ctx = session_context();

    register_source(&ctx, dfargs.inputs).await?;

    ctx.register_udf(udf_pow());
    ctx.register_udaf(udaf_string_agg());

    match dfargs.datafusion_sub_command {
        DataFusionSubCommand::Schema(_args) => {
            let df = ctx.sql("describe t0").await?;

            print_dataframe(df, dfargs.json).await?;

            Ok(())
        }
        DataFusionSubCommand::Query(args) => {
            let df = ctx.sql(&args.query).await?;

            print_dataframe(df.clone(), dfargs.json).await?;

            if let Some(output) = dfargs.output {
                write_file(df, output, dfargs.remove).await?;
            }
            Ok(())
        }
    }
}
