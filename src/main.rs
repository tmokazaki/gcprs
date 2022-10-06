mod bigquery;
use anyhow;
use anyhow::Result;
use bigquery::{Bq, BqListParam, BqQueryParam, BqTable};
use clap::{Args, Parser, Subcommand};
use gcprs::auth;
use json_to_table::{json_to_table, Orientation};
use std::env;
use std::process;
use tabled::Style;

#[derive(Debug, Subcommand)]
enum SubCommand {
    Bq(BqArgs),
}

#[derive(Debug, Args)]
struct BqArgs {
    /// GCP Project ID to use
    #[clap(short = 'p', long = "project")]
    project: Option<String>,

    /// Output raw JSON
    #[clap(short = 'r', long = "raw_json", default_value = "false")]
    raw: bool,

    #[clap(subcommand)]
    bq_sub_command: BqSubCommand,
}

#[derive(Default, Debug, Args)]
struct ListTableDataArgs {
    #[clap(short = 'm', long = "max_results", default_value = "1000")]
    max_results: u32,

    /// Dataset ID
    #[clap(short = 'd', long = "dataset")]
    dataset: String,

    /// Table ID
    #[clap(short = 't', long = "table")]
    table: String,
}

#[derive(Default, Debug, Args)]
struct TableSchemaArgs {
    /// Dataset ID
    #[clap(short = 'd', long = "dataset")]
    dataset: String,

    /// Table ID
    #[clap(short = 't', long = "table")]
    table: String,
}

#[derive(Default, Debug, Args)]
struct QueryArgs {
    /// Maximum result of API result
    #[clap(short = 'm', long = "max_results", default_value = "1000")]
    max_results: u32,

    /// Query String
    #[clap(short = 'q', long = "query")]
    query: String,
}

#[derive(Debug, Subcommand)]
enum BqSubCommand {
    /// Show Table Schema JSON
    TableSchema(TableSchemaArgs),
    /// Show Table Data as JSON format
    ListTableData(ListTableDataArgs),
    /// Show Query result as JSON format
    Query(QueryArgs),
}

#[derive(Debug, Parser)]
struct Arguments {
    #[clap(subcommand)]
    command: SubCommand,
}

fn render(json_str: String, raw_json: bool) -> Result<()> {
    if raw_json {
        println!("{}", json_str)
    } else {
        let json_value = serde_json::from_str(&json_str)?;
        println!(
            "{}",
            json_to_table(&json_value)
                .set_style(Style::markdown())
                //        .set_object_mode(Orientation::Horizontal)
                .to_string()
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let main_args = Arguments::parse();
    //println!("{:?}", args);
    match main_args.command {
        SubCommand::Bq(bqargs) => {
            let project = if let Some(project) = bqargs.project {
                project
            } else {
                match env::var("PROJECT_ID") {
                    Ok(project) => project,
                    Err(err) => {
                        println!("{}: PROJECT_ID is necessary", err);
                        process::exit(1);
                    }
                }
            };

            let spauth = auth::GcpAuth::from_user_auth().await.unwrap();
            let bigquery = Bq::new(spauth, &project).unwrap();
            match bqargs.bq_sub_command {
                BqSubCommand::ListTableData(args) => {
                    let mut list_params = BqListParam::new();
                    list_params.max_results(args.max_results);
                    let table = BqTable::new(&project, &args.dataset, &args.table);
                    let data = bigquery.list_tabledata(&table, &list_params).await?;
                    let json_str = serde_json::to_string(&data)?;
                    render(json_str, bqargs.raw)
                }
                BqSubCommand::Query(args) => {
                    let mut query_params = BqQueryParam::new(&args.query);
                    query_params.max_results(args.max_results);
                    let data = bigquery.query(&query_params).await?;
                    let json_str = serde_json::to_string(&data)?;
                    render(json_str, bqargs.raw)
                }
                BqSubCommand::TableSchema(args) => {
                    let data = bigquery
                        .get_table_schema(&args.dataset, &args.table)
                        .await?;
                    let json_str = serde_json::to_string(&data)?;
                    render(json_str, bqargs.raw)
                }
            }
        }
    }
}
