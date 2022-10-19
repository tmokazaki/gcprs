use anyhow::Result;
use bigquery::{Bq, BqListParam, BqQueryParam, BqTable};
use clap::{Args, Subcommand};
use gcprs::auth;
use gcprs::bigquery;
use json_to_table::{json_to_table, Orientation};
use std::env;
use std::process;
use tabled::Style;

#[derive(Debug, Args)]
pub struct BqArgs {
    /// GCP Project ID to use
    #[clap(short = 'p', long = "project")]
    pub project: Option<String>,

    /// Output raw JSON
    #[clap(short = 'r', long = "raw_json", default_value = "false")]
    pub raw: bool,

    #[clap(subcommand)]
    pub bq_sub_command: BqSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum BqSubCommand {
    /// Show Project JSON
    ListProject,
    /// Show Dataset JSON
    ListDataset,
    /// Show Table list JSON
    ListTables(ListTablesArgs),
    /// Show Table Schema JSON
    TableSchema(TableSchemaArgs),
    /// Show Table Data as JSON format
    ListTableData(ListTableDataArgs),
    /// Show Query result as JSON format
    Query(QueryArgs),
}

#[derive(Default, Debug, Args)]
pub struct ListTableDataArgs {
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
pub struct TableSchemaArgs {
    /// Dataset ID
    #[clap(short = 'd', long = "dataset")]
    dataset: String,

    /// Table ID
    #[clap(short = 't', long = "table")]
    table: String,
}

#[derive(Default, Debug, Args)]
pub struct ListTablesArgs {
    /// Dataset ID
    #[clap(short = 'd', long = "dataset")]
    dataset: String,
}

#[derive(Default, Debug, Args)]
pub struct QueryArgs {
    /// Maximum result of API result
    #[clap(short = 'm', long = "max_results", default_value = "1000")]
    max_results: u32,

    /// Dry run execution.
    #[clap(short = 'd', long = "dry_run")]
    dry_run: bool,

    /// Query String
    #[clap(short = 'q', long = "query")]
    query: String,
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
                .set_object_mode(Orientation::Horizontal)
                .to_string()
        );
    }
    Ok(())
}

pub async fn handle(bqargs: BqArgs) -> Result<()> {
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
    match bqargs.bq_sub_command {
        BqSubCommand::ListProject => {
            let data = Bq::list_project(spauth).await?;
            let json_str = serde_json::to_string(&data)?;
            render(json_str, bqargs.raw)
        }
        BqSubCommand::ListDataset => {
            let bigquery = Bq::new(spauth, &project).unwrap();
            let list_params = BqListParam::new();
            let data = bigquery.list_dataset(&list_params).await?;
            let json_str = serde_json::to_string(&data)?;
            render(json_str, bqargs.raw)
        }
        BqSubCommand::ListTables(args) => {
            let bigquery = Bq::new(spauth, &project).unwrap();
            let list_params = BqListParam::new();
            let data = bigquery.list_tables(&args.dataset, &list_params).await?;
            let json_str = serde_json::to_string(&data)?;
            render(json_str, bqargs.raw)
        }
        BqSubCommand::ListTableData(args) => {
            let bigquery = Bq::new(spauth, &project).unwrap();
            let mut list_params = BqListParam::new();
            list_params.max_results(args.max_results);
            let table = BqTable::new(&project, &args.dataset, &args.table);
            let data = bigquery.list_tabledata(&table, &list_params).await?;
            let json_str = serde_json::to_string(&data)?;
            render(json_str, bqargs.raw)
        }
        BqSubCommand::Query(args) => {
            let bigquery = Bq::new(spauth, &project).unwrap();
            let mut query_params = BqQueryParam::new(&args.query);
            query_params.max_results(args.max_results);
            query_params.dry_run(args.dry_run);
            let data = bigquery.query(&query_params).await?;
            let json_str = serde_json::to_string(&data)?;
            render(json_str, bqargs.raw)
        }
        BqSubCommand::TableSchema(args) => {
            let bigquery = Bq::new(spauth, &project).unwrap();
            let data = bigquery
                .get_table_schema(&args.dataset, &args.table)
                .await?;
            let json_str = serde_json::to_string(&data)?;
            render(json_str, bqargs.raw)
        }
    }
}
