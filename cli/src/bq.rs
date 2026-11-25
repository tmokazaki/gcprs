use crate::common::{render as render2, OutputFormat, TableView};
use anyhow::Result;
use bigquery::{Bq, BqDataset, BqListParam, BqProject, BqQueryParam, BqRow, BqTable, QueryResult};
use clap::{Args, Subcommand};
use gcprs::auth;
use gcprs::bigquery;
use json_to_table::{json_to_table, Orientation};
use std::env;
use tabled::settings::Style;

#[derive(Debug, Args)]
pub struct BqArgs {
    /// GCP Project ID to use
    #[clap(short = 'p', long = "project")]
    pub project: Option<String>,

    /// Output CSV
    #[clap(short = 'c', long = "csv", default_value = "false")]
    pub csv: bool,

    /// Output raw JSON
    #[clap(short = 'j', long = "json", default_value = "false")]
    pub json: bool,

    /// Output New line delimited JSON
    /// This must be used with json output otherwise ignored.
    #[clap(short = 'n', long = "new_line", default_value = "false")]
    pub new_line: bool,

    /// Authenticate with user application. otherwise authenticate with service account
    #[clap(short = 'a', long = "auth_user", default_value = "true")]
    pub auth_user: bool,

    #[clap(subcommand)]
    pub bq_sub_command: BqSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum BqSubCommand {
    /// Show available Project list
    ListProject,
    /// Show available Dataset list
    ListDataset,
    /// Show available Table list
    ListTables(ListTablesArgs),
    /// Show Table Schema JSON
    TableSchema(TableSchemaArgs),
    /// Delete Table
    TableDelete(TableDeleteArgs),
    /// Show Table Data
    ListTableData(ListTableDataArgs),
    /// Show Query result
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
pub struct TableDeleteArgs {
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

impl TableView for BqProject {
    fn columns(&self) -> Vec<String> {
        vec![
            "friendly_name".to_owned(),
            "id".to_owned(),
            "numeric_id".to_owned(),
        ]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.friendly_name.clone(),
            self.id.clone(),
            self.numeric_id.clone(),
        ]
    }
}

impl TableView for BqDataset {
    fn columns(&self) -> Vec<String> {
        vec!["project".to_owned(), "dataset".to_owned()]
    }

    fn values(&self) -> Vec<String> {
        vec![self.project.clone(), self.dataset.clone()]
    }
}

impl TableView for BqTable {
    fn columns(&self) -> Vec<String> {
        vec![
            "project".to_owned(),
            "dataset".to_owned(),
            "table".to_owned(),
            "created_at".to_owned(),
            "expired_at".to_owned(),
        ]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.dataset.project.clone(),
            self.dataset.dataset.clone(),
            self.table_id.clone(),
            self.created_at
                .map(|t| format!("{}", t))
                .unwrap_or("".to_string()),
            self.expired_at
                .map(|t| format!("{}", t))
                .unwrap_or("".to_string()),
        ]
    }
}

impl TableView for BqRow {
    fn columns(&self) -> Vec<String> {
        self.columns()
            .iter()
            .map(|c| c.name().unwrap_or("".to_string()))
            .collect()
    }

    fn values(&self) -> Vec<String> {
        self.columns()
            .iter()
            .map(|r| serde_json::to_string(r.value()).unwrap())
            .collect()
    }
}

fn render_json(json_str: String, raw_json: bool) -> Result<()> {
    if raw_json {
        println!("{}", json_str)
    } else {
        let json_value = serde_json::from_str(&json_str)?;
        println!(
            "{}",
            json_to_table(&json_value)
                .with(Style::markdown())
                .object_orientation(Orientation::Row)
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
                anyhow::bail!("{}: PROJECT_ID is necessary", err)
            }
        }
    };

    let spauth = if bqargs.auth_user {
        auth::GcpAuth::from_user_auth().await.unwrap()
    } else {
        auth::GcpAuth::from_service_account().await.unwrap()
    };
    match bqargs.bq_sub_command {
        BqSubCommand::ListProject => {
            let data = Bq::list_project(spauth).await?;
            render2(
                &data,
                if bqargs.json {
                    OutputFormat::Json
                } else {
                    OutputFormat::Stdout
                },
                bqargs.new_line,
            )
        }
        BqSubCommand::ListDataset => {
            let bigquery = Bq::new(&spauth, &project).unwrap();
            let list_params = BqListParam::new();
            let data = bigquery.list_dataset(&list_params).await?;
            render2(
                &data,
                if bqargs.json {
                    OutputFormat::Json
                } else {
                    OutputFormat::Stdout
                },
                bqargs.new_line,
            )
        }
        BqSubCommand::ListTables(args) => {
            let bigquery = Bq::new(&spauth, &project).unwrap();
            let list_params = BqListParam::new();
            let data = bigquery.list_tables(&args.dataset, &list_params).await?;
            render2(
                &data,
                if bqargs.json {
                    OutputFormat::Json
                } else {
                    OutputFormat::Stdout
                },
                bqargs.new_line,
            )
        }
        BqSubCommand::ListTableData(args) => {
            let bigquery = Bq::new(&spauth, &project).unwrap();
            let mut list_params = BqListParam::new();
            list_params.max_results(args.max_results);
            let table = BqTable::new(&project, &args.dataset, &args.table);
            let data = bigquery.list_tabledata(&table, &list_params).await?;

            render2(
                &data,
                if bqargs.json {
                    OutputFormat::Json
                } else if bqargs.csv {
                    OutputFormat::Csv
                } else {
                    OutputFormat::Stdout
                },
                bqargs.new_line,
            )
        }
        BqSubCommand::Query(args) => {
            let bigquery = Bq::new(&spauth, &project).unwrap();
            let mut query_params = BqQueryParam::new(&args.query);
            query_params.max_results(args.max_results);
            query_params.dry_run(args.dry_run);
            let data = bigquery.query(&query_params).await?;

            match data {
                QueryResult::Data(ds) => render2(
                    &ds,
                    if bqargs.json {
                        OutputFormat::Json
                    } else if bqargs.csv {
                        OutputFormat::Csv
                    } else {
                        OutputFormat::Stdout
                    },
                    bqargs.new_line,
                ),
                QueryResult::Schema(schemas) => {
                    let json_str = serde_json::to_string(&schemas)?;
                    render_json(json_str, bqargs.json)
                }
            }
        }
        BqSubCommand::TableSchema(args) => {
            let bigquery = Bq::new(&spauth, &project).unwrap();
            let data = bigquery.get_table(&args.dataset, &args.table).await?;
            let json_str = serde_json::to_string(&data.schemas)?;
            render_json(json_str, bqargs.json)
        }
        BqSubCommand::TableDelete(args) => {
            let bigquery = Bq::new(&spauth, &project).unwrap();
            bigquery.delete_table(&args.dataset, &args.table).await
        }
    }
}
