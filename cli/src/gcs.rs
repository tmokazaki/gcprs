use anyhow::Result;
use clap::{Args, Subcommand};
use gcprs::auth;
use gcprs::gcs as libgcs;
use libgcs::{Gcs, GcsListParam, GcsObject};
use serde::Serialize;
use tabled::{builder::Builder, Style};
use url::Url;

#[derive(Debug, Args)]
pub struct GcsArgs {
    #[clap(short = 'b', long = "bucket")]
    pub bucket: String,

    /// Output raw JSON
    #[clap(short = 'r', long = "raw_json", default_value = "false")]
    pub raw: bool,

    #[clap(subcommand)]
    pub gcs_sub_command: GcsSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum GcsSubCommand {
    /// Show list objects
    ListObject,
}

trait TableView {
    fn columns(&self) -> Vec<String>;
    fn values(&self) -> Vec<String>;
}

impl TableView for GcsObject {
    fn columns(&self) -> Vec<String> {
        vec![
            "bucket".to_string(),
            "content_type".to_string(),
            "name".to_string(),
            "size".to_string(),
            "created_at".to_string(),
            "updated_at".to_string(),
            "content".to_string(),
        ]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.bucket.clone(),
            self.content_type
                .as_ref()
                .map(|c| c.clone())
                .unwrap_or("".to_string()),
            self.name
                .as_ref()
                .map(|c| c.clone())
                .unwrap_or("".to_string()),
            self.size
                .map(|c| format!("{}", c))
                .unwrap_or("".to_string()),
            self.created_at
                .map(|c| format!("{}", c))
                .unwrap_or("".to_string()),
            self.updated_at
                .map(|c| format!("{}", c))
                .unwrap_or("".to_string()),
            self.content
                .as_ref()
                .map(|c| c.clone())
                .unwrap_or("".to_string()),
        ]
    }
}

fn render<T: TableView + Serialize>(data: &Vec<T>, raw_json: bool) -> Result<()> {
    if raw_json {
        let json_str = serde_json::to_string(&data)?;
        println!("{}", json_str)
    } else {
        let mut builder = Builder::default();
        let header = if 0 < data.len() {
            data[0].columns()
        } else {
            vec![]
        };
        builder.set_columns(header);
        for pj in data {
            builder.add_record(pj.values());
        }

        let mut table = builder.build();
        table.with(Style::markdown());

        println!("{}", table);
    }
    Ok(())
}

pub async fn handle(gcsargs: GcsArgs) -> Result<()> {
    let spauth = auth::GcpAuth::from_user_auth().await.unwrap();
    let url = Url::parse(&gcsargs.bucket)?;
    let cloud_storage = Gcs::new(
        spauth,
        url.host_str().unwrap_or(&"".to_string()).to_string(),
    );
    match gcsargs.gcs_sub_command {
        GcsSubCommand::ListObject => {
            let mut params = GcsListParam::new();
            params.prefix(url.path());
            let data = cloud_storage.list_objects(&params).await.unwrap();
            render(&data, gcsargs.raw)
        }
    }
}
