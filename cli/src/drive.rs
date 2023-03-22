use anyhow::Result;
use clap::{Args, Subcommand};
use gcprs::auth;
use gcprs::drive as libdrive;
use libdrive::{Drive, DriveFile, DriveListParam};
use serde::Serialize;
use tabled::{builder::Builder, Style};

#[derive(Debug, Args)]
pub struct DriveArgs {
    /// Authenticate with user application. otherwise authenticate with service account
    #[clap(short = 'a', long = "auth_user", default_value = "true")]
    pub auth_user: bool,

    /// Output raw JSON
    #[clap(short = 'r', long = "raw_json", default_value = "false")]
    pub raw: bool,

    #[clap(subcommand)]
    pub drive_sub_command: DriveSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum DriveSubCommand {
    /// Query drive file
    List(ListArgs),
}

#[derive(Default, Debug, Args)]
pub struct ListArgs {
    /// query. see: https://developers.google.com/drive/api/guides/search-files
    #[clap(short = 'q', long = "query")]
    query: String,
}

trait TableView {
    fn columns(&self) -> Vec<String>;
    fn values(&self) -> Vec<String>;
}

impl TableView for DriveFile {
    fn columns(&self) -> Vec<String> {
        vec![
            "id".to_string(),
            "name".to_string(),
            "mime_type".to_string(),
            "size".to_string(),
            "owners".to_string(),
            "created_at".to_string(),
            "modified_at".to_string(),
            "web_view_link".to_string(),
        ]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.id
                .as_ref()
                .map(|c| c.clone())
                .unwrap_or("".to_string()),
            self.name.clone(),
            self.mime_type
                .as_ref()
                .map(|c| c.clone())
                .unwrap_or("".to_string()),
            format!("{}", self.size),
            self.owners
                .as_ref()
                .map(|o| o.join(","))
                .unwrap_or("".to_string()),
            self.created_at
                .map(|c| format!("{}", c))
                .unwrap_or("".to_string()),
            self.modified_at
                .map(|c| format!("{}", c))
                .unwrap_or("".to_string()),
            self.web_view_link
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

pub async fn handle(dargs: DriveArgs) -> Result<()> {
    let spauth = if dargs.auth_user {
        auth::GcpAuth::from_user_auth().await.unwrap()
    } else {
        auth::GcpAuth::from_service_account().await.unwrap()
    };
    let drive = Drive::new(&spauth);
    match dargs.drive_sub_command {
        DriveSubCommand::List(args) => {
            let mut param = DriveListParam::new();
            param.query(&args.query);
            let res = drive.list_files(&param).await?;
            render(&res, dargs.raw)
        }
    }
}
