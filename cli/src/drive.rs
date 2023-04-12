use crate::common::{render, TableView};
use anyhow::Result;
use clap::{Args, Subcommand};
use gcprs::auth;
use gcprs::drive as libdrive;
use libdrive::{Drive, DriveFile, DriveListParam};

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

    /// Upload and create new file in Drive
    Upload(UploadArgs),

    /// Overwrite existing file with uploading file in Drive
    Overwrite(OverwriteArgs),

    /// Download a file in Drive
    Download(DownloadArgs),
}

#[derive(Default, Debug, Args)]
pub struct ListArgs {
    /// query. see: https://developers.google.com/drive/api/guides/search-files
    #[clap(short = 'q', long = "query")]
    query: String,
}

#[derive(Default, Debug, Args)]
pub struct UploadArgs {
    /// input file path
    #[clap(short = 'i', long = "input")]
    input: String,

    /// parent
    #[clap(short = 'p', long = "parent")]
    parent: Option<String>,
}

#[derive(Default, Debug, Args)]
pub struct DownloadArgs {
    /// input file path
    #[clap(short = 't', long = "target_id")]
    id: String,
}

#[derive(Default, Debug, Args)]
pub struct OverwriteArgs {
    /// input file path
    #[clap(short = 't', long = "target_id")]
    id: String,

    /// input file path
    #[clap(short = 'i', long = "input")]
    input: String,
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
            render(&res, dargs.raw, false)
        }
        DriveSubCommand::Upload(args) => {
            let res = drive
                .create_file(&args.input, args.parent.map(|p| vec![p]))
                .await?;
            render(&vec![res], dargs.raw, false)
        }
        DriveSubCommand::Download(args) => {
            let res = drive.get_file_by_id(&args.id).await?;
            render(&vec![res], dargs.raw, false)
        }
        DriveSubCommand::Overwrite(args) => {
            let meta = drive.get_file_meta_by_id(&args.id).await?;
            let res = drive.update_file(meta, &args.input).await?;
            render(&vec![res], dargs.raw, false)
        }
    }
}
