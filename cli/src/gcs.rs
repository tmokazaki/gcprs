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

    /// Authenticate with user application. otherwise authenticate with service account
    #[clap(short = 'a', long = "auth_user", default_value = "true")]
    pub auth_user: bool,

    #[clap(subcommand)]
    pub gcs_sub_command: GcsSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum GcsSubCommand {
    /// Show list objects
    ListObject,

    /// Get object metadata
    ObjectMetadata(ObjectArgs),

    /// Get object
    GetObject(ObjectArgs),

    /// Upload file
    UploadFile(UploadArgs),
}

#[derive(Default, Debug, Args)]
pub struct ObjectArgs {
    #[clap(short = 'n', long = "name")]
    name: String,
}

#[derive(Default, Debug, Args)]
pub struct UploadArgs {
    #[clap(short = 'f', long = "file")]
    file: String,

    #[clap(short = 'n', long = "name")]
    name: String,
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
    let spauth = if gcsargs.auth_user {
        auth::GcpAuth::from_user_auth().await.unwrap()
    } else {
        auth::GcpAuth::from_service_account().await.unwrap()
    };
    let (bucket, path) = if let Ok(url) = Url::parse(&gcsargs.bucket) {
        (
            url.host_str().unwrap_or(&"".to_string()).to_string(),
            url.path().to_string(),
        )
    } else {
        (gcsargs.bucket, "".to_string())
    };
    let cloud_storage = Gcs::new(spauth, bucket.clone());
    match gcsargs.gcs_sub_command {
        GcsSubCommand::ListObject => {
            let mut params = GcsListParam::new();
            params.prefix(&path);
            let data = cloud_storage.list_objects(&params).await?;
            render(&data, gcsargs.raw)
        }
        GcsSubCommand::ObjectMetadata(args) => {
            let data = cloud_storage.get_object_metadata(args.name).await?;
            render(&vec![data], gcsargs.raw)
        }
        GcsSubCommand::GetObject(args) => {
            let mut object = GcsObject::new(bucket, args.name);
            cloud_storage.get_object(&mut object).await?;
            if let Some(content) = object.content {
                println!("{}", content);
            }
            Ok(())
        }
        GcsSubCommand::UploadFile(args) => {
            let object = GcsObject::new(bucket, args.name);
            let result = cloud_storage.insert_file(&object, args.file, None).await?;
            render(&vec![result], gcsargs.raw)
        }
    }
}
