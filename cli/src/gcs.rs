use anyhow::Result;
use clap::{Args, Subcommand};
use gcprs::auth;
use gcprs::gcs as libgcs;
use libgcs::{Gcs, GcsListParam};
use url::Url;

#[derive(Debug, Args)]
pub struct GcsArgs {
    #[clap(short = 'b', long = "bucket")]
    pub bucket: String,

    #[clap(subcommand)]
    pub gcs_sub_command: GcsSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum GcsSubCommand {
    /// Show list objects
    ListObject,
}

pub async fn handle(gcsargs: GcsArgs) -> Result<()> {
    let spauth = auth::GcpAuth::from_user_auth().await.unwrap();
    let url = Url::parse(&gcsargs.bucket)?;
    let cloud_storage = Gcs::new(
        spauth,
        url.host_str().unwrap_or(&"".to_string()).to_string(),
    );
    match gcsargs.gcs_sub_command {
        ListObject => {
            let mut params = GcsListParam::new();
            params.prefix(url.path());
            let data = cloud_storage.list_objects(&params).await.unwrap();
            let json_str = serde_json::to_string(&data)?;
            println!("{}", json_str);
            Ok(())
        }
    }
}
