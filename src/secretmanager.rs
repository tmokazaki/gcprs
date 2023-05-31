use crate::auth;
use anyhow;
use anyhow::Result;
use google_secretmanager1 as secretmanager;
use secretmanager::{Error, Result as GcpResult, SecretManager as GcpSecretManager};

pub struct SecretGetParam {
    project_num: String,
    name: String,
    version: Option<String>,
}

impl SecretGetParam {
    pub fn new(project_num: &str, name: &str) -> Self {
        SecretGetParam {
            project_num: project_num.to_owned(),
            name: name.to_owned(),
            version: None,
        }
    }
    pub fn version(&mut self, version: &str) -> &mut Self {
        self.version = Some(version.to_string());
        self
    }
    fn to_resource(self) -> String {
        format!(
            "projects/{}/secrets/{}/versions/{}",
            self.project_num,
            self.name,
            self.version.unwrap_or_else(|| String::from("latest"))
        )
    }
}

pub struct SecretManager {
    api: GcpSecretManager<auth::HttpsConnector>,
}

impl SecretManager {
    pub fn new(auth: &auth::GcpAuth) -> Result<SecretManager> {
        let client = auth::new_client();
        let hub = GcpSecretManager::new(client, auth.authenticator());
        Ok(SecretManager { api: hub })
    }

    fn handle_error<T>(result: GcpResult<T>) -> Result<T> {
        match result {
            Err(e) => match e {
                // The Error enum provides details about what exactly happened.
                // You can also just use its `Debug`, `Display` or `Error` traits
                Error::HttpError(_)
                | Error::Io(_)
                | Error::MissingAPIKey
                | Error::MissingToken(_)
                | Error::Cancelled
                | Error::UploadSizeLimitExceeded(_, _)
                | Error::Failure(_)
                | Error::BadRequest(_)
                | Error::FieldClash(_)
                | Error::JsonDecodeError(_, _) => {
                    println!("{}", e);
                    Err(anyhow::anyhow!("{}", e))
                }
            },
            Ok(res) => Ok(res),
        }
    }

    pub async fn get(&self, p: SecretGetParam) -> Result<Option<String>> {
        let res = self
            .api
            .projects()
            .secrets_versions_access(&p.to_resource())
            .doit()
            .await;
        println!("{:?}", res);
        match res {
            Err(e) => match e {
                Error::BadRequest(_) => {
                    eprintln!("{}", e);
                    Err(anyhow::anyhow!("{}", e))
                }
                Error::HttpError(_)
                | Error::Io(_)
                | Error::MissingAPIKey
                | Error::MissingToken(_)
                | Error::Cancelled
                | Error::UploadSizeLimitExceeded(_, _)
                | Error::Failure(_)
                | Error::FieldClash(_)
                | Error::JsonDecodeError(_, _) => {
                    eprintln!("{}", e);
                    Err(anyhow::anyhow!("{}", e))
                }
            },
            Ok(resp) => {
                let secret = if let Some(payload) = resp.1.payload {
                    payload.data.map(|d| String::from_utf8(d).unwrap())
                } else {
                    None
                };
                Ok(secret)
            }
        }
    }
}
