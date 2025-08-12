use crate::auth::{hyper_util, oauth2};
use anyhow::Result;
use http_body_util::{BodyExt, Empty};
use hyper_util::client::legacy::Client;
use oauth2::hyper::{body::Bytes, Method, Request};
use oauth2::hyper_rustls;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::fmt;
use std::process::Command;
use std::str;

static METADATA_ROOT: &str = "http://metadata.google.internal/computeMetadata/v1/";

static REQUEST_TYPE_ACCESS_TOKEN: &str = "auth-request-type/at";

// Auth request type
#[derive(Clone)]
enum RequestType {
    AccessToken,
    IdToken,
    MdsPing,
    ReauthStart,
    ReauthContinue,
}

impl fmt::Display for RequestType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestType::AccessToken => write!(f, "auth-request-type/at"),
            RequestType::IdToken => write!(f, "auth-request-type/it"),
            RequestType::MdsPing => write!(f, "auth-request-type/mds"),
            RequestType::ReauthStart => write!(f, "auth-request-type/re-start"),
            RequestType::ReauthContinue => write!(f, "auth-request-type/re-cont"),
        }
    }
}

#[derive(Clone)]
enum CredentialType {
    User,
    ServiceAccountAssertion,
    ServiceAccountJwt,
    ServiceAccountMds,
    ServiceAccountImpersonate,
}

impl fmt::Display for CredentialType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CredentialType::User => write!(f, "cred-type/u"),
            CredentialType::ServiceAccountAssertion => write!(f, "cred-type/sa"),
            CredentialType::ServiceAccountJwt => write!(f, "cred-type/jwt"),
            CredentialType::ServiceAccountMds => write!(f, "cred-type/mds"),
            CredentialType::ServiceAccountImpersonate => write!(f, "cred-type/imp"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MetadataApi {}

pub type HttpsConnector =
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>;

pub fn new_client(
) -> Client<HttpsConnector, http_body_util::combinators::BoxBody<Bytes, Infallible>> {
    Client::builder(hyper_util::rt::TokioExecutor::new()).build(
        hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .unwrap()
            .https_or_http()
            .enable_http1()
            .build(),
    )
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServiceAccountInfo {
    pub aliases: Vec<String>,
    pub email: String,
    pub scopes: Vec<String>,
}

impl MetadataApi {
    pub fn new() -> Self {
        MetadataApi {}
    }

    pub async fn service_account_info(&self) -> Result<ServiceAccountInfo> {
        let url = format!(
            "{}instance/service-accounts/default/?recursive=true",
            METADATA_ROOT
        );
        let client = new_client();
        let req = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header("Metadata-Flavor", "Google")
            .body(Empty::<Bytes>::new().boxed())?;
        // println!("req: {:?}", req);
        let resp = client.request(req).await;
        // println!("resp: {:?}", resp);
        match resp {
            Ok(resp) => {
                let bytes = resp.into_body().boxed().collect().await?.to_bytes();
                let body = String::from_utf8(bytes.into())?;
                let info = serde_json::from_str::<ServiceAccountInfo>(&body)?;
                // println!("body: {:?}", info);
                Ok(info)
            }
            Err(e) => {
                println!("err: {:?}", e);
                Err(e.into())
            }
        }
    }

    pub async fn generate_id_token(&self, audience: &str) -> Result<String> {
        let url = format!(
            "{}instance/service-accounts/default/identity?audience={}&format=full",
            METADATA_ROOT, audience
        );
        let client = new_client();
        let req = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header("Metadata-Flavor", "Google")
            //.header("x-goog-api-client", format!("{} {} {}", , RequestType::IdToken, CredentialType::ServiceAccountMds))
            .body(Empty::<Bytes>::new().boxed())?;
        // println!("req: {:?}", req);
        let resp = client.request(req).await;
        //  println!("resp: {:?}", resp);
        match resp {
            Ok(resp) => {
                let bytes = resp.into_body().boxed().collect().await?.to_bytes();
                let body = String::from_utf8(bytes.into())?;
                println!("body: {:?}", body);
                Ok(body)
            }
            Err(_) => {
                let output = Command::new("gcloud")
                    .arg("auth")
                    .arg("print-identity-token")
                    .output();
                match output {
                    Ok(output) => Ok(String::from(str::from_utf8(&output.stdout).unwrap().trim())),
                    Err(e) => Err(e.into()),
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "metadata_test.rs"]
mod tests;
