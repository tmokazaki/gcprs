use std::fmt;
use anyhow::Result;
use hyper::{Request, Method, Client, client::HttpConnector};
use serde::{Deserialize, Serialize};

static METADATA_ROOT: &'static str = "http://metadata.google.internal/computeMetadata/v1/";

static REQUEST_TYPE_ACCESS_TOKEN: &'static str = "auth-request-type/at";

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

#[derive(Clone)]
pub struct MetadataApi {

}

pub fn new_client() -> hyper::Client<HttpConnector> {
    Client::new()
}

#[derive(Debug, Deserialize, Serialize)]
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
        let url = format!("{}instance/service-accounts/default/?recursive=true", METADATA_ROOT);
        let client = new_client();
        let req = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header("Metadata-Flavor", "Google")
            .body(hyper::Body::empty())?;
        // println!("req: {:?}", req);
        let resp = client.request(req).await;
        // println!("resp: {:?}", resp);
        match resp {
            Ok(resp) => {
                let bytes = hyper::body::to_bytes(resp.into_body()).await?;
                let body = String::from_utf8(bytes.into_iter().collect())?;
                let info = serde_json::from_str::<ServiceAccountInfo>(&body)?;
                // println!("body: {:?}", info);
                Ok(info)
            },
            Err(e) => {
                println!("err: {:?}", e);
                Err(e.into())
            }
        }
    }

    pub async fn generate_id_token(&self, audience: &str) -> Result<String> {
        let url = format!("{}instance/service-accounts/default/identity?audience={}&format=full", METADATA_ROOT, audience);
        let client = new_client();
        let req = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header("Metadata-Flavor", "Google")
            //.header("x-goog-api-client", format!("{} {} {}", , RequestType::IdToken, CredentialType::ServiceAccountMds))
            .body(hyper::Body::empty())?;
        // println!("req: {:?}", req);
        let resp = client.request(req).await;
       //  println!("resp: {:?}", resp);
        match resp {
            Ok(resp) => {
                let bytes = hyper::body::to_bytes(resp.into_body()).await?;
                let body = String::from_utf8(bytes.into_iter().collect())?;
                println!("body: {:?}", body);
                Ok(body)
            },
            Err(e) => {
                println!("err: {:?}", e);
                Err(e.into())
            }
        }
    }
}
