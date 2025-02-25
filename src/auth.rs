use anyhow::Result;
use chrono::{TimeZone, Utc};
use google_iamcredentials1 as iamcredentials1;
use http_body_util;
use http_body_util::BodyExt;
use hyper_util::client::legacy::Client;
use iamcredentials1::api::GenerateIdTokenRequest;
use iamcredentials1::{common::Body, hyper_rustls, IAMCredentials};
pub use iamcredentials1::{hyper, hyper_util, yup_oauth2 as oauth2};
use jsonwebtoken as jwt;
pub use oauth2::authenticator::Authenticator;
use oauth2::authenticator_delegate::{DefaultInstalledFlowDelegate, InstalledFlowDelegate};
use oauth2::{
    authenticator::ApplicationDefaultCredentialsTypes, ApplicationDefaultCredentialsAuthenticator,
    ApplicationDefaultCredentialsFlowOpts,
};
use std::env;
use std::future::Future;
use std::pin::Pin;

pub type HttpsConnector =
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>;

#[derive(Clone)]
pub struct GcpAuth {
    auth: Authenticator<HttpsConnector>,
}

pub fn new_client() -> Client<HttpsConnector, Body> {
    Client::builder(hyper_util::rt::TokioExecutor::new()).build(
        hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .unwrap()
            .https_or_http()
            .enable_http1()
            .build(),
    )
}

/// async function to be pinned by the `present_user_url` method of the trait
/// we use the existing `DefaultInstalledFlowDelegate::present_user_url` method as a fallback for
/// when the browser did not open for example, the user still see's the URL.
async fn browser_user_url(url: &str, need_code: bool) -> Result<String, String> {
    if webbrowser::open(url).is_ok() {
        println!("webbrowser was successfully opened.");
    }
    let def_delegate = DefaultInstalledFlowDelegate;
    def_delegate.present_user_url(url, need_code).await
}

/// our custom delegate struct we will implement a flow delegate trait for:
/// in this case we will implement the `InstalledFlowDelegated` trait
#[derive(Copy, Clone)]
struct InstalledFlowBrowserDelegate;

/// here we implement only the present_user_url method with the added webbrowser opening
/// the other behaviour of the trait does not need to be changed.
impl InstalledFlowDelegate for InstalledFlowBrowserDelegate {
    /// the actual presenting of URL and browser opening happens in the function defined above here
    /// we only pin it
    fn present_user_url<'a>(
        &'a self,
        url: &'a str,
        need_code: bool,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(browser_user_url(url, need_code))
    }
}

impl GcpAuth {
    pub fn authenticator(&self) -> Authenticator<HttpsConnector> {
        self.auth.clone()
    }

    /// Authenticate with service account.
    ///
    /// If there is `GOOGLE_APPLICATION_CREDENTIALS` in environment variables, use it first. Unless
    /// try to get credential from metadata server on GCP.
    pub async fn from_service_account() -> Result<Self> {
        let opts = ApplicationDefaultCredentialsFlowOpts::default();
        let authenticator = match ApplicationDefaultCredentialsAuthenticator::builder(opts).await {
            ApplicationDefaultCredentialsTypes::InstanceMetadata(auth) => auth
                .build()
                .await
                .expect("Unable to create instance metadata authenticator"),
            ApplicationDefaultCredentialsTypes::ServiceAccount(auth) => auth
                .build()
                .await
                .expect("Unable to create service account authenticator"),
        };

        Ok(GcpAuth {
            auth: authenticator,
        })
    }

    /// Authenticate with OAuth2 application.
    ///
    /// You have to set the secret JSON path to `GOOGLE_APPLICATION_SECRET` environment variable.
    pub async fn from_user_auth() -> Result<Self> {
        let application_secret_path = env::var("GOOGLE_APPLICATION_SECRET")?;
        let secret: oauth2::ApplicationSecret =
            oauth2::read_application_secret(application_secret_path)
                .await
                .expect("client secret could not be read");
        let auth = oauth2::InstalledFlowAuthenticator::builder(
            secret,
            oauth2::InstalledFlowReturnMethod::HTTPRedirect,
        )
        .persist_tokens_to_disk("tokencache.json")
        .flow_delegate(Box::new(InstalledFlowBrowserDelegate))
        .build()
        .await
        .expect("InstalledFlowAuthenticator failed to build");

        Ok(GcpAuth { auth })
    }

    pub async fn generate_id_token(&self) -> Option<String> {
        let hub = IAMCredentials::new(new_client(), self.authenticator());
        let req = GenerateIdTokenRequest::default();
        let name = format!(
            "projects/-/serviceAccounts/{}",
            "415279768469-compute@developer.gserviceaccount.com"
        );
        let result = hub
            .projects()
            .service_accounts_generate_id_token(req, &name)
            .doit()
            .await;
        println!("{:?}", result);
        match result {
            Ok((_, response)) => {
                println!("{:?}", response);
                return response.token.map(|t| t.clone());
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
        None
    }
}

const GOOGLE_OAUTH2_CERTS_URL: &str = "https://www.googleapis.com/oauth2/v1/certs";

fn get_iat(claim: &serde_json::Value) -> Option<chrono::DateTime<Utc>> {
    claim
        .get("iat")
        .map(|iat| match iat {
            serde_json::Value::Number(n) => {
                Some(Utc.timestamp_opt(n.as_i64().unwrap(), 0).unwrap())
            }
            _ => None,
        })
        .flatten()
}

fn get_exp(claim: &serde_json::Value) -> Option<chrono::DateTime<Utc>> {
    claim
        .get("exp")
        .map(|iat| match iat {
            serde_json::Value::Number(n) => {
                Some(Utc.timestamp_opt(n.as_i64().unwrap(), 0).unwrap())
            }
            _ => None,
        })
        .flatten()
}

/// Verify google jwt identity token
///
pub async fn verify_token(token: &String) -> Result<()> {
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .unwrap()
        .https_only()
        .enable_http1()
        .build();
    let client = new_client();
    let uri = GOOGLE_OAUTH2_CERTS_URL.parse().unwrap();

    let resp = client.get(uri).await?;
    if resp.status() != hyper::StatusCode::OK {
        println!("resp: {:?}", resp);
        anyhow::bail!("Access to secret api failure")
    }

    let bytes = resp.into_body().boxed().collect().await?.to_bytes();
    let body = String::from_utf8(bytes.into()).expect("response was not valid utf-8");
    let public_keys: serde_json::Value = serde_json::from_str(&body).unwrap();

    if let Ok(header) = jwt::decode_header(token) {
        //println!("{:?}", header);
        let secret = header
            .kid
            .map(|kid| match &public_keys.get(kid) {
                Some(serde_json::Value::String(s)) => Some(s),
                _ => None,
            })
            .flatten()
            .expect("there is no valid key");

        let mut validation = jwt::Validation::new(header.alg);
        validation.set_issuer(&["https://accounts.google.com", "accounts.google.com"]);
        validation.set_required_spec_claims(&["aud", "exp", "iss"]);
        let token_message = jwt::decode::<serde_json::Value>(
            &token,
            &jwt::DecodingKey::from_rsa_pem(secret.to_string().as_bytes())?,
            &validation,
        )?;
        println!("{:?}", token_message);
        println!(
            "{:?}, {:?}",
            get_iat(&token_message.claims),
            get_exp(&token_message.claims)
        );
    } else {
        anyhow::bail!("Invalid token format")
    }

    Ok(())
}
