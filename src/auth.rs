use anyhow::Result;
use hyper;
use hyper_rustls;
use oauth2::authenticator::Authenticator;
use oauth2::authenticator_delegate::{DefaultInstalledFlowDelegate, InstalledFlowDelegate};
use oauth2::{
    authenticator::ApplicationDefaultCredentialsTypes, ApplicationDefaultCredentialsAuthenticator,
    ApplicationDefaultCredentialsFlowOpts,
};
use std::env;
use std::future::Future;
use std::pin::Pin;
use yup_oauth2 as oauth2;

#[derive(Clone)]
pub struct GcpAuth {
    auth: Authenticator<hyper_rustls::HttpsConnector<hyper::client::connect::HttpConnector>>,
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
    pub fn authenticator(
        &self,
    ) -> Authenticator<hyper_rustls::HttpsConnector<hyper::client::connect::HttpConnector>> {
        self.auth.clone()
    }

    /// Authenticate with service account.
    ///
    /// If there is `GOOGLE_APPLICATION_CREDENTIALS` in environment variables, use it first. Unless
    /// try to get credential from metadata server on GCP.
    pub async fn from_service_account() -> Result<GcpAuth> {
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
    pub async fn from_user_auth() -> Result<GcpAuth> {
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
}
