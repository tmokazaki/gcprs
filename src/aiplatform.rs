use crate::auth;
use aiplatform::{
    api::{
        GoogleCloudAiplatformV1Content, GoogleCloudAiplatformV1GenerateContentRequest,
        GoogleCloudAiplatformV1GenerateContentResponse, GoogleCloudAiplatformV1Part,
    },
    hyper, Aiplatform, Error, Result as GcpResult,
};
use google_aiplatform1 as aiplatform;

use anyhow;
use anyhow::Result;
use http_body_util::combinators::BoxBody;
use http_body_util::BodyExt;
use hyper::body::Bytes;
use serde::{Deserialize, Serialize};

pub struct AiPlatform {
    api: Aiplatform<auth::HttpsConnector>,
    project_id: String,
    location: String,
}

fn publisher_from_model_name(model_name: &str) -> String {
    match model_name {
        model_name if model_name.starts_with("gemini") => return "google".to_string(),
        model_name if model_name.starts_with("claude") => return "anthropic".to_string(),
        _ => return "google".to_string(),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LlmResponse {
    result: LlmResponseModel,
    token_info: TokenInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LlmResponseModel {
    Text(String),
    Image(Vec<u8>),
    None,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    model_name: String,
    prompt_tokens: Option<i32>,
    completion_tokens: Option<i32>,
    total_tokens: Option<i32>,
}

impl AiPlatform {
    fn to_model_name(&self, model_id: &str) -> String {
        format!(
            "projects/{}/locations/{}/publishers/{}/models/{}",
            self.project_id,
            self.location,
            publisher_from_model_name(model_id),
            model_id
        )
    }

    pub fn new(auth: &auth::GcpAuth, project_id: &str, location: &str) -> Result<Self> {
        let client = auth::new_client();
        let mut api = Aiplatform::new(client, auth.authenticator());
        api.root_url(format!("https://{}-aiplatform.googleapis.com/", location));
        api.base_url(format!("https://{}-aiplatform.googleapis.com/", location));
        Ok(AiPlatform {
            api,
            project_id: project_id.to_string(),
            location: location.to_string(),
        })
    }

    async fn handle_error<T>(result: GcpResult<T>) -> Result<T> {
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
                | Error::BadRequest(_)
                | Error::FieldClash(_)
                | Error::JsonDecodeError(_, _) => {
                    println!("{}", e);
                    Err(anyhow::anyhow!("{}", e))
                }
                Error::Failure(f) => {
                    println!("{:?}", f);
                    let bytes = f.into_body().collect().await?.to_bytes();
                    println!("{:?}", String::from_utf8(bytes.into())?);
                    Err(anyhow::anyhow!("failure!"))
                }
            },
            Ok(res) => Ok(res),
        }
    }

    pub async fn generate_content(&self, req: &str, model_id: &str) -> Result<LlmResponse> {
        let mut request = GoogleCloudAiplatformV1GenerateContentRequest::default();
        let mut part = GoogleCloudAiplatformV1Part::default();
        part.text = Some(req.to_string());
        request.contents = Some(vec![GoogleCloudAiplatformV1Content {
            role: Some("user".to_string()),
            parts: Some(vec![part]),
        }]);
        let result = self
            .api
            .projects()
            .locations_publishers_models_generate_content(request, &self.to_model_name(model_id))
            .doit()
            .await;
        println!("{:?}", result);
        match Self::handle_error(result).await {
            Ok(resp) => {
                let google_resp: GoogleCloudAiplatformV1GenerateContentResponse = resp.1;
                let usage_metadata = google_resp.usage_metadata.unwrap();
                let token_info = TokenInfo {
                    model_name: model_id.to_string(),
                    prompt_tokens: usage_metadata.prompt_token_count,
                    completion_tokens: usage_metadata.candidates_token_count,
                    total_tokens: usage_metadata.total_token_count,
                };
                let llm_resp = LlmResponse {
                    result: google_resp
                        .candidates
                        .map(|c| {
                            c[0].content
                                .as_ref()
                                .map(|c| {
                                    c.parts
                                        .as_ref()
                                        .map(|ps| {
                                            ps[0]
                                                .text
                                                .as_ref()
                                                .map(|x| LlmResponseModel::Text(x.clone()))
                                        })
                                        .flatten()
                                })
                                .flatten()
                        })
                        .flatten()
                        .unwrap_or(LlmResponseModel::None),
                    token_info,
                };
                Ok(llm_resp)
            }
            Err(e) => Err(e),
        }
    }
}
