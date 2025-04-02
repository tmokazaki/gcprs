use crate::auth;
use aiplatform::{
    api::{
        GoogleCloudAiplatformV1beta1Blob, GoogleCloudAiplatformV1beta1Content,
        GoogleCloudAiplatformV1beta1FileData, GoogleCloudAiplatformV1beta1GenerateContentRequest,
        GoogleCloudAiplatformV1beta1GenerateContentResponse, GoogleCloudAiplatformV1beta1Part,
        GoogleCloudAiplatformV1beta1PredictRequest, GoogleCloudAiplatformV1beta1PredictResponse,
    },
    Aiplatform, Error, Result as GcpResult,
};
use google_aiplatform1_beta1 as aiplatform;

use anyhow;
use anyhow::Result;
use http_body_util::BodyExt;
use mime_guess;
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

trait PartConverter {
    fn to_part(&self) -> GoogleCloudAiplatformV1beta1Part;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerateContentFileUri {
    pub uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerateContentFileBody {
    pub body: Vec<u8>,
    pub mime_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GenerateContentPart {
    Text(String),
    File(GenerateContentFileUri),
    FileBody(GenerateContentFileBody),
}

impl PartConverter for GenerateContentPart {
    fn to_part(&self) -> GoogleCloudAiplatformV1beta1Part {
        let mut part = GoogleCloudAiplatformV1beta1Part::default();
        match self {
            GenerateContentPart::Text(s) => {
                part.text = Some(s.to_string());
            }
            GenerateContentPart::File(f) => {
                part.file_data = Some(GoogleCloudAiplatformV1beta1FileData {
                    file_uri: Some(f.uri.to_string()),
                    mime_type: Some(
                        mime_guess::from_path(f.uri.clone())
                            .first_or_text_plain()
                            .to_string(),
                    ),
                });
            }
            GenerateContentPart::FileBody(f) => {
                part.inline_data = Some(GoogleCloudAiplatformV1beta1Blob {
                    data: Some(f.body.clone()),
                    mime_type: Some(f.mime_type.to_string()),
                });
            }
        }
        part
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
    Embeddings(Vec<f32>),
    None,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    model_name: String,
    prompt_tokens: Option<i32>,
    completion_tokens: Option<i32>,
    total_tokens: Option<i32>,
}

pub trait EmbedRequest
where
    Self: Serialize,
{
    /// Converts the current instance into a JSON value that represents
    /// an embedding request. This is typically used to prepare data
    /// for embedding operations in AI platforms.
    fn to_embed_request(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }
}

impl EmbedRequest for TextEmbedRequest {}

#[derive(Debug, Serialize, Deserialize)]
pub struct TextEmbedRequest {
    content: String,
    task_type: Option<EmbedTaskType>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EmbedTaskType {
    SemanticSimilarity,
    Classification,
    Clustering,
    RetrievalDocument,
    RetrievalQuery,
    QuestionAnswering,
    FactVerification,
    CodeRetrievalQuery,
}

impl TextEmbedRequest {
    pub fn new(content: &str, task_type: Option<EmbedTaskType>) -> Self {
        TextEmbedRequest {
            content: content.to_string(),
            task_type,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImageEmbedRequest {
    image: Vec<u8>,
}

impl EmbedRequest for ImageEmbedRequest {}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageEmbedContent {
    bytes_base64_encoded: Option<String>,
    gcs_uri: Option<String>,
    mime_type: String,
}

#[derive(Debug, Deserialize)]
struct TokenStatistics {
    token_count: i32,
    truncated: bool,
}

#[derive(Debug, Deserialize)]
struct EmbeddingsContent {
    statistics: TokenStatistics,
    values: Vec<f32>,
}

#[derive(Debug, Deserialize)]
struct PredictResponse {
    embeddings: EmbeddingsContent,
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

    /// Generates content using the specified model and input parts.
    /// This asynchronous function takes a vector of `GenerateContentPart`
    /// and a model identifier, then returns a `Result` containing an
    /// `LlmResponse` on success or an error on failure.
    pub async fn generate_content(
        &self,
        parts: Vec<GenerateContentPart>,
        model_id: &str,
    ) -> Result<LlmResponse> {
        let mut request = GoogleCloudAiplatformV1beta1GenerateContentRequest::default();
        request.contents = Some(vec![GoogleCloudAiplatformV1beta1Content {
            role: Some("user".to_string()),
            parts: Some(parts.iter().map(|p| p.to_part()).collect()),
        }]);
        let result = self
            .api
            .projects()
            .locations_publishers_models_generate_content(request, &self.to_model_name(model_id))
            //.locations_publishers_models_stream_generate_content(request, &self.to_model_name(model_id))
            .doit()
            .await;
        println!("the result {:?}", result);
        match Self::handle_error(result).await {
            Ok(resp) => {
                let google_resp: GoogleCloudAiplatformV1beta1GenerateContentResponse = resp.1;
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
                        .and_then(|c| c[0].content.clone())
                        .and_then(|c| c.parts)
                        .and_then(|ps| ps[0].text.clone())
                        .map(|x| LlmResponseModel::Text(x.clone()))
                        .unwrap_or(LlmResponseModel::None),
                    token_info,
                };
                Ok(llm_resp)
            }
            Err(e) => Err(e),
        }
    }
    /// Generates embeddings for the given request using the specified model.
    /// This asynchronous function accepts a reference to an `EmbedRequest` and
    /// a model identifier, then returns a `Result` containing an `LlmResponse`
    /// on success or an error on failure.
    pub async fn generate_embeddings<T: EmbedRequest>(
        &self,
        req: &T,
        model_id: &str,
    ) -> Result<LlmResponse> {
        let mut request = GoogleCloudAiplatformV1beta1PredictRequest::default();
        request.instances = Some(vec![req.to_embed_request()]);
        println!("request {:?}", request);
        let result = self
            .api
            .projects()
            .locations_publishers_models_predict(request, &self.to_model_name(model_id))
            .doit()
            .await;
        // println!("the result {:?}", result);
        match Self::handle_error(result).await {
            Ok(resp) => {
                let google_resp: GoogleCloudAiplatformV1beta1PredictResponse = resp.1;
                if let Some(predictions) = google_resp.predictions {
                    let predict: PredictResponse =
                        serde_json::from_value(predictions[0].clone()).unwrap();
                    let token_info = TokenInfo {
                        model_name: model_id.to_string(),
                        prompt_tokens: Some(predict.embeddings.statistics.token_count),
                        completion_tokens: Some(0),
                        total_tokens: Some(predict.embeddings.statistics.token_count),
                    };
                    let llm_resp = LlmResponse {
                        result: LlmResponseModel::Embeddings(predict.embeddings.values),
                        token_info,
                    };
                    Ok(llm_resp)
                } else {
                    Err(anyhow::anyhow!("no predictions"))
                }
            }
            Err(e) => Err(e),
        }
    }
}
