#[cfg(test)]
mod tests {
    #![allow(clippy::all)]
    use super::super::*;
    use crate::test_util::{access_token_auth, MockServer};
    use aiplatform::common::GetToken;

    #[tokio::test]
    async fn test_aiplatform_token_adapter_returns_token() {
        let auth = access_token_auth("aiplatform-test-token").await;
        let adapter = AiPlatformToken(auth.authenticator());

        let token = adapter
            .get_token(&["https://www.googleapis.com/auth/cloud-platform"])
            .await
            .unwrap();
        assert_eq!(token.as_deref(), Some("aiplatform-test-token"));
    }

    #[tokio::test]
    async fn test_aiplatform_token_adapter_is_clonable_as_get_token() {
        let auth = access_token_auth("cloned-token").await;
        let original = AiPlatformToken(auth.authenticator());
        let boxed: Box<dyn GetToken> = Box::new(original.clone());

        let token = boxed.get_token(&[]).await.unwrap();
        assert_eq!(token.as_deref(), Some("cloned-token"));
    }

    #[test]
    fn test_publisher_from_model_name() {
        assert_eq!(publisher_from_model_name("gemini-2.5-pro"), "google");
        assert_eq!(publisher_from_model_name("claude-sonnet"), "anthropic");
        assert_eq!(publisher_from_model_name("text-embedding-005"), "google");
    }

    #[tokio::test]
    async fn test_generate_content_with_mock_server() {
        let server = MockServer::start(
            200,
            r#"{
                "candidates": [{"content": {"role": "model", "parts": [{"text": "hello!"}]}}],
                "usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 2, "totalTokenCount": 5}
            }"#,
        )
        .await;
        let auth = access_token_auth("gen-test-token").await;
        let mut ai = AiPlatform::new(&auth, "test-project", "us-central1").unwrap();
        ai.api.root_url(server.url.clone());
        ai.api.base_url(server.url.clone());

        let resp = ai
            .generate_content(
                vec![GenerateContentPart::Text("hi".to_string())],
                "gemini-test",
            )
            .await
            .unwrap();

        match resp.result {
            LlmResponseModel::Text(t) => assert_eq!(t, "hello!"),
            other => panic!("unexpected result: {:?}", other),
        }
        assert_eq!(resp.token_info.model_name, "gemini-test");
        assert_eq!(resp.token_info.prompt_tokens, Some(3));
        assert_eq!(resp.token_info.completion_tokens, Some(2));
        assert_eq!(resp.token_info.total_tokens, Some(5));

        let req = server.request().await;
        assert_eq!(req.method, "POST");
        assert!(
            req.path.contains(
                "projects/test-project/locations/us-central1/publishers/google/models/gemini-test:generateContent"
            ),
            "unexpected path: {}",
            req.path
        );
        assert_eq!(req.header("authorization"), Some("Bearer gen-test-token"));
        let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
        assert_eq!(body["contents"][0]["role"], "user");
        assert_eq!(body["contents"][0]["parts"][0]["text"], "hi");
    }
}
