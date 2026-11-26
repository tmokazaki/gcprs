//! Helpers shared by unit tests.

use crate::auth::{self, GcpAuth};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// A request captured by [`MockServer`].
#[derive(Debug)]
pub struct CapturedRequest {
    pub method: String,
    pub path: String,
    /// Header names are lower-cased.
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl CapturedRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// A minimal HTTP/1.1 server on 127.0.0.1 that answers one request with a fixed JSON body.
pub struct MockServer {
    /// e.g. `http://127.0.0.1:12345/`
    pub url: String,
    request: oneshot::Receiver<CapturedRequest>,
}

impl MockServer {
    pub async fn start(status: u16, json_body: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let (tx, rx) = oneshot::channel();
        let body = json_body.to_string();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];

            // Read until the end of the headers, then the body (Content-Length).
            let header_end = loop {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0, "connection closed before headers were received");
                buf.extend_from_slice(&chunk[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
            let mut lines = head.split("\r\n");
            let mut request_line = lines.next().unwrap().split(' ');
            let method = request_line.next().unwrap().to_string();
            let path = request_line.next().unwrap().to_string();
            let headers: Vec<(String, String)> = lines
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                .collect();
            let content_length = headers
                .iter()
                .find(|(k, _)| k == "content-length")
                .and_then(|(_, v)| v.parse::<usize>().ok())
                .unwrap_or(0);
            while buf.len() < header_end + content_length {
                let n = socket.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            let req_body = String::from_utf8_lossy(&buf[header_end..]).to_string();

            let response = format!(
                "HTTP/1.1 {} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                status,
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.ok();

            let _ = tx.send(CapturedRequest {
                method,
                path,
                headers,
                body: req_body,
            });
        });

        MockServer { url, request: rx }
    }

    /// Wait for the request the server received.
    pub async fn request(self) -> CapturedRequest {
        self.request.await.expect("mock server received no request")
    }
}

/// A `GcpAuth` that always returns the given access token, without network access.
pub async fn access_token_auth(token: &str) -> GcpAuth {
    auth::install_crypto_provider();
    let authenticator = auth::oauth2::AccessTokenAuthenticator::builder(token.to_string())
        .build()
        .await
        .expect("failed to build access token authenticator");
    GcpAuth::from_authenticator(authenticator)
}
