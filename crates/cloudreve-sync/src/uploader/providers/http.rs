//! Shared HTTP helpers for storage provider uploads

use reqwest::{Response, StatusCode};

/// Consume a failed response, returning its status code and body text.
///
/// The body is read best-effort: a decoding failure yields an empty string so
/// callers can still surface the status code. Providers use the returned pair
/// to build their provider-specific error messages (or fall back to a generic
/// `HTTP {status}: {body}`).
pub(super) async fn read_error_body(response: Response) -> (StatusCode, String) {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    (status, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn returns_status_and_body() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
            .mount(&server)
            .await;

        let response = reqwest::get(server.uri()).await.unwrap();
        let (status, body) = read_error_body(response).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "upstream down");
    }

    #[tokio::test]
    async fn empty_body_yields_empty_string() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let response = reqwest::get(server.uri()).await.unwrap();
        let (status, body) = read_error_body(response).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.is_empty());
    }
}
