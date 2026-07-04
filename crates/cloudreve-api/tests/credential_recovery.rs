//! Behavior tests for credential handling: a token rejected or expired must be
//! transparently refreshed instead of flagging the drive as "credentials expired".

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use base64::Engine;
use cloudreve_api::client::RequestOptions;
use cloudreve_api::models::user::Token;
use cloudreve_api::{Client, ClientConfig};
use chrono::{Duration, Utc};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Build a syntactically valid JWT with a non-empty `scopes` claim
/// (the client validates scopes on refreshed tokens).
fn fake_jwt(label: &str) -> String {
    let enc = |v: &serde_json::Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v.to_string())
    };
    format!(
        "{}.{}.sig-{label}",
        enc(&json!({"alg": "HS256", "typ": "JWT"})),
        enc(&json!({"scopes": ["file"], "label": label}))
    )
}

fn success_body(data: serde_json::Value) -> serde_json::Value {
    json!({"code": 0, "msg": "", "data": data})
}

fn session_expired_body() -> serde_json::Value {
    json!({"code": 40089, "msg": "session expired"})
}

fn token_data(access: &str, refresh: &str) -> serde_json::Value {
    json!({
        "access_token": access,
        "refresh_token": refresh,
        "access_expires": (Utc::now() + Duration::hours(1)).to_rfc3339(),
        "refresh_expires": (Utc::now() + Duration::days(90)).to_rfc3339(),
    })
}

/// Client whose on_credential_invalid callback counts invocations.
async fn client_with_tokens(
    server: &MockServer,
    access: &str,
    access_expires_in: Duration,
) -> (Arc<Client>, Arc<AtomicUsize>) {
    let mut client = Client::new(ClientConfig::new(server.uri()));
    let invalid_calls = Arc::new(AtomicUsize::new(0));
    let counter = invalid_calls.clone();
    client.set_on_credential_invalid(Arc::new(move || {
        let counter = counter.clone();
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
        })
    }));
    client
        .load_tokens(&Token {
            access_token: access.to_string(),
            refresh_token: "refresh-1".to_string(),
            access_expires: (Utc::now() + access_expires_in).to_rfc3339(),
            refresh_expires: (Utc::now() + Duration::days(90)).to_rfc3339(),
        })
        .await;
    (Arc::new(client), invalid_calls)
}

/// The server rejects an access token the client still believes is valid
/// (revocation, clock skew...). The client must refresh and retry the request
/// instead of reporting invalid credentials.
#[tokio::test]
async fn rejected_access_token_is_refreshed_and_request_retried() {
    let server = MockServer::start().await;
    let new_jwt = fake_jwt("new");

    // Stale token: rejected by the server
    Mock::given(method("GET"))
        .and(path("/api/v4/user/me"))
        .and(header("Authorization", "Bearer stale-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_expired_body()))
        .mount(&server)
        .await;
    // Refresh succeeds
    Mock::given(method("POST"))
        .and(path("/api/v4/session/token/refresh"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(success_body(token_data(&new_jwt, "refresh-2"))),
        )
        .mount(&server)
        .await;
    // Retried request with the new token succeeds
    Mock::given(method("GET"))
        .and(path("/api/v4/user/me"))
        .and(header("Authorization", format!("Bearer {new_jwt}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(success_body(json!({"ok": true}))))
        .mount(&server)
        .await;

    // Client thinks the token is still valid for 1h
    let (client, invalid_calls) =
        client_with_tokens(&server, "stale-token", Duration::hours(1)).await;

    let result: Result<serde_json::Value, _> =
        client.get("/user/me", RequestOptions::new()).await;

    assert!(result.is_ok(), "request should succeed after refresh: {result:?}");
    assert_eq!(
        invalid_calls.load(Ordering::SeqCst),
        0,
        "a recoverable token rejection must not report invalid credentials"
    );
}

/// Concurrent requests with an expired access token must result in a single
/// refresh call. With refresh token rotation, a second refresh with the same
/// (now consumed) refresh token fails and wrongly flags credentials as expired.
#[tokio::test]
async fn concurrent_requests_trigger_single_refresh() {
    let server = MockServer::start().await;
    let new_jwt = fake_jwt("new");

    // First refresh succeeds...
    Mock::given(method("POST"))
        .and(path("/api/v4/session/token/refresh"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(success_body(token_data(&new_jwt, "refresh-2"))),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    // ...any further refresh is rejected (refresh token rotated server-side)
    Mock::given(method("POST"))
        .and(path("/api/v4/session/token/refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_expired_body()))
        .mount(&server)
        .await;
    // Requests with the refreshed token succeed
    Mock::given(method("GET"))
        .and(path("/api/v4/user/me"))
        .and(header("Authorization", format!("Bearer {new_jwt}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(success_body(json!({"ok": true}))))
        .mount(&server)
        .await;

    // Access token already expired: every request needs a refresh first
    let (client, invalid_calls) =
        client_with_tokens(&server, "expired-token", Duration::seconds(-10)).await;

    let (r1, r2, r3, r4, r5) = tokio::join!(
        client.get::<serde_json::Value>("/user/me", RequestOptions::new()),
        client.get::<serde_json::Value>("/user/me", RequestOptions::new()),
        client.get::<serde_json::Value>("/user/me", RequestOptions::new()),
        client.get::<serde_json::Value>("/user/me", RequestOptions::new()),
        client.get::<serde_json::Value>("/user/me", RequestOptions::new()),
    );

    for (i, r) in [r1, r2, r3, r4, r5].iter().enumerate() {
        assert!(r.is_ok(), "concurrent request {i} should succeed: {r:?}");
    }
    assert_eq!(
        invalid_calls.load(Ordering::SeqCst),
        0,
        "concurrent refreshes must not report invalid credentials"
    );
}

/// When the refresh token itself is rejected, credentials are genuinely
/// invalid and the callback must fire.
#[tokio::test]
async fn refresh_rejection_notifies_credential_invalid() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v4/session/token/refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_expired_body()))
        .mount(&server)
        .await;

    let (client, invalid_calls) =
        client_with_tokens(&server, "expired-token", Duration::seconds(-10)).await;

    let result: Result<serde_json::Value, _> =
        client.get("/user/me", RequestOptions::new()).await;

    assert!(result.is_err(), "request must fail when refresh is rejected");
    assert!(
        invalid_calls.load(Ordering::SeqCst) >= 1,
        "a rejected refresh token must report invalid credentials"
    );
}
