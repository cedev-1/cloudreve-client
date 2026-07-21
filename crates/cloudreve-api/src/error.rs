use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// Standard API response wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub data: Option<T>,
    pub code: i32,
    pub msg: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregated_error: Option<HashMap<String, ApiResponse<T>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockConflictDetail {
    pub path: String,
    #[serde(rename = "type")]
    pub lock_type: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockOwner {
    pub owner: String,
    pub application: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockApplication {
    #[serde(rename = "type")]
    pub application_type: String,
}

/// Error codes used by the Cloudreve API
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Success = 0,
    Continue = 203,
    ObjectExisted = 40004,
    ParentNotExist = 40016,
    CredentialInvalid = 40020,
    IncorrectPassword = 40069,
    LockConflict = 40073,
    StaleVersion = 40076,
    BatchOperationNotFullyCompleted = 40081,
    DomainNotLicensed = 40087,
    AnonymousAccessDenied = 40088,
    SessionExpired = 40089,
    PurchaseRequired = 40083,
    LoginRequired = 401,
    PermissionDenied = 403,
    NotFound = 404,
}

impl ErrorCode {
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::Success),
            203 => Some(Self::Continue),
            40020 => Some(Self::CredentialInvalid),
            40069 => Some(Self::IncorrectPassword),
            40073 => Some(Self::LockConflict),
            40076 => Some(Self::StaleVersion),
            40081 => Some(Self::BatchOperationNotFullyCompleted),
            40087 => Some(Self::DomainNotLicensed),
            40088 => Some(Self::AnonymousAccessDenied),
            40089 => Some(Self::SessionExpired),
            40083 => Some(Self::PurchaseRequired),
            401 => Some(Self::LoginRequired),
            403 => Some(Self::PermissionDenied),
            404 => Some(Self::NotFound),
            _ => None,
        }
    }

    /// Check if this error code indicates an authentication/credential issue
    pub fn is_credential_error(&self) -> bool {
        matches!(
            self,
            Self::CredentialInvalid | Self::LoginRequired | Self::SessionExpired
        )
    }
}

/// Main error type for the Cloudreve API client
#[derive(Error, Debug)]
pub enum ApiError {
    /// API returned an error response
    #[error("API error (code {code}): {message}")]
    ApiError {
        code: i32,
        message: String,
        error_detail: Option<String>,
        correlation_id: Option<String>,
        aggregated_errors: Option<HashMap<String, String>>,
    },

    /// Lock conflict error (40073)
    #[error("Lock conflict: {message}")]
    LockConflict {
        message: String,
        detail: Option<LockConflictDetail>,
    },

    /// Batch operation not fully completed (40081)
    #[error("Batch operation not fully completed: {message}")]
    BatchError {
        message: String,
        aggregated_errors: Option<HashMap<String, String>>,
    },

    /// Login required or credential invalid (401, 40020)
    #[error("Login required: {0}")]
    LoginRequired(String),

    /// Access token expired and needs refresh
    #[error("Access token expired")]
    AccessTokenExpired,

    /// Refresh token expired, need to login again
    #[error("Refresh token expired, please login again")]
    RefreshTokenExpired,

    /// HTTP request error
    #[error("HTTP request error: {0}")]
    RequestError(#[from] reqwest::Error),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Invalid URL
    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    /// No tokens available
    #[error("No authentication tokens available")]
    NoTokensAvailable,

    /// Invalid JWT token
    #[error("Invalid token: {0}")]
    InvalidToken(String),

    /// SSE connection returned non-SSE response (server returned error before upgrading)
    #[error("SSE connection failed (code {code}): {message}")]
    SseNotUpgraded { code: i32, message: String },

    /// SSE stream error
    #[error("SSE stream error: {0}")]
    SseStreamError(String),

    /// Generic error
    #[error("{0}")]
    Other(String),
}

impl ApiError {
    /// Create an ApiError from an API response
    pub fn from_response<T>(response: ApiResponse<T>) -> Self {
        let code = response.code;

        // Handle specific error codes
        match ErrorCode::from_code(code) {
            Some(ErrorCode::LockConflict) => ApiError::LockConflict {
                message: response.msg,
                detail: None, // Will be populated by the client when parsing raw response
            },
            Some(ErrorCode::BatchOperationNotFullyCompleted) => {
                let aggregated = response
                    .aggregated_error
                    .map(|errors| errors.into_iter().map(|(k, v)| (k, v.msg)).collect());
                ApiError::BatchError {
                    message: response.msg,
                    aggregated_errors: aggregated,
                }
            }
            Some(ErrorCode::LoginRequired)
            | Some(ErrorCode::CredentialInvalid)
            | Some(ErrorCode::SessionExpired) => ApiError::LoginRequired(response.msg),
            _ => ApiError::ApiError {
                code,
                message: response.msg,
                error_detail: response.error,
                correlation_id: response.correlation_id,
                aggregated_errors: response
                    .aggregated_error
                    .map(|errors| errors.into_iter().map(|(k, v)| (k, v.msg)).collect()),
            },
        }
    }

    /// Check if this error is recoverable by retrying with a refreshed token
    pub fn is_token_expired(&self) -> bool {
        matches!(self, ApiError::AccessTokenExpired)
    }

    /// Check if this error requires login
    pub fn requires_login(&self) -> bool {
        matches!(
            self,
            ApiError::LoginRequired(_) | ApiError::RefreshTokenExpired
        )
    }
}

/// Result type alias for API operations
pub type ApiResult<T> = Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn response(code: i32, msg: &str) -> ApiResponse<()> {
        ApiResponse {
            data: None,
            code,
            msg: msg.to_string(),
            error: None,
            correlation_id: None,
            aggregated_error: None,
        }
    }

    #[test]
    fn error_code_from_code_maps_known_values() {
        assert_eq!(ErrorCode::from_code(0), Some(ErrorCode::Success));
        assert_eq!(ErrorCode::from_code(40073), Some(ErrorCode::LockConflict));
        assert_eq!(ErrorCode::from_code(401), Some(ErrorCode::LoginRequired));
        assert_eq!(ErrorCode::from_code(404), Some(ErrorCode::NotFound));
    }

    #[test]
    fn error_code_from_code_returns_none_for_unknown() {
        assert_eq!(ErrorCode::from_code(12345), None);
        // ParentNotExist has a variant but is intentionally not mapped.
        assert_eq!(ErrorCode::from_code(40016), None);
    }

    #[test]
    fn is_credential_error_only_for_auth_codes() {
        assert!(ErrorCode::CredentialInvalid.is_credential_error());
        assert!(ErrorCode::LoginRequired.is_credential_error());
        assert!(ErrorCode::SessionExpired.is_credential_error());
        assert!(!ErrorCode::NotFound.is_credential_error());
        assert!(!ErrorCode::Success.is_credential_error());
    }

    #[test]
    fn from_response_maps_lock_conflict() {
        let err = ApiError::from_response(response(40073, "locked"));
        match err {
            ApiError::LockConflict { message, detail } => {
                assert_eq!(message, "locked");
                assert!(detail.is_none());
            }
            other => panic!("expected LockConflict, got {other:?}"),
        }
    }

    #[test]
    fn from_response_maps_batch_error_with_aggregated() {
        let mut resp = response(40081, "partial");
        let mut agg = HashMap::new();
        agg.insert("file1".to_string(), response(404, "missing"));
        resp.aggregated_error = Some(agg);

        match ApiError::from_response(resp) {
            ApiError::BatchError {
                message,
                aggregated_errors,
            } => {
                assert_eq!(message, "partial");
                let agg = aggregated_errors.expect("aggregated errors present");
                assert_eq!(agg.get("file1").map(String::as_str), Some("missing"));
            }
            other => panic!("expected BatchError, got {other:?}"),
        }
    }

    #[test]
    fn from_response_maps_auth_codes_to_login_required() {
        for code in [401, 40020, 40089] {
            match ApiError::from_response(response(code, "auth")) {
                ApiError::LoginRequired(msg) => assert_eq!(msg, "auth"),
                other => panic!("expected LoginRequired for {code}, got {other:?}"),
            }
        }
    }

    #[test]
    fn from_response_defaults_to_generic_api_error() {
        let mut resp = response(500, "boom");
        resp.error = Some("stacktrace".to_string());
        resp.correlation_id = Some("cid-1".to_string());

        match ApiError::from_response(resp) {
            ApiError::ApiError {
                code,
                message,
                error_detail,
                correlation_id,
                aggregated_errors,
            } => {
                assert_eq!(code, 500);
                assert_eq!(message, "boom");
                assert_eq!(error_detail.as_deref(), Some("stacktrace"));
                assert_eq!(correlation_id.as_deref(), Some("cid-1"));
                assert!(aggregated_errors.is_none());
            }
            other => panic!("expected ApiError, got {other:?}"),
        }
    }

    #[test]
    fn is_token_expired_only_for_access_token_expired() {
        assert!(ApiError::AccessTokenExpired.is_token_expired());
        assert!(!ApiError::RefreshTokenExpired.is_token_expired());
        assert!(!ApiError::NoTokensAvailable.is_token_expired());
    }

    #[test]
    fn requires_login_for_login_and_refresh_expiry() {
        assert!(ApiError::LoginRequired("x".into()).requires_login());
        assert!(ApiError::RefreshTokenExpired.requires_login());
        assert!(!ApiError::AccessTokenExpired.requires_login());
    }

    #[test]
    fn display_includes_code_and_message() {
        let err = ApiError::ApiError {
            code: 42,
            message: "nope".to_string(),
            error_detail: None,
            correlation_id: None,
            aggregated_errors: None,
        };
        assert_eq!(err.to_string(), "API error (code 42): nope");
    }
}
