//! Keeping credentials out of logs.
//!
//! Upload and download URLs handed out by storage providers are signed: the
//! signature, an expiry and often an access key id ride in the query string
//! (S3 `X-Amz-Signature`, OSS `Signature`, COS `q-signature`, ...), sometimes
//! as userinfo. Whoever reads a log file holding one can fetch or overwrite
//! the object until it expires. Log lines and error messages therefore only
//! ever carry what identifies the endpoint: scheme, host, port and path.

/// `url` reduced to scheme, host, port and path — no userinfo, query or
/// fragment. Something that does not parse as a URL is not echoed back at
/// all: it could be anything, a secret included.
pub fn redact_url(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(parsed) => {
            let mut out = format!("{}://{}", parsed.scheme(), parsed.host_str().unwrap_or(""));
            if let Some(port) = parsed.port() {
                out.push_str(&format!(":{port}"));
            }
            out.push_str(parsed.path());
            out
        }
        Err(_) => "<unparsable url>".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_presigned_s3_url_keeps_only_its_endpoint() {
        let url = "https://bucket.s3.eu-west-3.amazonaws.com/uploads/abc/part?partNumber=3&uploadId=u-77&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAEXAMPLE%2F20261110&X-Amz-Signature=0f3c9e";
        assert_eq!(redact_url(url), "https://bucket.s3.eu-west-3.amazonaws.com/uploads/abc/part");
    }

    #[test]
    fn userinfo_and_fragment_are_dropped_and_the_port_kept() {
        assert_eq!(
            redact_url("http://admin:hunter2@10.0.0.5:5212/api/v4/slave/upload/xyz?chunk=0#frag"),
            "http://10.0.0.5:5212/api/v4/slave/upload/xyz"
        );
    }

    #[test]
    fn something_that_is_not_a_url_is_not_echoed() {
        assert_eq!(redact_url("token=secret-value"), "<unparsable url>");
    }
}
