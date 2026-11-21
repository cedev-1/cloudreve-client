//! Whether an on-demand drive's uploads are failing in a way the user has
//! to hear about.
//!
//! The vfs retries a failed upload on its own (`VfsEvent::UploadFailed
//! { will_retry: true }`) and keeps the change as a draft for as long as it
//! takes, so a single failure is nothing to report. But a save that can
//! never land — the account is full, the file is over the server's limit,
//! the folder is read-only — used to fail silently forever: the popup only
//! showed "waiting to upload". This tracks consecutive failures per drive
//! and turns them into a plain-language reason once they stop looking
//! transient.

/// Why uploads are not getting through, as far as the error text tells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadProblem {
    /// The account's storage is full.
    QuotaExceeded,
    /// The file is larger than the server or storage policy accepts.
    FileTooLarge,
    /// The server refuses writes there (read-only share, policy, group).
    PermissionDenied,
    /// Anything else, failing again and again (server down, network).
    KeepsFailing,
}

/// Consecutive failures after which even an unexplained error is reported:
/// one failure is routine (a dropped connection), three in a row on every
/// retry cycle is not.
pub const REPEATED_FAILURES: u32 = 3;

/// The problem an error message points to on its own, without waiting for
/// repeats: the ones retrying cannot fix. `None` for anything that may well
/// succeed next time.
///
/// Matched on text because that is what reaches this crate (the vfs sends
/// the error rendered, cause chain included): Cloudreve's own messages, the
/// storage providers' error codes relayed by the uploader, and the HTTP
/// status the server answered with.
pub fn classify(error: &str) -> Option<UploadProblem> {
    let e = error.to_lowercase();
    let any = |needles: &[&str]| needles.iter().any(|n| e.contains(n));
    if any(&["insufficient capacity", "insufficient storage", "storage quota", "quota exceeded", "capacity exceeded", "not enough capacity"]) {
        Some(UploadProblem::QuotaExceeded)
    } else if any(&["too large", "entitytoolarge", "exceeds the maximum", "file size limit", "size limit exceeded"]) {
        Some(UploadProblem::FileTooLarge)
    } else if any(&["permission denied", "accessdenied", "access denied", "forbidden", "not allowed", "status: 403", "(code 403)"]) {
        Some(UploadProblem::PermissionDenied)
    } else {
        None
    }
}

/// Upload outcomes of one drive, folded into whether there is a problem.
#[derive(Debug, Default)]
pub struct UploadHealth {
    consecutive_failures: u32,
    problem: Option<UploadProblem>,
}

impl UploadHealth {
    /// Records a failed upload cycle. `gave_up`: the vfs will not retry this
    /// one on its own. Returns the problem when this failure is the one
    /// that makes it (newly, or a different one) worth telling the user.
    pub fn on_failure(&mut self, error: &str, gave_up: bool) -> Option<UploadProblem> {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let now = classify(error).or_else(|| {
            (gave_up || self.consecutive_failures >= REPEATED_FAILURES).then_some(UploadProblem::KeepsFailing)
        });
        let Some(now) = now else { return None };
        let changed = self.problem != Some(now);
        self.problem = Some(now);
        changed.then_some(now)
    }

    /// Any upload getting through ends the problem.
    pub fn on_success(&mut self) {
        self.consecutive_failures = 0;
        self.problem = None;
    }

    pub fn problem(&self) -> Option<UploadProblem> {
        self.problem
    }
}

/// What the popup and the notification say for `problem` on `drive`.
pub fn problem_message(problem: UploadProblem, drive: &str) -> String {
    match problem {
        UploadProblem::QuotaExceeded => t!("uploadProblemQuota", drive = drive).into_owned(),
        UploadProblem::FileTooLarge => t!("uploadProblemTooLarge", drive = drive).into_owned(),
        UploadProblem::PermissionDenied => t!("uploadProblemPermission", drive = drive).into_owned(),
        UploadProblem::KeepsFailing => t!("uploadProblemRepeated", drive = drive).into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_retrying_cannot_fix_are_recognized() {
        assert_eq!(
            classify("failed to create upload session: API error (code 40050): Insufficient capacity"),
            Some(UploadProblem::QuotaExceeded)
        );
        assert_eq!(
            classify("upload chunk 3 failed: S3 error (EntityTooLarge): Your proposed upload exceeds the maximum allowed size"),
            Some(UploadProblem::FileTooLarge)
        );
        assert_eq!(
            classify("failed to create upload session: API error (code 40006): Storage policy not allowed for this group"),
            Some(UploadProblem::PermissionDenied)
        );
        assert_eq!(classify("HTTP 403 Forbidden: AccessDenied"), Some(UploadProblem::PermissionDenied));
    }

    #[test]
    fn transient_errors_are_not_a_problem_on_their_own() {
        for e in [
            "failed to upload chunk: error sending request: connection reset by peer",
            "failed to create upload session: operation timed out",
            "HTTP 502: Bad Gateway",
        ] {
            assert_eq!(classify(e), None, "{e}");
        }
    }

    #[test]
    fn a_full_account_is_reported_at_the_first_failure() {
        let mut h = UploadHealth::default();
        assert_eq!(h.on_failure("API error (code 40050): Insufficient capacity", false), Some(UploadProblem::QuotaExceeded));
        assert_eq!(h.on_failure("API error (code 40050): Insufficient capacity", false), None, "already reported");
        assert_eq!(h.problem(), Some(UploadProblem::QuotaExceeded));
    }

    #[test]
    fn a_dropped_connection_is_only_reported_once_it_keeps_happening() {
        let mut h = UploadHealth::default();
        let e = "error sending request: connection reset by peer";
        assert_eq!(h.on_failure(e, false), None);
        assert_eq!(h.on_failure(e, false), None);
        assert_eq!(h.on_failure(e, false), Some(UploadProblem::KeepsFailing));
    }

    #[test]
    fn an_upload_the_vfs_gave_up_on_is_reported_at_once() {
        let mut h = UploadHealth::default();
        assert_eq!(h.on_failure("operation timed out", true), Some(UploadProblem::KeepsFailing));
    }

    #[test]
    fn one_upload_getting_through_clears_the_problem_and_the_count() {
        let mut h = UploadHealth::default();
        let e = "operation timed out";
        h.on_failure(e, false);
        h.on_failure(e, false);
        h.on_failure(e, false);
        h.on_success();
        assert_eq!(h.problem(), None);
        assert_eq!(h.on_failure(e, false), None, "the count started over");
    }
}
