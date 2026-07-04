//! Behavior tests for the drive credential-expired state.

mod common;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cloudreve_api::models::user::Token;
use cloudreve_sync::drive::commands::MountCommand;
use common::TestEnv;

/// A successful token refresh proves credentials are valid again: the
/// "credentials expired" state must clear without manual re-authorization.
#[tokio::test]
async fn successful_token_refresh_clears_credential_expired_flag() {
    let env = TestEnv::new().await;
    let mount = Arc::new(env.mount);
    mount.spawn_command_processor(mount.clone()).await;

    mount.set_credential_expired(true).await;
    assert!(mount.get_status_flags().await.is_credential_expired());

    mount
        .command_tx
        .send(MountCommand::RefreshCredentials {
            credentials: Token {
                access_token: "new-access-token".to_string(),
                refresh_token: "new-refresh-token".to_string(),
                access_expires: (Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
                refresh_expires: (Utc::now() + chrono::Duration::days(90)).to_rfc3339(),
            },
        })
        .expect("send RefreshCredentials");

    for _ in 0..40 {
        if !mount.get_status_flags().await.is_credential_expired() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("credential expired flag still set after a successful token refresh");
}
