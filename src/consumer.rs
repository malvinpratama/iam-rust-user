//! Subscribes the user service to auth's lifecycle events and keeps the profile
//! store in sync. Handlers are idempotent (at-least-once delivery):
//! UserRegistered upserts a profile, UserDeleted drops it.

use async_nats::jetstream::{
    self,
    consumer::{pull, AckPolicy, PullConsumer},
    AckKind,
};
use futures::StreamExt;
use uuid::Uuid;

use crate::repo::Repo;

/// Create durable pull consumers and spawn a task per subject.
pub async fn run(repo: Repo, js: jetstream::Context) -> anyhow::Result<()> {
    let stream = js
        .get_stream(common::events::STREAM)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let registered = stream
        .get_or_create_consumer(
            "user-service-registered",
            pull::Config {
                durable_name: Some("user-service-registered".to_string()),
                filter_subject: common::events::SUBJECT_USER_REGISTERED.to_string(),
                ack_policy: AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let deleted = stream
        .get_or_create_consumer(
            "user-service-deleted",
            pull::Config {
                durable_name: Some("user-service-deleted".to_string()),
                filter_subject: common::events::SUBJECT_USER_DELETED.to_string(),
                ack_policy: AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let r = repo.clone();
    tokio::spawn(async move { consume_registered(r, registered).await });
    tokio::spawn(async move { consume_deleted(repo, deleted).await });
    tracing::info!("event consumer started");
    Ok(())
}

async fn consume_registered(repo: Repo, consumer: PullConsumer) {
    loop {
        let mut messages = match consumer.messages().await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "messages stream error");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        while let Some(item) = messages.next().await {
            let msg = match item {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, "recv error");
                    break;
                }
            };
            match serde_json::from_slice::<common::events::UserRegistered>(&msg.payload) {
                Ok(ev) => match Uuid::parse_str(&ev.user_id) {
                    Ok(uid) => match repo.upsert_profile(uid, &ev.display_name).await {
                        Ok(_) => {
                            let _ = msg.ack().await;
                            tracing::info!(user_id = %ev.user_id, "profile created from event");
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "upsert failed; will retry");
                            let _ = msg.ack_with(AckKind::Nak(None)).await;
                        }
                    },
                    Err(_) => {
                        let _ = msg.ack().await; // unparseable id → don't redeliver
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "bad UserRegistered payload");
                    let _ = msg.ack().await;
                }
            }
        }
    }
}

async fn consume_deleted(repo: Repo, consumer: PullConsumer) {
    loop {
        let mut messages = match consumer.messages().await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "messages stream error");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        while let Some(item) = messages.next().await {
            let msg = match item {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, "recv error");
                    break;
                }
            };
            match serde_json::from_slice::<common::events::UserDeleted>(&msg.payload) {
                Ok(ev) => match Uuid::parse_str(&ev.user_id) {
                    Ok(uid) => match repo.delete_profile(uid).await {
                        Ok(_) => {
                            let _ = msg.ack().await;
                            tracing::info!(user_id = %ev.user_id, "profile deleted from event");
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "delete failed; will retry");
                            let _ = msg.ack_with(AckKind::Nak(None)).await;
                        }
                    },
                    Err(_) => {
                        let _ = msg.ack().await;
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "bad UserDeleted payload");
                    let _ = msg.ack().await;
                }
            }
        }
    }
}
