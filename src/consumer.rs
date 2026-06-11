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

    let restored = stream
        .get_or_create_consumer(
            "user-service-restored",
            pull::Config {
                durable_name: Some("user-service-restored".to_string()),
                filter_subject: common::events::SUBJECT_USER_RESTORED.to_string(),
                ack_policy: AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let r = repo.clone();
    let js_pub = js.clone();
    tokio::spawn(async move { consume_registered(r, registered, js_pub).await });
    let r2 = repo.clone();
    tokio::spawn(async move { consume_deleted(r2, deleted).await });
    tokio::spawn(async move { consume_restored(repo, restored).await });
    tracing::info!("event consumer started");
    Ok(())
}

/// Redelivery bound before the saga gives up and emits a compensation event.
const MAX_PROFILE_ATTEMPTS: i64 = 5;

async fn consume_registered(repo: Repo, consumer: PullConsumer, js: jetstream::Context) {
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
                            // Saga: after exhausting retries, emit a compensation
                            // event so auth rolls back the half-created identity.
                            let delivered = msg.info().map(|i| i.delivered).unwrap_or(0);
                            if delivered >= MAX_PROFILE_ATTEMPTS {
                                let payload = serde_json::to_vec(&common::events::ProfileCreationFailed {
                                    user_id: ev.user_id.clone(),
                                    reason: e.to_string(),
                                })
                                .unwrap_or_default();
                                match js.publish(common::events::SUBJECT_PROFILE_FAILED, payload.into()).await {
                                    Ok(_) => {
                                        tracing::error!(user_id = %ev.user_id, attempts = delivered, "profile creation failed permanently; emitted compensation");
                                        let _ = msg.ack_with(AckKind::Term).await;
                                    }
                                    Err(pe) => {
                                        tracing::warn!(error = %pe, "emit compensation failed; will retry");
                                        let _ = msg.ack_with(AckKind::Nak(None)).await;
                                    }
                                }
                            } else {
                                tracing::warn!(error = %e, attempt = delivered, "upsert failed; will retry");
                                let _ = msg.ack_with(AckKind::Nak(None)).await;
                            }
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
                    Ok(uid) => {
                        // Soft by default; hard removes the row (mirrors the auth side).
                        let res = if ev.hard {
                            repo.hard_delete_profile(uid).await
                        } else {
                            repo.delete_profile(uid).await
                        };
                        match res {
                            Ok(_) => {
                                let _ = msg.ack().await;
                                tracing::info!(user_id = %ev.user_id, hard = ev.hard, "profile deleted from event");
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "delete failed; will retry");
                                let _ = msg.ack_with(AckKind::Nak(None)).await;
                            }
                        }
                    }
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

async fn consume_restored(repo: Repo, consumer: PullConsumer) {
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
            match serde_json::from_slice::<common::events::UserRestored>(&msg.payload) {
                Ok(ev) => match Uuid::parse_str(&ev.user_id) {
                    Ok(uid) => match repo.restore_profile(uid).await {
                        Ok(_) => {
                            let _ = msg.ack().await;
                            tracing::info!(user_id = %ev.user_id, "profile restored from event");
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "restore failed; will retry");
                            let _ = msg.ack_with(AckKind::Nak(None)).await;
                        }
                    },
                    Err(_) => {
                        let _ = msg.ack().await;
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "bad UserRestored payload");
                    let _ = msg.ack().await;
                }
            }
        }
    }
}
