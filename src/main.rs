mod consumer;
mod grpc;
mod repo;

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use tonic::transport::Server;

use proto::user::v1::user_service_server::UserServiceServer;

use crate::grpc::UserSvc;
use crate::repo::Repo;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    common::telemetry::init("user");

    let db_url = common::must_env("USER_DATABASE_URL");
    let port = common::env_or("USER_GRPC_PORT", "50052");

    let pool = connect_with_retry(&db_url).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    tracing::info!("migrations applied");

    let repo = Repo::new(pool);

    // Subscribe to auth lifecycle events to keep profiles in sync. Optional:
    // without NATS_URL profiles are created lazily on first read instead.
    match common::config::nats_url() {
        url if !url.is_empty() => {
            let js = common::events::connect(&url).await?;
            common::events::ensure_stream(&js).await?;
            consumer::run(repo.clone(), js).await?;
            tracing::info!(nats = %url, "event consumer connected");
        }
        _ => tracing::warn!("NATS_URL not set — event consumer disabled"),
    }

    let svc = UserSvc::new(repo);

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<UserServiceServer<UserSvc>>().await;

    // Defense-in-depth: require the shared internal token on UserService calls.
    let token = common::config::internal_token();
    let check = move |req: tonic::Request<()>| -> Result<tonic::Request<()>, tonic::Status> {
        if token.is_empty() {
            return Ok(req);
        }
        match req.metadata().get("x-internal-token").and_then(|v| v.to_str().ok()) {
            Some(t) if t == token => Ok(req),
            _ => Err(tonic::Status::unauthenticated(
                "missing or invalid internal service token",
            )),
        }
    };

    let addr = format!("0.0.0.0:{port}").parse()?;
    tracing::info!(%addr, "user service listening");
    Server::builder()
        .add_service(health_service)
        .add_service(UserServiceServer::with_interceptor(svc, check))
        .serve(addr)
        .await?;
    Ok(())
}

async fn connect_with_retry(url: &str) -> anyhow::Result<sqlx::PgPool> {
    let mut last_err = None;
    for _ in 0..15 {
        match PgPoolOptions::new().max_connections(10).connect(url).await {
            Ok(pool) => return Ok(pool),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Err(anyhow::anyhow!("postgres not reachable: {}", last_err.unwrap()))
}
