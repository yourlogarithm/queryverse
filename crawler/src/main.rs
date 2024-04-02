#[macro_use]
extern crate lazy_static;

mod crawl;
mod redis;
mod robots;
mod state;
mod tls;

use axum::{http::StatusCode, routing::get, Json, Router};
use deadpool_redis::{Config, Runtime};
use dotenvy::dotenv;
use lapin::{options::QueueDeclareOptions, types::FieldTable, Connection, ConnectionProperties};
use std::{env, net::SocketAddr};
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::state::State;

static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

async fn root() -> Json<&'static str> {
    Json("{\"status\": \"OK\"}")
}

async fn fallback_route() -> (StatusCode, &'static str) {
    warn!("Invalid route accessed");
    (
        StatusCode::NOT_FOUND,
        "The requested resource was not found",
    )
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crawler=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg =
        Config::from_url(env::var("REDIS_URL").unwrap_or("redis://localhost:6379".to_string()));
    let redis_pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    let reqwest_client = reqwest::Client::builder()
        .user_agent(APP_USER_AGENT)
        .build()
        .unwrap();

    let options = ConnectionProperties::default()
        .with_executor(tokio_executor_trait::Tokio::current())
        .with_reactor(tokio_reactor_trait::Tokio);

    let connection = Connection::connect(
        &env::var("AMQP_URL").unwrap_or("amqp://localhost:5672".to_string()),
        options,
    )
    .await
    .unwrap();
    let amqp_channel = connection.create_channel().await.unwrap();

    let _queue = amqp_channel
        .queue_declare(
            "crawled_urls",
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    let state = State {
        redis_pool,
        reqwest_client,
        amqp_channel,
    };

    let app = Router::new()
        .route("/", get(root))
        .nest(
            "/v1",
            Router::new()
                .route("/url/:url", get(crawl::crawl))
                .with_state(state),
        )
        .fallback(fallback_route);

    let ports = tls::Ports {
        http: 8080,
        https: 8443,
    };
    let config = tls::tls_config(ports).await;
    let addr = SocketAddr::from(([0, 0, 0, 0], ports.https));
    info!("Listening on {}", addr);
    axum_server::bind_rustls(addr, config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
