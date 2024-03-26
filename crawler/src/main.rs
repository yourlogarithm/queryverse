#[macro_use]
extern crate lazy_static;

mod crawl;
mod redis;
mod robots;
mod state;

use axum::{
    http::StatusCode,
    // http::{HeaderValue, StatusCode, Method},
    // response::IntoResponse,
    routing::get,
    Json,
    Router,
};
use deadpool_redis::{Config, Runtime};
use lapin::{options::QueueDeclareOptions, types::FieldTable, Connection, ConnectionProperties};
use std::env;
use tracing::warn;

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
    tracing_subscriber::fmt::init();

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
            QueueDeclareOptions { durable: true, ..QueueDeclareOptions::default() },
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
        // .layer(
        //     CorsLayer::new()
        //         .allow_origin("http://localhost:3000").parse::<HeaderValue>().unwrap()
        //         .allow_methods([Method::GET])
        // )
        .fallback(fallback_route);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8000")
        .await
        .unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
