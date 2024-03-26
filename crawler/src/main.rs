mod crawl;
mod redis;
mod robots;
mod state;

use axum::{
    http::StatusCode,
    // http::{HeaderValue, StatusCode, Method},
    // response::IntoResponse,
    routing::{get, post},
    Json,
    Router,
};
use deadpool_redis::{Config, Runtime};
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

    let cfg = Config::from_url(env::var("REDIS_URL").unwrap());
    let redis_pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    let reqwest_client = reqwest::Client::builder()
        .user_agent(APP_USER_AGENT)
        .build()
        .unwrap();

    let state = State {
        redis_pool,
        reqwest_client,
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
