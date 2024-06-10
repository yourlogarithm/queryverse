#[macro_use]
extern crate lazy_static;

mod crawl;
mod models;
mod redis;
mod robots;
mod state;

use axum::{http::StatusCode, routing::get, Router};
use axum_server::tls_rustls::RustlsConfig;
use state::AppState;

use std::{net::SocketAddr, path::PathBuf};
use tracing::{info, warn};

#[axum::debug_handler]
async fn root() -> StatusCode {
    StatusCode::OK
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
    let state = AppState::new().await;

    let app = Router::new()
        .route("/", get(root))
        .nest(
            "/v1",
            Router::new()
                .route("/url/:url", get(crawl::crawl))
                .with_state(state),
        )
        .fallback(fallback_route);

    let config = RustlsConfig::from_pem_file(
        PathBuf::from("certificates/certificate.pem"),
        PathBuf::from("certificates/privatekey.pem"),
    )
    .await
    .unwrap();
    let addr = SocketAddr::from(([0, 0, 0, 0], 8000));
    info!("Listening on {}", addr);
    axum_server::bind_rustls(addr, config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
