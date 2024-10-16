use proto::{CrawlRequest, Url};
use state::AppState;

mod state;

mod proto {
    tonic::include_proto!("crawler");
    tonic::include_proto!("messaging");
}

async fn listen() {
    let AppState {
        mut messaging_client,
        mut crawler_client,
    } = state::AppState::new().await;
    let mut stream = messaging_client.subscribe(()).await.unwrap().into_inner();
    while let Some(Url { url }) = stream.message().await.unwrap() {
        tracing::info!(url = %url, "Received message");
        if let Err(e) = crawler_client.crawl(CrawlRequest { url }).await {
            tracing::error!(error = %e, "Failed to crawl");
        }
    }
}

#[tokio::main]
async fn main() {
    utils::start(env!("CARGO_PKG_NAME"), Box::pin(listen())).await;
}
