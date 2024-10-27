mod core;
mod log;
mod robots;
mod state;
mod traverse;

use log::Log;
use mongodb::{bson::doc, options::CountOptions};
use mongodm::{f, prelude::GreaterThan, ToRepository};
use proto::{
    crawler_server::{Crawler, CrawlerServer},
    CrawlRequest,
};
use tokio::time::Instant;
use tonic::{transport::Server, Request, Response, Status};
use utils::database::{Page, DATABASE};

use crate::{core::process, robots::is_robots_allowed, state::AppState};

mod proto {
    tonic::include_proto!("crawler");
    tonic::include_proto!("tei.v1");
}

struct CrawlerService {
    state: AppState,
}

#[tonic::async_trait]
impl Crawler for CrawlerService {
    async fn crawl(&self, request: Request<CrawlRequest>) -> Result<Response<()>, Status> {
        let instant = Instant::now();

        let CrawlRequest { url } = request.into_inner();
        let url = match reqwest::Url::parse(&url) {
            Err(e) => {
                tracing::error!(error = %e, "Failed to parse URL");
                return Err(Status::invalid_argument(e.to_string()));
            }
            Ok(url) => url,
        };
        tracing::debug!(url = %url, "Received crawl request");

        let (log, response) = match is_robots_allowed(&url, &self.state).await {
            Ok(true) => {
                tracing::debug!(url = %url, "robots.txt allows crawling");
                let repo = self
                    .state
                    .mongo_client
                    .database(DATABASE)
                    .repository::<Page>();
                let count_options = CountOptions::builder().limit(1).build();
                let past = chrono::Utc::now() - chrono::Duration::hours(1);
                let filter = doc! {
                    f!(url in Page): url.as_str(),
                    f!(last in Page): { GreaterThan: past }
                };
                match repo
                    .count_documents(filter)
                    .with_options(count_options)
                    .await
                {
                    Ok(0) => (),
                    Ok(_) => {
                        tracing::debug!(url = %url, "URL already crawled within the last hour");
                        return Ok(Response::new(()));
                    }
                    Err(e) => {
                        tracing::error!(error = %e, url = %url, "Failed to check if URL was already crawled");
                        return Err(Status::internal(e.to_string()));
                    }
                }
                let mut log = Log::from_url(&url, true);
                match process(&url, &mut log, &self.state).await {
                    Ok(_) => {
                        tracing::debug!(url = %url, "Successfully crawled URL");
                        (log, Ok(Response::new(())))
                    }
                    Err(e) => {
                        tracing::error!(error = %e, url = %url, "Failed to process URL");
                        log.error = true;
                        (log, Err(Status::internal(e.to_string())))
                    }
                }
            }
            Ok(false) => {
                tracing::debug!(url = %url, "Robots.txt disallows crawling");
                (Log::from_url(&url, false), Ok(Response::new(())))
            }
            Err(e) => {
                tracing::error!(error = %e, url = %url, "Failed to check robots.txt");
                let mut log = Log::from_url(&url, false);
                log.error = true;
                (log, Err(Status::internal(e.to_string())))
            }
        };

        tracing::info!(url = %url, "Crawled in {:.2}ms", instant.elapsed().as_millis());

        let logstash_post = self
            .state
            .reqwest_client
            .post(&self.state.logstash_uri)
            .json(&log);

        tokio::spawn(async move {
            if let Err(e) = logstash_post.send().await {
                tracing::error!("Failed to send logstash log: {e:#}");
            }
        });

        response
    }
}

async fn serve() {
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<CrawlerServer<CrawlerService>>()
        .await;

    let state = AppState::new().await;
    let addr = "0.0.0.0:50051".parse().unwrap();
    let crawler = CrawlerService { state };
    let server = CrawlerServer::new(crawler);

    tracing::info!("Serving at {addr}");

    Server::builder()
        .add_service(health_service)
        .add_service(server)
        .serve(addr)
        .await
        .unwrap();
}

#[tokio::main]
async fn main() {
    utils::start(env!("CARGO_PKG_NAME"), Box::pin(serve())).await;
}
