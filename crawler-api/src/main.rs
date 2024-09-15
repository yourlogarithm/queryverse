mod core;
mod database;
mod robots;
mod state;

use bson::doc;
use mongodm::{f, prelude::GreaterThan, ToRepository};
use proto::{
    crawler_server::{Crawler, CrawlerServer},
    CrawlRequest,
};
use tonic::{transport::Server, Code, Request, Response, Status};

use crate::{
    core::process,
    database::{Document, DATABASE},
    robots::is_robots_allowed,
    state::AppState,
};

mod proto {
    tonic::include_proto!("crawler");
    tonic::include_proto!("tei.v1");
    tonic::include_proto!("messaging");
}

struct CrawlerService {
    state: AppState,
}

#[tonic::async_trait]
impl Crawler for CrawlerService {
    async fn crawl(&self, request: Request<CrawlRequest>) -> Result<Response<()>, Status> {
        let CrawlRequest { url } = request.into_inner();
        let url = match reqwest::Url::parse(&url) {
            Err(e) => {
                tracing::error!(error = %e, "Failed to parse URL");
                return Err(Status::new(Code::InvalidArgument, e.to_string()));
            }
            Ok(url) => url,
        };
        tracing::info!(url = %url, "Received crawl request");

        match is_robots_allowed(&url, &self.state).await {
            Ok(true) => {
                tracing::info!(url = %url, "Robots.txt allows crawling");
                let repo = self
                    .state
                    .mongo_client
                    .database(DATABASE)
                    .repository::<Document>();
                let count_options = mongodm::mongo::options::CountOptions::builder()
                    .hint(mongodm::mongo::options::Hint::Keys(
                        doc! {f!(url in Document): 1},
                    ))
                    .limit(1)
                    .build();
                let past = chrono::Utc::now() - chrono::Duration::hours(1);
                let filter = doc! {
                    f!(url in Document): url.as_str(),
                    f!(last in Document): { GreaterThan: past }
                };
                match repo
                    .count_documents(filter)
                    .with_options(count_options)
                    .await
                {
                    Ok(0) => (),
                    Ok(_) => {
                        tracing::info!(url = %url, "URL already crawled within the last hour");
                        return Ok(Response::new(()));
                    }
                    Err(e) => {
                        tracing::error!(error = %e, url = %url, "Failed to check if URL was already crawled");
                        return Err(Status::new(Code::Internal, e.to_string()));
                    }
                }
                let process_result = process(url.clone(), &self.state).await;
                match process_result {
                    Ok(_) => {
                        tracing::info!(url = %url, "Successfully crawled URL");
                        Ok(Response::new(()))
                    }
                    Err(e) => {
                        tracing::error!(error = %e, url = %url, "Failed to process URL");
                        Err(Status::new(Code::Internal, e.to_string()))
                    }
                }
            }
            Ok(false) => {
                tracing::info!(url = %url, "Robots.txt disallows crawling");
                Ok(Response::new(()))
            }
            Err(e) => {
                tracing::error!(error = %e, url = %url, "Failed to check robots.txt");
                Err(Status::new(Code::Internal, e.to_string()))
            }
        }
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
