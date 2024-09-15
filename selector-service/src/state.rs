use config::{Config, Environment};
use serde::Deserialize;

use crate::proto::{crawler_client::CrawlerClient, messaging_client::MessagingClient};

#[derive(Clone)]
pub struct AppState {
    pub crawler_client: CrawlerClient<tonic::transport::Channel>,
    pub messaging_client: MessagingClient<tonic::transport::Channel>,
}

#[derive(Deserialize)]
struct AppConfig {
    pub crawler_uri: String,
    pub messaging_uri: String,
}

impl AppState {
    pub async fn new() -> Self {
        let env = Environment::default().ignore_empty(true);

        let config = Config::builder()
            .add_source(env)
            .build()
            .expect("Failed to build configuration");

        let app_config: AppConfig = config
            .try_deserialize()
            .expect("Failed to deserialize configuration");

        tracing::info!("Connected to Crawler Service");
        let crawler_client = CrawlerClient::connect(app_config.crawler_uri)
            .await
            .unwrap();

        tracing::info!("Connected to Messaging Service");
        let messaging_client = MessagingClient::connect(app_config.messaging_uri)
            .await
            .unwrap();

        Self {
            crawler_client,
            messaging_client,
        }
    }
}
