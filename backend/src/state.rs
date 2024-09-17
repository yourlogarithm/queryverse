use std::sync::Arc;

use config::{Config, Environment};
use mongodb::Client;
use qdrant_client::Qdrant;
use serde::Deserialize;
use crate::proto::embed_client::EmbedClient;

#[derive(Clone)]
pub struct AppState {
    pub mongo_client: Client,
    pub qdrant_client: Arc<Qdrant>,
    pub tei_client: EmbedClient<tonic::transport::Channel>
}

impl AppState {
    pub async fn new(config: AppConfig) -> Self {
        let mongo_client = Client::with_uri_str(&config.mongo_uri_read)
            .await
            .expect("Failed to connect to MongoDB");
        let qdrant_client = Arc::new(
            Qdrant::from_url(&config.qdrant_uri_read)
                .build()
                .expect("Failed to connect to Qdrant"),
        );
        let tei_client = EmbedClient::connect(config.tei_uri).await.unwrap();
        Self {
            mongo_client,
            qdrant_client,
            tei_client
        }
    }
}

#[derive(Deserialize)]
pub struct AppConfig {
    pub mongo_uri_read: String,
    pub qdrant_uri_read: String,
    pub tei_uri: String,
}

impl AppConfig {
    pub fn new() -> Self {
        let env = Environment::default().ignore_empty(true);

        let config = Config::builder()
            .add_source(env)
            .build()
            .expect("Failed to build configuration");

        config
            .try_deserialize()
            .expect("Failed to deserialize configuration")
    }
}