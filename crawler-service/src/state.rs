use std::sync::Arc;

use config::{Config, Environment};
use qdrant_client::Qdrant;
use serde::Deserialize;
use utils::database::{init_mongo, init_qdrant};

use crate::proto::{embed_client::EmbedClient, messaging_client::MessagingClient};

pub const APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

#[derive(Clone)]
pub struct AppState {
    pub redis_client: redis::Client,
    pub reqwest_client: reqwest::Client,
    pub qdrant_client: Arc<Qdrant>,
    pub mongo_client: mongodm::mongo::Client,
    pub tei_client: EmbedClient<tonic::transport::Channel>,
    pub messaging_client: MessagingClient<tonic::transport::Channel>,
}

#[derive(Deserialize)]
struct AppConfig {
    pub redis_uri: String,
    pub qdrant_uri_write: String,
    pub mongo_uri_write: String,
    pub tei_uri: String,
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

        tracing::info!("Initializing Redis client");
        let redis_client = redis::Client::open(app_config.redis_uri).unwrap();

        tracing::info!("Initializing Reqwest client");
        let reqwest_client = reqwest::Client::builder()
            .user_agent(APP_USER_AGENT)
            .build()
            .unwrap();

        let tei_client = EmbedClient::connect(app_config.tei_uri).await.unwrap();
        let messaging_client = MessagingClient::connect(app_config.messaging_uri)
            .await
            .unwrap();

        Self {
            redis_client,
            reqwest_client,
            qdrant_client: Arc::new(init_qdrant(&app_config.qdrant_uri_write).await),
            mongo_client: init_mongo(&app_config.mongo_uri_write).await.unwrap(),
            tei_client,
            messaging_client,
        }
    }
}
