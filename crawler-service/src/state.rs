use std::sync::Arc;

use deadpool_redis::{Config, Runtime};
use dotenvy::dotenv;
use qdrant_client::client::QdrantClient;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::database::{init_mongo, init_qdrant};

pub const APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

#[derive(Clone)]
pub struct AppState {
    pub redis_pool: deadpool_redis::Pool,
    pub reqwest_client: reqwest::Client,
    // pub amqp_channel: lapin::Channel,
    pub qdrant_client: Arc<QdrantClient>,
    pub mongo_client: mongodm::mongo::Client,
}

impl AppState {
    pub async fn new() -> Self {
        dotenv().ok();
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or("INFO".into()))
            .with(tracing_subscriber::fmt::layer().without_time())
            .init();

        let cfg = Config::from_url(env!("REDIS_URI"));
        let redis_pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

        let reqwest_client = reqwest::Client::builder()
            .user_agent(APP_USER_AGENT)
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();

        Self {
            redis_pool,
            reqwest_client,
            // amqp_channel,
            qdrant_client: Arc::new(init_qdrant().await),
            mongo_client: init_mongo().await.unwrap(),
        }
    }
}
