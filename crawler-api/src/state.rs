use std::sync::Arc;

use lapin::{Connection, ConnectionProperties};
use qdrant_client::client::QdrantClient;
use tracing::debug;

use crate::database::{init_mongo, init_qdrant};

pub const APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

#[derive(Clone)]
pub struct AppState {
    pub redis_client: redis::Client,
    pub reqwest_client: reqwest::Client,
    pub amqp_channel: lapin::Channel,
    pub qdrant_client: Arc<QdrantClient>,
    pub mongo_client: mongodm::mongo::Client,
}

impl AppState {
    pub async fn new() -> Self {
        debug!("Initializing Redis client");
        let redis_client = redis::Client::open(env!("REDIS_URI")).unwrap();

        debug!("Initializing Reqwest client");
        let reqwest_client = reqwest::Client::builder()
            .user_agent(APP_USER_AGENT)
            .build()
            .unwrap();

        debug!("Initializing AMQP channel");
        let options = ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);

        let connection = Connection::connect(env!("AMQP_URI"), options)
            .await
            .unwrap();
        let amqp_channel = connection.create_channel().await.unwrap();

        Self {
            redis_client,
            reqwest_client,
            amqp_channel,
            qdrant_client: Arc::new(init_qdrant().await),
            mongo_client: init_mongo().await.unwrap(),
        }
    }
}
