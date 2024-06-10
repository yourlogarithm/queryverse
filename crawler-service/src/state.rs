use std::sync::Arc;

use deadpool_redis::{Config, Runtime};
use dotenvy::dotenv;
use lapin::{options::QueueDeclareOptions, types::FieldTable, Connection, ConnectionProperties};
use qdrant_client::{
    client::QdrantClient,
    qdrant::{
        vectors_config::Config as QConfig, CreateCollection, Distance, VectorParams, VectorsConfig,
    },
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub const APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

#[derive(Clone)]
pub struct AppState {
    pub redis_pool: deadpool_redis::Pool,
    pub reqwest_client: reqwest::Client,
    pub amqp_channel: lapin::Channel,
    pub qdrant_client: Arc<QdrantClient>,
}

impl AppState {
    async fn init_qdrant(qdrant_client: &QdrantClient) {
        qdrant_client
            .create_collection(&CreateCollection {
                collection_name: "documents".to_string(),
                vectors_config: Some(VectorsConfig {
                    config: Some(QConfig::Params(VectorParams {
                        size: 768,
                        distance: Distance::Cosine.into(),
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    pub async fn new() -> Self {
        dotenv().ok();
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or("INFO".into()))
            .with(tracing_subscriber::fmt::layer())
            .init();

        let cfg = Config::from_url(
            std::env::var("REDIS_URI").unwrap_or("redis://localhost:6379".to_string()),
        );
        let redis_pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

        let reqwest_client = reqwest::Client::builder()
            .user_agent(APP_USER_AGENT)
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();

        let options = ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);

        let connection = Connection::connect(
            &std::env::var("AMQP_URI").unwrap_or("amqp://localhost:5672".to_string()),
            options,
        )
        .await
        .unwrap();
        let amqp_channel = connection.create_channel().await.unwrap();

        let _queue = amqp_channel
            .queue_declare(
                "crawled_urls",
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();

        let qdrant_client = Arc::new(
            QdrantClient::from_url(
                &std::env::var("QDRANT_URI").unwrap_or("http://localhost:6334".to_string()),
            )
            .build()
            .unwrap(),
        );

        Self::init_qdrant(&qdrant_client).await;

        Self {
            redis_pool,
            reqwest_client,
            amqp_channel,
            qdrant_client,
        }
    }
}
