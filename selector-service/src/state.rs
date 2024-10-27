use rabbitmq_management_client::{config::RabbitMqConfiguration, RabbitMqClient, RabbitMqClientBuilder};
use serde::Deserialize;

use crate::proto::crawler_client::CrawlerClient;

#[derive(Clone)]
pub struct AppState {
    pub crawler_client: CrawlerClient<tonic::transport::Channel>,
    pub redis_client: redis::Client,
    pub management_client: RabbitMqClient,
    pub amqp_channel: lapin::Channel,
}

#[derive(Deserialize)]
pub struct AppConfig {
    pub crawler_uri: String,
    pub rabbitmq_api_url: String,
    pub amqp_usr: String,
    pub amqp_pwd: String,
    pub redis_uri: String,
    pub amqp_uri: String,
    pub selector_concurrent: usize,
}

impl AppState {
    pub async fn new(config: AppConfig) -> Self {
        tracing::debug!("Connected to Crawler Service");
        let crawler_client = CrawlerClient::connect(config.crawler_uri).await.unwrap();

        let management_config = RabbitMqConfiguration {
            rabbitmq_api_url: config.rabbitmq_api_url,
            rabbitmq_username: config.amqp_usr,
            rabbitmq_password: config.amqp_pwd,
        };
        let management_client = RabbitMqClientBuilder::new(management_config).build().unwrap();
        let redis_client = redis::Client::open(config.redis_uri).unwrap();

        let options = lapin::ConnectionProperties::default();
        // .with_executor(tokio_executor_trait::Tokio::current())
        // .with_reactor(tokio_reactor_trait::Tokio);
        let connection = lapin::Connection::connect(&config.amqp_uri, options)
            .await
            .unwrap();
        let amqp_channel = connection.create_channel().await.unwrap();

        Self {
            crawler_client,
            redis_client,
            management_client,
            amqp_channel
        }
    }
}
