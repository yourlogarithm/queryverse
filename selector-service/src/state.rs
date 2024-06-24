use lapin::Connection;
use tracing::debug;

#[derive(Clone)]
pub struct AppState {
    pub redis_client: redis::Client,
    pub reqwest_client: reqwest::Client,
    pub amqp_channel: lapin::Channel,
}

impl AppState {
    pub async fn new() -> Self {
        debug!("Connecting to Redis");
        let redis_client = redis::Client::open(env!("REDIS_URI")).unwrap();

        debug!("Creating reqwest client");
        let reqwest_client = reqwest::Client::builder().build().unwrap();

        debug!("Connecting to AMQP");
        let connection = Connection::connect(env!("AMQP_URI"), Default::default())
            .await
            .unwrap();
        let amqp_channel = connection.create_channel().await.unwrap();

        Self {
            redis_client,
            reqwest_client,
            amqp_channel,
        }
    }
}
