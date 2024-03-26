#[derive(Clone)]
pub struct State {
    pub redis_pool: deadpool_redis::Pool,
    pub reqwest_client: reqwest::Client,
    pub amqp_channel: lapin::Channel,
}