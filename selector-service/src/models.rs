use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct QueueInfo {
    pub name: String,
    #[serde(default)]
    pub messages: u32,
}