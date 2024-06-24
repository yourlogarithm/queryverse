use anyhow::Context;
use lapin::options::BasicNackOptions;
use tracing::{debug, error, warn};

use crate::state::AppState;

const ENDPOINT: &str = concat!(env!("CRAWLER_API"), "/v1/crawl");

#[tracing::instrument(skip(app_state))]
pub async fn consume(domain: String, app_state: AppState) -> anyhow::Result<()> {
    debug!("Getting message from {domain}");
    if let Some(msg) = app_state
        .amqp_channel
        .basic_get(&domain, Default::default())
        .await
        .context("basic get")?
    {
        debug!("Got message {msg:?} from {domain}");
        let url = match String::from_utf8(msg.delivery.data.clone()) {
            Ok(url) => url,
            Err(e) => {
                error!("Bad payload - {e:#}");
                msg.delivery
                    .reject(Default::default())
                    .await
                    .context("basic reject")?;
                return Ok(());
            }
        };
        debug!("Sending POST");
        if let Err(e) = app_state
            .reqwest_client
            .post(ENDPOINT)
            .body(url)
            .send()
            .await
        {
            error!("Failed to send POST request - {e:#}");
            msg.delivery
                .nack(BasicNackOptions {
                    requeue: true,
                    multiple: false,
                })
                .await
                .context("basic nack")
        } else {
            msg.delivery
                .ack(Default::default())
                .await
                .context("basick ack")
        }
    } else {
        warn!("No message in {domain}");
        Ok(())
    }
}
