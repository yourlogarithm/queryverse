use anyhow::Context;
use lapin::options::BasicNackOptions;
use tracing::{debug, error, warn};

use crate::state::AppState;

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
        match std::str::from_utf8(&msg.delivery.data)
            .map(urlencoding::encode)
            .context("data decoding")
        {
            Ok(encoded) => {
                let endpoint = format!("{}/v1/crawl/{encoded}", env!("CRAWLER_API"));
                debug!("Sending POST request to {endpoint}");
                if let Err(e) = app_state.reqwest_client.post(&endpoint).send().await {
                    error!("Failed to send POST request to {endpoint} - {e:#}");
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
            }
            Err(e) => {
                error!("Bad payload {:?} - {e:#}", msg.delivery.data);
                msg.delivery
                    .reject(Default::default())
                    .await
                    .context("basic reject")
            }
        }
    } else {
        warn!("No message in {domain}");
        Ok(())
    }
}
