use crate::state::{self, APP_USER_AGENT};
use anyhow::Context;
use redis::AsyncCommands;
use robotstxt::DefaultMatcher;
use tracing::debug;
use utils::redis::Key;

pub async fn is_robots_allowed(url: &url::Url, state: &state::AppState) -> anyhow::Result<bool> {
    let mut conn = state
        .redis_client
        .get_multiplexed_async_connection()
        .await
        .context("redis connection")
        .unwrap();
    let domain = url
        .domain()
        .ok_or_else(|| anyhow::anyhow!("Missing domain for {url}"))?;
    let key = Key::Robots(domain);
    if let Some(allowed) = conn
        .get::<_, Option<u8>>(&key)
        .await
        .context("Redis GET")?
    {
        debug!("Using cached robots.txt for {domain}");
        return Ok(allowed == 1);
    }
    let scheme = url.scheme();
    let robots_url = format!("{}://{}/robots.txt", scheme, domain);
    let robots_url = url::Url::parse(&robots_url).context("domain parsing")?;
    let content = state
        .reqwest_client
        .get(robots_url)
        .send()
        .await
        .context("http GET")?
        .text()
        .await
        .context("robots.txt content")?;
    let mut matcher = DefaultMatcher::default();
    let allowed = matcher.one_agent_allowed_by_robots(&content, APP_USER_AGENT, url.as_str());
    redis::pipe()
        .atomic()
        .set(&key, if allowed { 1 } else { 0 })
        .expire(&key, 60 * 60 * 24 * 30)
        .query_async(&mut conn)
        .await
        .context("Redis SET & EXPIRE")?;
    debug!("Cached robots.txt for {domain}");
    Ok(allowed)
}
