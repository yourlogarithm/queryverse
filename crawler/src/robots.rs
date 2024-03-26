use crate::{redis::Key, state, APP_USER_AGENT};
use anyhow::Context;
use deadpool_redis::redis::AsyncCommands;
use robotstxt::DefaultMatcher;
use tracing::debug;

pub async fn is_robots_allowed(url: &url::Url, state: &state::State) -> anyhow::Result<bool> {
    let mut conn = state
        .redis_pool
        .get()
        .await
        .with_context(|| "Failed to establish redis connection")
        .unwrap();
    let domain = url
        .domain()
        .ok_or_else(|| anyhow::anyhow!("Missing domain for {url}"))?;
    let key = Key::Robots(domain);
    let content = if let Some(content) = conn
        .get::<_, Option<String>>(&key)
        .await
        .with_context(|| "Failed to GET from redis")?
    {
        content
    } else {
        let content = state
            .reqwest_client
            .get(format!("{domain}/robots.txt"))
            .send()
            .await
            .with_context(|| "Failed to GET robots.txt")?
            .text()
            .await
            .with_context(|| "Failed to read robots.txt")?;
        deadpool_redis::redis::pipe()
            .atomic()
            .set(&key, &content)
            .expire(&key, 60 * 60 * 24 * 30)
            .query_async(&mut conn)
            .await
            .with_context(|| "Failed to SET and EXPIRE robots.txt in redis")?;
        debug!("Cached robots.txt for {domain}");
        content
    };
    let mut matcher = DefaultMatcher::default();
    Ok(matcher.one_agent_allowed_by_robots(&content, APP_USER_AGENT, url.as_str()))
}
