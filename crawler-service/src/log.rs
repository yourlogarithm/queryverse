use std::borrow::Cow;

use serde::Serialize;
use url::Url;

#[derive(Default, Serialize)]
pub struct Content {
    #[serde(rename = "cl")]
    pub content_length: usize,
    #[serde(rename = "bl")]
    pub body_length: usize,
    #[serde(rename = "e")]
    pub edges: usize
}

#[derive(Serialize)]
pub struct Log<'a> {
    #[serde(rename = "u")]
    pub url: Cow<'a, str>,
    #[serde(rename = "d", skip_serializing_if = "Option::is_none")]
    pub domain: Option<Cow<'a, str>>,
    #[serde(rename = "r")]
    pub robots_allows: bool,
    #[serde(rename = "e")]
    pub error: bool,
    #[serde(rename = "c", skip_serializing_if = "Option::is_none")]
    pub data: Option<Content>,
}

impl<'a> Log<'a> {
    pub fn from_url(url: &'a Url, robots_allows: bool) -> Self {
        Self {
            url: Cow::Borrowed(url.as_str()),
            domain: url.domain().map(|d| Cow::Borrowed(d)),
            robots_allows,
            error: false,
            data: None,
        }
    }
}