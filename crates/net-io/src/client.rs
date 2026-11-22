use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use crate::error::GatewayError;
use bytes::Bytes;
use reqwest::{Client, header};

const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_MAX_RETRIES: u32 = 3;
const BACKOFF_BASE_MS: u64 = 1000;
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

/// Upper bound for pooled clients. Calls typically use one or two distinct
/// configurations; the cap only protects against caller-provided timeouts
/// churning the cache.
const MAX_CACHED_CLIENTS: usize = 8;

#[derive(Clone, PartialEq, Eq, Hash)]
struct ClientKey {
    timeout_ms: u64,
    proxy: Option<String>,
    no_proxy: bool,
}

static CLIENT_CACHE: LazyLock<Mutex<HashMap<ClientKey, Client>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Return a pooled `reqwest::Client` for the given transport configuration.
///
/// Reusing clients keeps HTTP/2 and TLS connections alive across Python calls
/// (notably MinerU polling), which otherwise pays a fresh handshake per call.
fn cached_client(
    timeout_ms: Option<u64>,
    proxy: Option<&str>,
    no_proxy: bool,
) -> Result<Client, GatewayError> {
    let key = ClientKey {
        timeout_ms: timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS),
        proxy: proxy.map(str::to_owned),
        no_proxy,
    };
    let mut cache = CLIENT_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if let Some(client) = cache.get(&key) {
        return Ok(client.clone());
    }

    let client = build_client(key.timeout_ms, key.proxy.as_deref(), no_proxy)?;
    if cache.len() >= MAX_CACHED_CLIENTS {
        cache.clear();
    }
    cache.insert(key, client.clone());
    Ok(client)
}

fn build_client(
    timeout_ms: u64,
    proxy: Option<&str>,
    no_proxy: bool,
) -> Result<Client, GatewayError> {
    let mut builder = Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .connect_timeout(Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS))
        .user_agent(USER_AGENT)
        .default_headers(default_headers())
        .gzip(true)
        .redirect(reqwest::redirect::Policy::limited(10));

    if no_proxy {
        builder = builder.no_proxy();
    }

    if let Some(proxy_url) = proxy {
        let proxy = reqwest::Proxy::all(proxy_url)
            .map_err(|e| GatewayError::Other(format!("invalid proxy: {e}")))?;
        builder = builder.proxy(proxy);
    }

    Ok(builder.build()?)
}

fn retry_backoff(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(20);
    Duration::from_millis(BACKOFF_BASE_MS * (1u64 << shift))
}

/// Build default headers that mimic a standard browser to avoid 403 from
/// academic publishers and DOI resolvers.
fn default_headers() -> header::HeaderMap {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::ACCEPT,
        header::HeaderValue::from_static(
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        ),
    );
    headers.insert(
        header::ACCEPT_LANGUAGE,
        header::HeaderValue::from_static("en-US,en;q=0.9"),
    );
    headers.insert(
        header::ACCEPT_ENCODING,
        header::HeaderValue::from_static("gzip, deflate"),
    );
    headers
}

#[derive(Clone)]
pub struct HttpClient {
    inner: Client,
    max_retries: u32,
}

impl HttpClient {
    pub fn new(
        timeout_ms: Option<u64>,
        max_retries: Option<u32>,
        proxy: Option<&str>,
    ) -> Result<Self, GatewayError> {
        Ok(Self {
            inner: cached_client(timeout_ms, proxy, false)?,
            max_retries: max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
        })
    }

    /// Create a client that explicitly ignores system proxy settings.
    pub fn new_no_proxy(
        timeout_ms: Option<u64>,
        max_retries: Option<u32>,
    ) -> Result<Self, GatewayError> {
        Ok(Self {
            inner: cached_client(timeout_ms, None, true)?,
            max_retries: max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
        })
    }

    /// GET a URL, retrying transport failures and non-2xx responses.
    async fn get_ok(&self, url: &str) -> Result<reqwest::Response, GatewayError> {
        let attempts = self.max_retries.max(1);
        let mut last_err: Option<GatewayError> = None;

        for attempt in 1..=attempts {
            match self.inner.get(url).send().await {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    last_err = Some(GatewayError::Http(
                        response
                            .error_for_status()
                            .expect_err("response was checked to be non-success"),
                    ));
                }
                Err(err) => last_err = Some(GatewayError::Http(err)),
            }

            if attempt < attempts {
                tokio::time::sleep(retry_backoff(attempt)).await;
            }
        }

        Err(last_err.unwrap_or_else(|| GatewayError::Other(format!("GET {url} failed"))))
    }

    pub async fn get_json(
        &self,
        url: &str,
        query: &serde_json::Value,
    ) -> Result<serde_json::Value, GatewayError> {
        let url = self.build_url(url, query)?;
        let json = self.get_ok(url.as_str()).await?.json().await?;
        Ok(json)
    }

    pub async fn get_text(&self, url: &str) -> Result<String, GatewayError> {
        let text = self.get_ok(url).await?.text().await?;
        Ok(text)
    }

    /// GET raw bytes with retry. Returns (bytes, final_url, status_code).
    ///
    /// On transport-level failures (DNS, connection, TLS) after all retries,
    /// returns `(Bytes::new(), url, 0)` so callers can branch on status code
    /// instead of catching exceptions.
    pub async fn get_bytes(&self, url: &str) -> Result<(Bytes, String, u16), GatewayError> {
        let attempts = self.max_retries.max(1);
        let mut last_status: u16 = 0;

        for attempt in 1..=attempts {
            if let Ok(resp) = self.inner.get(url).send().await {
                let status = resp.status().as_u16();
                let final_url = resp.url().to_string();
                if resp.status().is_success() {
                    let bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| GatewayError::Other(format!("bytes read failed: {e}")))?;
                    return Ok((bytes, final_url, status));
                }
                last_status = status;
            }
            if attempt < attempts {
                tokio::time::sleep(retry_backoff(attempt)).await;
            }
        }

        if last_status > 0 {
            // HTTP error status — return it so caller can branch on status code.
            Ok((Bytes::new(), url.to_string(), last_status))
        } else {
            // Transport failure — return status 0 so caller sees non-success
            // without exception handling.
            Ok((Bytes::new(), url.to_string(), 0))
        }
    }

    /// POST JSON with optional Authorization header.
    pub async fn post_json(
        &self,
        url: &str,
        body: &serde_json::Value,
        auth_header: Option<&str>,
    ) -> Result<serde_json::Value, GatewayError> {
        let mut request = self
            .inner
            .post(url)
            .header("Content-Type", "application/json")
            .header("Accept", "*/*");

        if let Some(auth) = auth_header {
            request = request.header("Authorization", auth);
        }

        let response = request.json(body).send().await?;
        let json = response.error_for_status()?.json().await?;
        Ok(json)
    }

    /// PUT bytes with an optional Content-Type header, returning an empty JSON object.
    pub async fn put_bytes(
        &self,
        url: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<serde_json::Value, GatewayError> {
        let mut request = self.inner.put(url).body(bytes);

        if let Some(value) = content_type {
            request = request.header("Content-Type", value);
        }

        request.send().await?.error_for_status()?;
        Ok(serde_json::json!({}))
    }

    /// GET with optional Authorization header, returning JSON.
    pub async fn get_json_with_auth(
        &self,
        url: &str,
        auth_header: Option<&str>,
    ) -> Result<serde_json::Value, GatewayError> {
        let mut request = self.inner.get(url);

        if let Some(auth) = auth_header {
            request = request.header("Authorization", auth);
        }

        let response = request.send().await?;
        let json = response.error_for_status()?.json().await?;
        Ok(json)
    }

    fn build_url(&self, base: &str, query: &serde_json::Value) -> Result<url::Url, GatewayError> {
        let mut url = url::Url::parse(base)?;
        if let Some(obj) = query.as_object() {
            for (key, value) in obj {
                if let Some(s) = value.as_str() {
                    url.query_pairs_mut().append_pair(key, s);
                } else if let Some(j) = value.as_i64() {
                    url.query_pairs_mut().append_pair(key, &j.to_string());
                } else if let Some(j) = value.as_u64() {
                    url.query_pairs_mut().append_pair(key, &j.to_string());
                }
            }
        }
        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_caps_large_attempt_numbers() {
        assert_eq!(retry_backoff(1), Duration::from_millis(BACKOFF_BASE_MS));
        assert_eq!(
            retry_backoff(21),
            Duration::from_millis(BACKOFF_BASE_MS * (1u64 << 20))
        );
        assert_eq!(
            retry_backoff(32),
            Duration::from_millis(BACKOFF_BASE_MS * (1u64 << 20))
        );
    }

    #[test]
    fn build_url_percent_encodes_query_values_once() {
        let client = HttpClient::new(None, None, None).unwrap();
        let doi = "10.1002/(SICI)1097-0258(19980815)17:15<1661::AID-SIM889>3.0.CO;2-2";
        let url = client
            .build_url(
                "https://api.openalex.org/works",
                &serde_json::json!({ "filter": format!("doi:{doi}") }),
            )
            .unwrap();
        let query = url.query().unwrap();

        assert!(query.contains("filter=doi%3A10.1002%2F%28SICI%29"));
        assert!(query.contains("%3C1661%3A%3AAID-SIM889%3E"));
        assert!(!query.contains("%25"));
    }
}
