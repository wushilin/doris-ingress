use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, LOCATION};
use reqwest::{Client, StatusCode, redirect};
use serde::Deserialize;

use super::{LoadState, LoadUnit, Loader};
use crate::error::{Error, Result};
use crate::schema::{LOAD_DELETE_COLUMN, SeqTarget, TableDef};

#[derive(Clone, Debug)]
pub struct StreamLoadConfig {
    /// FE HTTP base URL, e.g. `http://fe:8030`. Used for label-state lookups, and for loads
    /// unless `be_url` is set.
    pub fe_url: String,
    /// Optional BE HTTP base URL, e.g. `http://be:8040`. The FE answers a load with a redirect
    /// only after the body has been sent, so going to a BE directly avoids uploading twice.
    pub be_url: Option<String>,
    pub user: String,
    pub password: String,
    /// Per-request timeout, covering upload and publish.
    pub timeout: std::time::Duration,
}

/// Loads units with Stream Load (`PUT /api/{db}/{table}/_stream_load`).
///
/// Redirects from FE to BE are followed by hand so the credentials are re-sent to the BE
/// (an HTTP client drops `Authorization` on a cross-host redirect).
pub struct StreamLoader {
    cfg: StreamLoadConfig,
    http: Client,
}

const MAX_REDIRECTS: usize = 3;

impl StreamLoader {
    pub fn new(cfg: StreamLoadConfig) -> Result<Self> {
        crate::tls::ensure_crypto_provider();
        let http = Client::builder()
            .redirect(redirect::Policy::none())
            .timeout(cfg.timeout)
            .build()
            .map_err(|e| Error::Http(e.to_string()))?;
        Ok(Self { cfg, http })
    }

    fn headers(unit: &LoadUnit) -> Result<HeaderMap> {
        let mut h = HeaderMap::new();
        let mut put = |k: &'static str, v: &str| -> Result<()> {
            let v = HeaderValue::from_str(v).map_err(|e| Error::Http(format!("header {k}: {e}")))?;
            h.insert(k, v);
            Ok(())
        };
        put("label", &unit.label)?;
        put("format", "parquet")?;
        put("columns", &unit.columns.join(","))?;
        put("merge_type", "MERGE")?;
        put("delete", &format!("{LOAD_DELETE_COLUMN}=1"))?;
        if unit.table.seq == SeqTarget::Hidden {
            put("function_column.sequence_col", unit.table.load_seq_column())?;
        }
        put("Expect", "100-continue")?;
        Ok(h)
    }

    async fn put_following_redirects(&self, unit: &LoadUnit) -> Result<StreamLoadResponse> {
        let t = &unit.table;
        let base = self.cfg.be_url.as_deref().unwrap_or(&self.cfg.fe_url);
        let mut url = format!("{base}/api/{}/{}/_stream_load", t.db, t.name);
        let headers = Self::headers(unit)?;
        for _ in 0..=MAX_REDIRECTS {
            let resp = self
                .http
                .put(&url)
                .basic_auth(&self.cfg.user, Some(&self.cfg.password))
                .headers(headers.clone())
                .body(unit.parquet.clone())
                .send()
                .await
                .map_err(|e| Error::Http(format!("stream load {}: {e}", unit.label)))?;
            let status = resp.status();
            if status.is_redirection() {
                url = resp
                    .headers()
                    .get(LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| Error::Http(format!("redirect {status} without Location")))?
                    .to_string();
                continue;
            }
            let body = resp.text().await.map_err(|e| Error::Http(e.to_string()))?;
            if status != StatusCode::OK {
                return Err(Error::Http(format!("stream load {}: HTTP {status}: {body}", unit.label)));
            }
            return serde_json::from_str(&body)
                .map_err(|e| Error::Http(format!("stream load {}: bad response {body}: {e}", unit.label)));
        }
        Err(Error::Http(format!("stream load {}: too many redirects", unit.label)))
    }
}

#[derive(Debug, Deserialize)]
struct StreamLoadResponse {
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "Message", default)]
    message: String,
    #[serde(rename = "ExistingJobStatus", default)]
    existing_job_status: String,
    #[serde(rename = "ErrorURL", default)]
    error_url: String,
}

#[derive(Debug, Deserialize)]
struct LoadStateResponse {
    #[serde(default)]
    data: serde_json::Value,
    #[serde(default)]
    msg: String,
}

#[async_trait]
impl Loader for StreamLoader {
    fn kind(&self) -> &'static str {
        "stream"
    }

    async fn submit(&self, unit: &LoadUnit) -> Result<LoadState> {
        let r = self.put_following_redirects(unit).await?;
        Ok(match r.status.as_str() {
            "Success" => LoadState::Visible,
            // Committed; visibility is checked through the label.
            "Publish Timeout" => LoadState::Running,
            "Label Already Exists" => match r.existing_job_status.as_str() {
                "FINISHED" | "VISIBLE" => LoadState::Visible,
                _ => LoadState::Running,
            },
            _ => {
                let mut msg = format!("{}: {}", r.status, r.message);
                if !r.error_url.is_empty() {
                    msg.push_str(&format!(" (see {})", r.error_url));
                }
                LoadState::Failed(msg)
            }
        })
    }

    async fn state(&self, table: &TableDef, label: &str) -> Result<LoadState> {
        let url = format!("{}/api/{}/get_load_state", self.cfg.fe_url, table.db);
        let resp = self
            .http
            .get(&url)
            .query(&[("label", label)])
            .basic_auth(&self.cfg.user, Some(&self.cfg.password))
            .send()
            .await
            .map_err(|e| Error::Http(format!("get_load_state {label}: {e}")))?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| Error::Http(e.to_string()))?;
        if status != StatusCode::OK {
            return Err(Error::Http(format!("get_load_state {label}: HTTP {status}: {body}")));
        }
        let r: LoadStateResponse = serde_json::from_str(&body)
            .map_err(|e| Error::Http(format!("get_load_state {label}: bad response {body}: {e}")))?;
        if r.msg.eq_ignore_ascii_case("error") {
            // Doris reports a label it has never seen as an error ("Label [...] has not found").
            let detail = r.data.as_str().unwrap_or_default();
            if detail.contains("not found") {
                return Ok(LoadState::Unknown);
            }
            return Err(Error::Http(format!("get_load_state {label}: {detail}")));
        }
        let state = r.data.as_str().unwrap_or_default().to_ascii_uppercase();
        Ok(match state.as_str() {
            "VISIBLE" => LoadState::Visible,
            "PREPARE" | "PRECOMMITTED" | "COMMITTED" => LoadState::Running,
            "ABORTED" => LoadState::Failed("transaction aborted".into()),
            "UNKNOWN" => LoadState::Unknown,
            _ => return Err(Error::Http(format!("get_load_state {label}: unexpected {body} ({})", r.msg))),
        })
    }
}
