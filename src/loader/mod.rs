//! Loading flush units into Doris.
//!
//! Every load carries a deterministic label. The flusher records an intent before submitting and
//! only treats a unit as done once [`Loader::state`] reports [`LoadState::Visible`] for its label.

mod broker;
mod sdk;
mod stream;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

pub use broker::{BrokerLoadConfig, BrokerLoader, S3Config};
pub use sdk::{SdkLoadConfig, SdkLoader};
pub use stream::{StreamLoadConfig, StreamLoader};

use crate::error::Result;
use crate::schema::TableDef;

/// State of a load label in Doris.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoadState {
    /// Doris has no record of the label.
    Unknown,
    /// Accepted but not yet visible (Stream Load PREPARE/COMMITTED, Broker Load PENDING/LOADING…).
    Running,
    /// Data is visible to queries. The only state that completes a flush.
    Visible,
    /// The load ended without making data visible. The label may be reused.
    Failed(String),
}

/// One flush unit, encoded once as Parquet.
#[derive(Clone, Debug)]
pub struct LoadUnit {
    pub table: Arc<TableDef>,
    pub label: String,
    /// Columns in file order: table columns, the sequence column, the delete flag.
    pub columns: Vec<String>,
    pub parquet: Bytes,
    pub rows: usize,
    pub seq_start: u64,
    pub seq_end: u64,
}

#[async_trait]
pub trait Loader: Send + Sync {
    /// Short name recorded in the intent log ("stream", "broker", …).
    fn kind(&self) -> &'static str;

    /// Submits the unit under its label and returns the state Doris reported. Submitting a label
    /// that already exists must not fail: return the state of the existing job instead.
    async fn submit(&self, unit: &LoadUnit) -> Result<LoadState>;

    /// Looks up the state of a label.
    async fn state(&self, table: &TableDef, label: &str) -> Result<LoadState>;

    /// Called once the label is visible or abandoned, to release staging resources.
    async fn cleanup(&self, _table: &TableDef, _label: &str) -> Result<()> {
        Ok(())
    }
}

/// Deterministic label for a flush unit. Doris labels allow `[-_A-Za-z0-9:]`, at most 128 chars.
pub fn make_label(table: &TableDef, seq_start: u64, seq_end: u64) -> String {
    let clean = |s: &str| -> String {
        s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' }).collect()
    };
    let suffix = format!("_{seq_start}_{seq_end}");
    let mut head = format!("ingress_{}_{}", clean(&table.db), clean(&table.name));
    head.truncate(128 - suffix.len());
    head + &suffix
}

/// Quotes an identifier for Doris SQL.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Quotes a string literal for Doris SQL.
pub(crate) fn quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}
