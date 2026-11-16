//! Raft log storage on the doris-ingress WAL.
//!
//! Entries are MessagePack-encoded into WAL frames. Vote and purge state live in a `raft-meta`
//! container file, replaced atomically through the WAL IO thread so it is ordered with log
//! writes.
//!
//! `purge` is logical first: the purged id is recorded and reported, but segments are only
//! deleted below the state machine's *retention floor* — the oldest entry whose rows are still
//! buffered. The state machine rebuilds its memtables from those entries after a restart.

use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::{Entry, LogId, TypeConfig, Vote, decode, encode};
use crate::wal::format::{FileKind, Frame, FrameKind};
use crate::wal::{Wal, read_container_file};

const META_FILE: &str = "raft-meta";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct RaftMeta {
    vote: Option<Vote>,
    purged: Option<LogId>,
}

/// Oldest log index the state machine still needs (`u64::MAX` when none).
#[derive(Clone, Debug)]
pub struct RetentionFloor(Arc<AtomicU64>);

impl Default for RetentionFloor {
    fn default() -> Self {
        Self(Arc::new(AtomicU64::new(u64::MAX)))
    }
}

impl RetentionFloor {
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
    pub fn set(&self, v: u64) {
        self.0.store(v, Ordering::Release)
    }
}

#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Inner>,
}

struct Inner {
    wal: Arc<Wal>,
    meta: Mutex<RaftMeta>,
    last: Mutex<Option<LogId>>,
    floor: RetentionFloor,
}

impl LogStore {
    pub fn open(wal: Arc<Wal>, floor: RetentionFloor) -> io::Result<Self> {
        let path = wal.dir().join(META_FILE);
        let meta = if path.exists() {
            let (_, frames) = read_container_file(&path)?;
            match frames.iter().find(|f| f.kind == FrameKind::RaftMeta) {
                Some(f) => decode(&f.body)?,
                None => RaftMeta::default(),
            }
        } else {
            RaftMeta::default()
        };
        let mut last = meta.purged;
        if let Some((_, last_index)) = wal.range() {
            let body = wal.read(last_index)?.ok_or_else(|| io::Error::other("last entry vanished"))?;
            let entry: Entry = decode(&body)?;
            if last.as_ref().is_none_or(|p| entry.log_id.index > p.index) {
                last = Some(entry.log_id);
            }
        }
        Ok(Self { inner: Arc::new(Inner { wal, meta: Mutex::new(meta), last: Mutex::new(last), floor }) })
    }

    async fn persist_meta(&self) -> io::Result<()> {
        let body = encode(&*self.inner.meta.lock())?;
        let frame = Frame::new(FrameKind::RaftMeta, 0, body);
        self.inner.wal.write_file(META_FILE, FileKind::RaftMeta, &[frame]).await
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let start = match range.start_bound() {
            Bound::Included(s) => *s,
            Bound::Excluded(s) => s + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(e) => e + 1,
            Bound::Excluded(e) => *e,
            Bound::Unbounded => u64::MAX,
        };
        let purged_next = self.inner.meta.lock().purged.as_ref().map(|p| p.index + 1).unwrap_or(0);
        let start = start.max(purged_next);
        if start >= end {
            return Ok(Vec::new());
        }
        self.inner.wal.read_range(start, end)?.iter().map(|(_, b)| decode(b)).collect()
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.inner.meta.lock().vote)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let purged = self.inner.meta.lock().purged;
        let last = (*self.inner.last.lock()).or(purged);
        Ok(LogState { last_purged_log_id: purged, last_log_id: last })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        self.inner.meta.lock().vote = Some(*vote);
        self.persist_meta().await
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut items = Vec::new();
        let mut last = None;
        for e in entries {
            items.push((e.log_id.index, encode(&e)?));
            last = Some(e.log_id);
        }
        if let Some(last) = last {
            *self.inner.last.lock() = Some(last);
        }
        self.inner.wal.append(items, Box::new(move |r| callback.io_completed(r)))
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        let from = last_log_id.as_ref().map(|l| l.index + 1).unwrap_or(0);
        self.inner.wal.truncate_from(from).await?;
        let purged = self.inner.meta.lock().purged;
        let mut last = self.inner.last.lock();
        let keep = match last_log_id {
            // Truncating beyond the end removes nothing.
            Some(l) if last.as_ref().is_some_and(|cur| cur.index <= l.index) => *last,
            Some(l) => Some(l),
            None => purged,
        };
        *last = keep;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        let index = log_id.index;
        self.inner.meta.lock().purged = Some(log_id);
        self.persist_meta().await?;
        {
            let mut last = self.inner.last.lock();
            if last.as_ref().is_none_or(|l| l.index < index) {
                *last = Some(log_id);
            }
        }
        // Keep entries the state machine still needs for rebuilding its memtables.
        let floor = self.inner.floor.get();
        if floor == 0 {
            return Ok(());
        }
        self.inner.wal.purge_upto(index.min(floor - 1)).await
    }
}
