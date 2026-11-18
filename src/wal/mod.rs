//! Segmented write-ahead log with group commit.
//!
//! * Records are [`format::Frame`]s (CRC32C each) in segment files named by their first index.
//! * One IO thread executes all mutations in submission order. Consecutive appends are written
//!   together and made durable with a single `fdatasync`; their callbacks run only after that.
//! * Truncate, purge and metadata writes go through the same thread, so they are never reordered
//!   with appends submitted before them (a vote write never overtakes an earlier log write).
//! * Reads take the in-memory index and `pread` the segment, verifying the CRC on every read.
//!   Entries submitted but not yet written are served from memory.
//! * On open, the last segment's torn tail (a partial or corrupt suffix from a crash during a
//!   write) is truncated. Damage anywhere else is reported as corruption.

pub mod format;

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::JoinHandle;

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;

use format::{FileHeader, FileKind, FormatError, Frame, FrameKind};

pub type Callback = Box<dyn FnOnce(io::Result<()>) + Send>;

#[derive(Clone, Debug)]
pub struct WalOptions {
    pub dir: PathBuf,
    /// Start a new segment once the current one exceeds this size.
    pub segment_bytes: u64,
}

impl WalOptions {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into(), segment_bytes: 100 << 20 }
    }
}

/// What `open` found and repaired.
#[derive(Clone, Debug, Default)]
pub struct Recovery {
    pub segments: usize,
    pub entries: u64,
    /// Bytes cut from the end of the last segment (torn write).
    pub truncated_tail_bytes: u64,
    /// Build tags of the writers of the segments found.
    pub writer_builds: Vec<String>,
}

pub struct Wal {
    shared: Arc<Shared>,
    tx: Mutex<Sender<Cmd>>,
    io: Option<JoinHandle<()>>,
}

struct Shared {
    opts: WalOptions,
    index: Mutex<Index>,
}

#[derive(Default)]
struct Index {
    /// Index of `locs[0]` (or of the next append if the log is empty).
    first: u64,
    /// Written frames, contiguous from `first`.
    locs: VecDeque<Loc>,
    /// Frames submitted to the IO thread but not yet written; contiguous after `locs`.
    pending: VecDeque<Bytes>,
    segments: BTreeMap<u64, Arc<Segment>>,
    /// True once any entry has been appended or recovered; before that any start index is allowed.
    started: bool,
    /// Set after an IO error; the WAL refuses further work.
    failed: Option<String>,
}

impl Index {
    fn next(&self) -> u64 {
        self.first + self.locs.len() as u64 + self.pending.len() as u64
    }
}

#[derive(Clone, Copy)]
struct Loc {
    segment: u64,
    offset: u64,
    len: u32,
}

struct Segment {
    path: PathBuf,
    file: File,
}

enum Cmd {
    Append { frames: Vec<Bytes>, first: u64, done: Callback },
    Truncate { from: u64, done: Callback },
    Purge { upto: u64, done: Callback },
    WriteFile { name: String, contents: Bytes, done: Callback },
    Shutdown,
}

fn segment_name(first: u64) -> String {
    format!("{first:020}.wal")
}

fn other(msg: impl Into<String>) -> io::Error {
    io::Error::other(msg.into())
}

fn corrupt(path: &Path, offset: u64, e: FormatError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{}: offset {offset}: {e}", path.display()))
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

impl Wal {
    pub fn open(opts: WalOptions) -> io::Result<(Self, Recovery)> {
        fs::create_dir_all(&opts.dir)?;
        let (index, active, recovery) = recover(&opts)?;
        let shared = Arc::new(Shared { opts, index: Mutex::new(index) });
        let (tx, rx) = channel();
        let io_shared = shared.clone();
        let io = std::thread::Builder::new()
            .name("wal-io".into())
            .spawn(move || IoThread { shared: io_shared, active }.run(rx))?;
        Ok((Self { shared, tx: Mutex::new(tx), io: Some(io) }, recovery))
    }

    pub fn dir(&self) -> &Path {
        &self.shared.opts.dir
    }

    /// First and last index present, if any.
    pub fn range(&self) -> Option<(u64, u64)> {
        let idx = self.shared.index.lock();
        let next = idx.next();
        (next > idx.first).then(|| (idx.first, next - 1))
    }

    /// Submits entries for durable append. `entries` must start at the next index (any index if
    /// the log is empty) and be contiguous. `done` runs on the IO thread once they are durable.
    pub fn append(&self, entries: Vec<(u64, Bytes)>, done: Callback) -> io::Result<()> {
        if entries.is_empty() {
            done(Ok(()));
            return Ok(());
        }
        let mut idx = self.shared.index.lock();
        if let Some(f) = &idx.failed {
            return Err(other(format!("wal failed earlier: {f}")));
        }
        let first = entries[0].0;
        let empty = idx.locs.is_empty() && idx.pending.is_empty();
        if empty {
            if idx.started && first != idx.first {
                return Err(other(format!("append at {first}, expected {}", idx.first)));
            }
            idx.first = first;
        } else if first != idx.next() {
            return Err(other(format!("append at {first}, expected {}", idx.next())));
        }
        let mut frames = Vec::with_capacity(entries.len());
        for (i, (index, body)) in entries.into_iter().enumerate() {
            if index != first + i as u64 {
                return Err(other(format!("non-contiguous append at {index}")));
            }
            frames.push(Frame::new(FrameKind::RaftEntry, index, body).encode());
        }
        idx.started = true;
        idx.pending.extend(frames.iter().cloned());
        self.tx.lock().send(Cmd::Append { frames, first, done }).map_err(|_| other("wal io thread stopped"))
    }

    /// Reads entries `[start, end)` that are present. Stops at the first missing index.
    pub fn read_range(&self, start: u64, end: u64) -> io::Result<Vec<(u64, Bytes)>> {
        // Each entry is read either from a segment (written) or from memory (still pending).
        type Source = Result<(Arc<Segment>, Loc), Bytes>;
        let mut plan: Vec<(u64, Source)> = Vec::new();
        {
            let idx = self.shared.index.lock();
            let start = start.max(idx.first);
            let end = end.min(idx.next());
            for i in start..end {
                let off = (i - idx.first) as usize;
                if off < idx.locs.len() {
                    let loc = idx.locs[off];
                    let seg = idx.segments.get(&loc.segment).cloned().ok_or_else(|| other("segment missing"))?;
                    plan.push((i, Ok((seg, loc))));
                } else {
                    plan.push((i, Err(idx.pending[off - idx.locs.len()].clone())));
                }
            }
        }
        let mut out = Vec::with_capacity(plan.len());
        for (i, src) in plan {
            let raw = match src {
                Ok((seg, loc)) => {
                    let mut buf = vec![0u8; loc.len as usize];
                    seg.file.read_exact_at(&mut buf, loc.offset)?;
                    Bytes::from(buf)
                }
                Err(bytes) => bytes,
            };
            let (frame, _) = Frame::decode(&raw).map_err(|e| other(format!("entry {i}: {e}")))?;
            if frame.index != i {
                return Err(other(format!("entry {i}: frame holds index {}", frame.index)));
            }
            out.push((i, frame.body));
        }
        Ok(out)
    }

    pub fn read(&self, index: u64) -> io::Result<Option<Bytes>> {
        Ok(self.read_range(index, index + 1)?.pop().map(|(_, b)| b))
    }

    /// Removes every entry at or after `from`. Removing everything lets the next append start at
    /// any index.
    pub async fn truncate_from(&self, from: u64) -> io::Result<()> {
        self.call(|done| Cmd::Truncate { from, done }).await
    }

    /// Deletes segments whose entries are all `<= upto`. If `upto` covers the whole log, the log
    /// becomes empty and the next append must be at `upto + 1`.
    pub async fn purge_upto(&self, upto: u64) -> io::Result<()> {
        self.call(|done| Cmd::Purge { upto, done }).await
    }

    /// Atomically replaces a small file in the WAL directory with a container file holding
    /// `frames`. Ordered after (and durable after) every earlier append.
    pub async fn write_file(&self, name: &str, kind: FileKind, frames: &[Frame]) -> io::Result<()> {
        let mut b = BytesMut::new();
        b.extend_from_slice(&FileHeader::new(kind, 0).encode());
        for f in frames {
            f.encode_into(&mut b);
        }
        let contents = b.freeze();
        let name = name.to_string();
        self.call(|done| Cmd::WriteFile { name, contents, done }).await
    }

    async fn call(&self, make: impl FnOnce(Callback) -> Cmd) -> io::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let done: Callback = Box::new(move |r| {
            let _ = tx.send(r);
        });
        self.tx.lock().send(make(done)).map_err(|_| other("wal io thread stopped"))?;
        rx.await.map_err(|_| other("wal io thread dropped request"))?
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        let _ = self.tx.lock().send(Cmd::Shutdown);
        if let Some(h) = self.io.take() {
            let _ = h.join();
        }
    }
}

/// Reads a container file written by [`Wal::write_file`] (or a snapshot file).
pub fn read_container_file(path: &Path) -> io::Result<(FileHeader, Vec<Frame>)> {
    let buf = fs::read(path)?;
    let (header, mut pos) = FileHeader::decode(&buf).map_err(|e| corrupt(path, 0, e))?;
    let mut frames = Vec::new();
    while pos < buf.len() {
        let (f, n) = Frame::decode(&buf[pos..]).map_err(|e| corrupt(path, pos as u64, e))?;
        frames.push(f);
        pos += n;
    }
    Ok((header, frames))
}

/// Writes a container file atomically (temp file, fsync, rename, fsync dir) outside the WAL
/// IO thread. Used for snapshot files, which are not ordered with log writes.
pub fn write_container_file(path: &Path, kind: FileKind, frames: &[Frame]) -> io::Result<()> {
    let mut b = BytesMut::new();
    b.extend_from_slice(&FileHeader::new(kind, 0).encode());
    for f in frames {
        f.encode_into(&mut b);
    }
    replace_file(path, &b)
}

/// Writes a container file frame by frame, for content that should not be held in memory at
/// once (a snapshot being received). The file is complete only after [`Self::finish`]; give it
/// its final name with [`publish_file`].
pub struct ContainerWriter {
    file: File,
    buf: BytesMut,
}

impl ContainerWriter {
    pub fn create(path: &Path, kind: FileKind) -> io::Result<Self> {
        let mut file = File::create(path)?;
        file.write_all(&FileHeader::new(kind, 0).encode())?;
        Ok(Self { file, buf: BytesMut::new() })
    }

    pub fn append(&mut self, frame: &Frame) -> io::Result<()> {
        self.buf.clear();
        frame.encode_into(&mut self.buf);
        self.file.write_all(&self.buf)
    }

    /// Makes the content durable.
    pub fn finish(self) -> io::Result<()> {
        self.file.sync_all()
    }
}

/// Renames a finished file to its final name, durably.
pub fn publish_file(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)?;
    sync_dir(to.parent().unwrap_or(Path::new(".")))
}

fn replace_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    sync_dir(path.parent().unwrap_or(Path::new(".")))
}

/// Scans the segments, truncating a torn tail of the last one.
fn recover(opts: &WalOptions) -> io::Result<(Index, Option<Active>, Recovery)> {
    let mut firsts: Vec<u64> = Vec::new();
    for entry in fs::read_dir(&opts.dir)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if let Some(stem) = name.strip_suffix(".wal")
            && let Ok(first) = stem.parse::<u64>()
        {
            firsts.push(first);
        }
    }
    firsts.sort_unstable();
    let mut idx = Index::default();
    let mut rec = Recovery::default();
    let mut active = None;
    let count = firsts.len();
    for (n, first) in firsts.into_iter().enumerate() {
        let last_segment = n + 1 == count;
        let path = opts.dir.join(segment_name(first));
        let buf = fs::read(&path)?;
        let header = match FileHeader::decode(&buf) {
            Ok((h, len)) => (h, len),
            Err(e) if last_segment => {
                // Crash while creating the segment: nothing in it was ever acknowledged.
                tracing::warn!(path = %path.display(), error = %e, "removing last segment with unreadable header");
                rec.truncated_tail_bytes += buf.len() as u64;
                fs::remove_file(&path)?;
                sync_dir(&opts.dir)?;
                continue;
            }
            Err(e) => return Err(corrupt(&path, 0, e)),
        };
        let (h, mut pos) = header;
        if h.kind != FileKind::LogSegment || h.first_index != first {
            return Err(other(format!("{}: header says {:?} first_index {}", path.display(), h.kind, h.first_index)));
        }
        if idx.started && first != idx.next() {
            return Err(other(format!("{}: gap, expected first index {}", path.display(), idx.next())));
        }
        if !idx.started {
            idx.first = first;
            idx.started = true;
        }
        rec.writer_builds.push(h.build);
        let mut expected = first;
        while pos < buf.len() {
            match Frame::decode(&buf[pos..]) {
                Ok((f, len)) if f.index == expected && f.kind == FrameKind::RaftEntry => {
                    idx.locs.push_back(Loc { segment: first, offset: pos as u64, len: len as u32 });
                    expected += 1;
                    pos += len;
                }
                Ok((f, _)) if !last_segment => {
                    return Err(other(format!("{}: frame index {} kind {:?}, expected {expected}", path.display(), f.index, f.kind)));
                }
                Err(e) if !last_segment => return Err(corrupt(&path, pos as u64, e)),
                other_result => {
                    let cut = buf.len() - pos;
                    tracing::warn!(
                        path = %path.display(), offset = pos, bytes = cut,
                        reason = ?other_result.err(),
                        "truncating torn tail of last WAL segment"
                    );
                    rec.truncated_tail_bytes += cut as u64;
                    let f = OpenOptions::new().write(true).open(&path)?;
                    f.set_len(pos as u64)?;
                    f.sync_all()?;
                    break;
                }
            }
        }
        rec.entries += expected - first;
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        idx.segments.insert(first, Arc::new(Segment { path: path.clone(), file }));
        if last_segment {
            let write = OpenOptions::new().append(true).open(&path)?;
            active = Some(Active { first, file: write, len: pos as u64 });
        }
        rec.segments += 1;
    }
    Ok((idx, active, rec))
}

struct Active {
    first: u64,
    file: File,
    len: u64,
}

struct IoThread {
    shared: Arc<Shared>,
    active: Option<Active>,
}

impl IoThread {
    fn run(mut self, rx: Receiver<Cmd>) {
        let mut next: Option<Cmd> = None;
        loop {
            let cmd = match next.take() {
                Some(c) => c,
                None => match rx.recv() {
                    Ok(c) => c,
                    Err(_) => return,
                },
            };
            match cmd {
                Cmd::Append { frames, first, done } => {
                    // Group commit: take every append already queued behind this one.
                    let mut batch = vec![(frames, first, done)];
                    while let Ok(c) = rx.try_recv() {
                        match c {
                            Cmd::Append { frames, first, done } => batch.push((frames, first, done)),
                            other => {
                                next = Some(other);
                                break;
                            }
                        }
                    }
                    self.write_batch(batch);
                }
                Cmd::Truncate { from, done } => {
                    let r = self.guard(|t| t.truncate(from));
                    done(r);
                }
                Cmd::Purge { upto, done } => {
                    let r = self.guard(|t| t.purge(upto));
                    done(r);
                }
                Cmd::WriteFile { name, contents, done } => {
                    let path = self.shared.opts.dir.join(name);
                    let r = self.guard(|_| replace_file(&path, &contents));
                    done(r);
                }
                Cmd::Shutdown => return,
            }
        }
    }

    /// Runs a mutation unless the WAL already failed; marks it failed on error.
    fn guard(&mut self, f: impl FnOnce(&mut Self) -> io::Result<()>) -> io::Result<()> {
        if let Some(e) = &self.shared.index.lock().failed {
            return Err(other(format!("wal failed earlier: {e}")));
        }
        let r = f(self);
        if let Err(e) = &r {
            self.shared.index.lock().failed = Some(e.to_string());
        }
        r
    }

    fn write_batch(&mut self, batch: Vec<(Vec<Bytes>, u64, Callback)>) {
        let mut written: Vec<(u64, Loc)> = Vec::new();
        let result = self.guard(|t| {
            let mut buf = BytesMut::new();
            let mut buf_start: Option<(u64, u64)> = None; // (segment, offset) where buf begins
            for (frames, first, _) in &batch {
                for (i, frame) in frames.iter().enumerate() {
                    let index = first + i as u64;
                    let roll = match &t.active {
                        None => true,
                        Some(a) => a.len > 0 && a.len + frame.len() as u64 > t.shared.opts.segment_bytes && a.first != index,
                    };
                    if roll {
                        t.flush_buf(&mut buf, &mut buf_start)?;
                        if let Some(a) = &t.active {
                            a.file.sync_data()?;
                        }
                        t.new_segment(index)?;
                    }
                    let a = t.active.as_mut().expect("segment opened above");
                    if buf_start.is_none() {
                        buf_start = Some((a.first, a.len));
                    }
                    written.push((index, Loc { segment: a.first, offset: a.len, len: frame.len() as u32 }));
                    a.len += frame.len() as u64;
                    buf.extend_from_slice(frame);
                }
            }
            t.flush_buf(&mut buf, &mut buf_start)?;
            if let Some(a) = &t.active {
                a.file.sync_data()?;
            }
            Ok(())
        });
        if result.is_ok() {
            let mut idx = self.shared.index.lock();
            for (index, loc) in written {
                debug_assert_eq!(index, idx.first + idx.locs.len() as u64);
                idx.pending.pop_front();
                idx.locs.push_back(loc);
            }
        }
        for (_, _, done) in batch {
            done(match &result {
                Ok(()) => Ok(()),
                Err(e) => Err(other(e.to_string())),
            });
        }
    }

    fn flush_buf(&mut self, buf: &mut BytesMut, start: &mut Option<(u64, u64)>) -> io::Result<()> {
        if let (Some(_), Some(a)) = (start.take(), self.active.as_mut()) {
            a.file.write_all(buf)?;
        }
        buf.clear();
        Ok(())
    }

    fn new_segment(&mut self, first: u64) -> io::Result<()> {
        let dir = self.shared.opts.dir.clone();
        let path = dir.join(segment_name(first));
        let header = FileHeader::new(FileKind::LogSegment, first).encode();
        {
            let mut f = OpenOptions::new().create_new(true).write(true).open(&path)?;
            f.write_all(&header)?;
            f.sync_all()?;
        }
        sync_dir(&dir)?;
        let read = OpenOptions::new().read(true).write(true).open(&path)?;
        let write = OpenOptions::new().append(true).open(&path)?;
        self.shared.index.lock().segments.insert(first, Arc::new(Segment { path, file: read }));
        self.active = Some(Active { first, file: write, len: header.len() as u64 });
        Ok(())
    }

    fn truncate(&mut self, from: u64) -> io::Result<()> {
        let dir = self.shared.opts.dir.clone();
        let mut idx = self.shared.index.lock();
        debug_assert!(idx.pending.is_empty(), "appends before a truncate are written first");
        let next = idx.next();
        if from >= next {
            return Ok(());
        }
        if from <= idx.first {
            // Everything goes.
            for (_, seg) in std::mem::take(&mut idx.segments) {
                fs::remove_file(&seg.path)?;
            }
            idx.locs.clear();
            idx.first = from;
            idx.started = false;
            self.active = None;
            drop(idx);
            return sync_dir(&dir);
        }
        let keep = (from - idx.first) as usize;
        let cut = idx.locs[keep];
        let doomed: Vec<u64> = idx.segments.range(cut.segment + 1..).map(|(k, _)| *k).collect();
        for k in doomed {
            if let Some(seg) = idx.segments.remove(&k) {
                fs::remove_file(&seg.path)?;
            }
        }
        let seg = idx.segments.get(&cut.segment).cloned().ok_or_else(|| other("segment missing"))?;
        seg.file.set_len(cut.offset)?;
        seg.file.sync_all()?;
        idx.locs.truncate(keep);
        let write = OpenOptions::new().append(true).open(&seg.path)?;
        self.active = Some(Active { first: cut.segment, file: write, len: cut.offset });
        drop(idx);
        sync_dir(&dir)
    }

    fn purge(&mut self, upto: u64) -> io::Result<()> {
        let dir = self.shared.opts.dir.clone();
        let mut idx = self.shared.index.lock();
        debug_assert!(idx.pending.is_empty());
        let next = idx.next();
        if upto + 1 >= next {
            for (_, seg) in std::mem::take(&mut idx.segments) {
                fs::remove_file(&seg.path)?;
            }
            idx.locs.clear();
            idx.first = idx.first.max(upto + 1).max(next);
            idx.started = true;
            self.active = None;
            drop(idx);
            return sync_dir(&dir);
        }
        // Remove whole segments that end at or before `upto`. A segment ends where the next begins.
        let firsts: Vec<u64> = idx.segments.keys().copied().collect();
        let mut removed = false;
        for w in firsts.windows(2) {
            let (seg_first, next_first) = (w[0], w[1]);
            if next_first - 1 <= upto {
                if let Some(seg) = idx.segments.remove(&seg_first) {
                    fs::remove_file(&seg.path)?;
                    removed = true;
                }
                let drop_n = (next_first - idx.first) as usize;
                idx.locs.drain(..drop_n);
                idx.first = next_first;
            } else {
                break;
            }
        }
        drop(idx);
        if removed { sync_dir(&dir) } else { Ok(()) }
    }
}

#[cfg(test)]
mod tests;
