use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

fn opts(dir: &Path) -> WalOptions {
    WalOptions { dir: dir.to_path_buf(), segment_bytes: 300 }
}

fn body(i: u64) -> Bytes {
    Bytes::from(format!("entry-{i}-{}", "x".repeat((i % 7) as usize * 10)))
}

async fn append(wal: &Wal, range: std::ops::Range<u64>) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    wal.append(range.map(|i| (i, body(i))).collect(), Box::new(move |r| tx.send(r).unwrap())).unwrap();
    rx.await.unwrap().unwrap();
}

fn check(wal: &Wal, range: std::ops::Range<u64>) {
    let got = wal.read_range(range.start, range.end).unwrap();
    assert_eq!(got.len() as u64, range.end - range.start);
    for (i, b) in got {
        assert_eq!(b, body(i), "entry {i}");
    }
}

fn segment_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn appends_roll_segments_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (wal, rec) = Wal::open(opts(dir.path())).unwrap();
        assert_eq!(rec.entries, 0);
        append(&wal, 1..20).await;
        append(&wal, 20..40).await;
        check(&wal, 1..40);
        assert_eq!(wal.range(), Some((1, 39)));
    }
    assert!(segment_files(dir.path()).len() > 3, "small segments roll over");
    let (wal, rec) = Wal::open(opts(dir.path())).unwrap();
    assert_eq!(rec.entries, 39);
    assert_eq!(rec.truncated_tail_bytes, 0);
    assert!(rec.writer_builds.iter().all(|b| b.starts_with("doris-ingress ")));
    check(&wal, 1..40);
    append(&wal, 40..45).await;
    check(&wal, 1..45);
}

#[tokio::test]
async fn queued_appends_are_group_committed_and_acknowledged_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(WalOptions::new(dir.path())).unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut tx = Some(tx);
    for i in 1..=200u64 {
        let (order, done) = (order.clone(), done.clone());
        let last = if i == 200 { tx.take() } else { None };
        wal.append(
            vec![(i, body(i))],
            Box::new(move |r| {
                r.unwrap();
                order.lock().push(i);
                done.fetch_add(1, Ordering::SeqCst);
                if let Some(tx) = last {
                    tx.send(()).unwrap();
                }
            }),
        )
        .unwrap();
        // Entries are readable as soon as they are submitted.
        assert_eq!(wal.read(i).unwrap().unwrap(), body(i));
    }
    rx.await.unwrap();
    assert_eq!(*order.lock(), (1..=200).collect::<Vec<_>>());
    check(&wal, 1..201);
}

#[tokio::test]
async fn rejects_gaps_and_out_of_order_appends() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(opts(dir.path())).unwrap();
    append(&wal, 5..8).await;
    assert!(wal.append(vec![(9, body(9))], Box::new(|_| {})).is_err());
    assert!(wal.append(vec![(8, body(8)), (10, body(10))], Box::new(|_| {})).is_err());
}

#[tokio::test]
async fn torn_tail_is_truncated_and_log_continues() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (wal, _) = Wal::open(opts(dir.path())).unwrap();
        append(&wal, 1..10).await;
    }
    let last = segment_files(dir.path()).pop().unwrap();
    let good_len = fs::metadata(&last).unwrap().len();
    // A crash mid-write leaves part of a frame behind.
    let partial = Frame::new(FrameKind::RaftEntry, 10, body(10)).encode();
    let mut f = OpenOptions::new().append(true).open(&last).unwrap();
    f.write_all(&partial[..partial.len() / 2]).unwrap();
    drop(f);

    let (wal, rec) = Wal::open(opts(dir.path())).unwrap();
    assert_eq!(rec.truncated_tail_bytes, (partial.len() / 2) as u64);
    assert_eq!(fs::metadata(&last).unwrap().len(), good_len);
    check(&wal, 1..10);
    append(&wal, 10..12).await;
    check(&wal, 1..12);
}

#[tokio::test]
async fn bit_flip_in_last_segment_cuts_the_log_there() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (wal, _) = Wal::open(WalOptions::new(dir.path())).unwrap();
        append(&wal, 1..6).await;
    }
    let seg = segment_files(dir.path()).pop().unwrap();
    let mut bytes = fs::read(&seg).unwrap();
    let n = bytes.len();
    bytes[n - 10] ^= 0x01; // inside the last frame
    fs::write(&seg, &bytes).unwrap();
    let (wal, rec) = Wal::open(WalOptions::new(dir.path())).unwrap();
    assert!(rec.truncated_tail_bytes > 0);
    assert_eq!(wal.range(), Some((1, 4)));
}

#[tokio::test]
async fn corruption_in_an_older_segment_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (wal, _) = Wal::open(opts(dir.path())).unwrap();
        append(&wal, 1..30).await;
    }
    let first = segment_files(dir.path()).remove(0);
    let mut bytes = fs::read(&first).unwrap();
    let n = bytes.len();
    bytes[n - 3] ^= 0xff;
    fs::write(&first, &bytes).unwrap();
    let err = Wal::open(opts(dir.path())).err().expect("open must fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
}

#[tokio::test]
async fn read_detects_corruption_after_open() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(WalOptions::new(dir.path())).unwrap();
    append(&wal, 1..4).await;
    let seg = segment_files(dir.path()).pop().unwrap();
    let mut bytes = fs::read(&seg).unwrap();
    let n = bytes.len();
    bytes[n - 6] ^= 0x10;
    fs::write(&seg, &bytes).unwrap();
    assert!(wal.read(3).is_err());
    assert_eq!(wal.read(1).unwrap().unwrap(), body(1));
}

#[tokio::test]
async fn truncate_after_across_segments_then_append_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (wal, _) = Wal::open(opts(dir.path())).unwrap();
        append(&wal, 1..40).await;
        wal.truncate_from(13).await.unwrap();
        assert_eq!(wal.range(), Some((1, 12)));
        assert!(wal.read(13).unwrap().is_none());
        append(&wal, 13..20).await;
        check(&wal, 1..20);
    }
    let (wal, rec) = Wal::open(opts(dir.path())).unwrap();
    assert_eq!(rec.truncated_tail_bytes, 0);
    assert_eq!(wal.range(), Some((1, 19)));
    check(&wal, 1..20);
}

#[tokio::test]
async fn truncate_everything() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(opts(dir.path())).unwrap();
    append(&wal, 3..10).await;
    wal.truncate_from(3).await.unwrap();
    assert_eq!(wal.range(), None);
    append(&wal, 0..5).await;
    check(&wal, 0..5);
}

#[tokio::test]
async fn purge_removes_whole_segments_only() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(opts(dir.path())).unwrap();
    append(&wal, 1..40).await;
    let before = segment_files(dir.path()).len();
    wal.purge_upto(20).await.unwrap();
    let (first, last) = wal.range().unwrap();
    assert!(first > 1 && first <= 21, "first={first}");
    assert_eq!(last, 39);
    assert!(segment_files(dir.path()).len() < before);
    check(&wal, first..40);
    drop(wal);
    let (wal, _) = Wal::open(opts(dir.path())).unwrap();
    assert_eq!(wal.range(), Some((first, 39)));
}

#[tokio::test]
async fn purge_beyond_the_end_empties_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(opts(dir.path())).unwrap();
    append(&wal, 1..10).await;
    wal.purge_upto(100).await.unwrap();
    assert_eq!(wal.range(), None);
    assert!(wal.append(vec![(50, body(50))], Box::new(|_| {})).is_err());
    append(&wal, 101..103).await;
    check(&wal, 101..103);
}

#[tokio::test]
async fn metadata_file_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = Wal::open(WalOptions::new(dir.path())).unwrap();
    let frame = Frame::new(FrameKind::RaftMeta, 0, Bytes::from_static(b"meta"));
    wal.write_file("raft-meta", FileKind::RaftMeta, std::slice::from_ref(&frame)).await.unwrap();
    let (h, frames) = read_container_file(&dir.path().join("raft-meta")).unwrap();
    assert_eq!(h.kind, FileKind::RaftMeta);
    assert_eq!(frames, vec![frame]);
}
