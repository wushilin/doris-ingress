//! WAL binary container format.
//!
//! Every WAL file (log segment, Raft metadata, snapshot) starts with a [`FileHeader`] and holds a
//! sequence of [`Frame`]s. Both are versioned and extensible so the format can change without a
//! migration:
//!
//! * The file header carries the container `format_version`, the `min_reader_version` a reader
//!   must support to read the file, a `header_len` so new fixed fields can be appended (older
//!   readers skip them), the **build tag** of the writer, and a list of TLV extensions.
//! * Each frame carries a `frame_version`, a `kind` (what the body is), a `codec` (how the body is
//!   encoded), `flags` (e.g. compression), an extension block of TLVs, the body, and a CRC32C
//!   over everything before the CRC.
//!
//! Unknown TLV tags are skipped. Unknown frame kinds or codecs, or a `min_reader_version` above
//! what this build supports, are reported as errors rather than silently misread.
//!
//! ```text
//! FileHeader (little endian)
//!   0  [8]  magic "DIWAL\0\0\x01"
//!   8  u16  format_version
//!  10  u16  min_reader_version
//!  12  u32  header_len      total header bytes, including the trailing CRC
//!  16  u8   file_kind       1 = log segment, 2 = raft meta, 3 = snapshot
//!  17  [3]  reserved (0)
//!  20  u32  flags
//!  24  u64  first_index     first log index in the file (segments), 0 otherwise
//!  32  u64  created_unix_ms
//!  40  u16  build_len, then build tag bytes (UTF-8), e.g. "doris-ingress 0.1.0 (git 1a2b3c)"
//!   .. u16  tlv_count, then TLVs: u16 tag, u32 len, bytes
//!   .. (bytes added by newer versions; skipped)
//!   .. u32  crc32c of all preceding header bytes
//!
//! Frame (little endian)
//!   0  u16  magic 0xD1F0
//!   2  u8   frame_version
//!   3  u8   kind
//!   4  u8   codec
//!   5  u8   flags
//!   6  u16  ext_len         bytes of TLV extensions after the fixed part
//!   8  u64  index           log index (0 when not applicable)
//!  16  u32  body_len
//!  20  [ext_len] TLV extensions
//!   .. [body_len] body
//!   .. u32  crc32c of all preceding frame bytes
//! ```

use bytes::{BufMut, Bytes, BytesMut};

pub const FILE_MAGIC: [u8; 8] = *b"DIWAL\0\0\x01";
/// Container version this build writes.
pub const FORMAT_VERSION: u16 = 1;
/// Highest `min_reader_version` this build can read.
pub const SUPPORTED_READER_VERSION: u16 = 1;

pub const FRAME_MAGIC: u16 = 0xD1F0;
pub const FRAME_VERSION: u8 = 1;
const FRAME_FIXED: usize = 20;
const FRAME_TRAILER: usize = 4;
/// Upper bound on one frame body, to reject garbage lengths before allocating.
pub const MAX_BODY: usize = 1 << 30;

/// Build tag written into every file header.
pub fn build_tag() -> String {
    format!("doris-ingress {} (git {})", env!("CARGO_PKG_VERSION"), env!("DORIS_INGRESS_GIT_SHA"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FileKind {
    LogSegment = 1,
    RaftMeta = 2,
    Snapshot = 3,
}

impl FileKind {
    fn from_u8(v: u8) -> Result<Self, FormatError> {
        Ok(match v {
            1 => Self::LogSegment,
            2 => Self::RaftMeta,
            3 => Self::Snapshot,
            other => return Err(FormatError::Unsupported(format!("file kind {other}"))),
        })
    }
}

/// What a frame body holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// One Raft log entry.
    RaftEntry = 1,
    /// Raft metadata (vote, committed, purged).
    RaftMeta = 2,
    /// Snapshot metadata and state.
    SnapshotMeta = 3,
}

impl FrameKind {
    fn from_u8(v: u8) -> Result<Self, FormatError> {
        Ok(match v {
            1 => Self::RaftEntry,
            2 => Self::RaftMeta,
            3 => Self::SnapshotMeta,
            other => return Err(FormatError::Unsupported(format!("frame kind {other}"))),
        })
    }
}

/// How a frame body is encoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Codec {
    /// MessagePack with named fields (serde), so fields can be added compatibly.
    MsgPack = 1,
}

impl Codec {
    fn from_u8(v: u8) -> Result<Self, FormatError> {
        match v {
            1 => Ok(Self::MsgPack),
            other => Err(FormatError::Unsupported(format!("codec {other}"))),
        }
    }
}

/// Optional tagged field in a header or frame. Readers skip tags they do not know.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tlv {
    pub tag: u16,
    pub value: Bytes,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FormatError {
    /// Not enough bytes for a complete header or frame (e.g. a torn write at the tail).
    #[error("truncated: need {need} bytes, have {have}")]
    Truncated { need: usize, have: usize },
    #[error("bad magic")]
    BadMagic,
    #[error("checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    Checksum { stored: u32, computed: u32 },
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("malformed: {0}")]
    Malformed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileHeader {
    pub format_version: u16,
    pub min_reader_version: u16,
    pub kind: FileKind,
    pub flags: u32,
    pub first_index: u64,
    pub created_unix_ms: u64,
    pub build: String,
    pub ext: Vec<Tlv>,
}

impl FileHeader {
    pub fn new(kind: FileKind, first_index: u64) -> Self {
        let created_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            format_version: FORMAT_VERSION,
            min_reader_version: 1,
            kind,
            flags: 0,
            first_index,
            created_unix_ms,
            build: build_tag(),
            ext: Vec::new(),
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(64 + self.build.len());
        b.put_slice(&FILE_MAGIC);
        b.put_u16_le(self.format_version);
        b.put_u16_le(self.min_reader_version);
        b.put_u32_le(0); // header_len, patched below
        b.put_u8(self.kind as u8);
        b.put_slice(&[0; 3]);
        b.put_u32_le(self.flags);
        b.put_u64_le(self.first_index);
        b.put_u64_le(self.created_unix_ms);
        b.put_u16_le(self.build.len() as u16);
        b.put_slice(self.build.as_bytes());
        put_tlvs_counted(&mut b, &self.ext);
        let header_len = (b.len() + 4) as u32;
        b[12..16].copy_from_slice(&header_len.to_le_bytes());
        let crc = crc32c::crc32c(&b);
        b.put_u32_le(crc);
        b.freeze()
    }

    /// Decodes a header from the start of `buf`; returns it and its length.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), FormatError> {
        need(buf, 16)?;
        if buf[..8] != FILE_MAGIC {
            return Err(FormatError::BadMagic);
        }
        let format_version = u16_at(buf, 8);
        let min_reader_version = u16_at(buf, 10);
        if min_reader_version > SUPPORTED_READER_VERSION {
            return Err(FormatError::Unsupported(format!(
                "file needs reader version {min_reader_version}, this build reads up to {SUPPORTED_READER_VERSION}"
            )));
        }
        let header_len = u32_at(buf, 12) as usize;
        if !(44..=1 << 20).contains(&header_len) {
            return Err(FormatError::Malformed(format!("header_len {header_len}")));
        }
        need(buf, header_len)?;
        let stored = u32_at(buf, header_len - 4);
        let computed = crc32c::crc32c(&buf[..header_len - 4]);
        if stored != computed {
            return Err(FormatError::Checksum { stored, computed });
        }
        let body = &buf[..header_len - 4];
        let kind = FileKind::from_u8(body[16])?;
        let flags = u32_at(body, 20);
        let first_index = u64_at(body, 24);
        let created_unix_ms = u64_at(body, 32);
        let mut pos = 40;
        let build_len = u16_at(body, pos) as usize;
        pos += 2;
        let build = body
            .get(pos..pos + build_len)
            .ok_or_else(|| FormatError::Malformed("build tag".into()))?;
        let build = String::from_utf8_lossy(build).into_owned();
        pos += build_len;
        let (ext, _) = read_tlvs_counted(body, pos)?;
        Ok((
            Self { format_version, min_reader_version, kind, flags, first_index, created_unix_ms, build, ext },
            header_len,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub version: u8,
    pub kind: FrameKind,
    pub codec: Codec,
    pub flags: u8,
    pub index: u64,
    pub ext: Vec<Tlv>,
    pub body: Bytes,
}

impl Frame {
    pub fn new(kind: FrameKind, index: u64, body: Bytes) -> Self {
        Self { version: FRAME_VERSION, kind, codec: Codec::MsgPack, flags: 0, index, ext: Vec::new(), body }
    }

    pub fn encoded_len(&self) -> usize {
        FRAME_FIXED + tlvs_len(&self.ext) + self.body.len() + FRAME_TRAILER
    }

    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(self.encoded_len());
        self.encode_into(&mut b);
        b.freeze()
    }

    pub fn encode_into(&self, b: &mut BytesMut) {
        let start = b.len();
        b.put_u16_le(FRAME_MAGIC);
        b.put_u8(self.version);
        b.put_u8(self.kind as u8);
        b.put_u8(self.codec as u8);
        b.put_u8(self.flags);
        b.put_u16_le(tlvs_len(&self.ext) as u16);
        b.put_u64_le(self.index);
        b.put_u32_le(self.body.len() as u32);
        put_tlvs(b, &self.ext);
        b.put_slice(&self.body);
        let crc = crc32c::crc32c(&b[start..]);
        b.put_u32_le(crc);
    }

    /// Decodes one frame from the start of `buf`; returns it and its encoded length.
    /// [`FormatError::Truncated`] means `buf` ends before the frame does.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), FormatError> {
        need(buf, FRAME_FIXED)?;
        if u16_at(buf, 0) != FRAME_MAGIC {
            return Err(FormatError::BadMagic);
        }
        let version = buf[2];
        if version != FRAME_VERSION {
            return Err(FormatError::Unsupported(format!("frame version {version}")));
        }
        let ext_len = u16_at(buf, 6) as usize;
        let index = u64_at(buf, 8);
        let body_len = u32_at(buf, 16) as usize;
        if body_len > MAX_BODY {
            return Err(FormatError::Malformed(format!("body_len {body_len}")));
        }
        let total = FRAME_FIXED + ext_len + body_len + FRAME_TRAILER;
        need(buf, total)?;
        let stored = u32_at(buf, total - 4);
        let computed = crc32c::crc32c(&buf[..total - 4]);
        if stored != computed {
            return Err(FormatError::Checksum { stored, computed });
        }
        // Only interpret the header after the checksum matched.
        let kind = FrameKind::from_u8(buf[3])?;
        let codec = Codec::from_u8(buf[4])?;
        let flags = buf[5];
        if flags != 0 {
            return Err(FormatError::Unsupported(format!("frame flags {flags:#04x}")));
        }
        let ext = read_tlvs(&buf[FRAME_FIXED..FRAME_FIXED + ext_len])?;
        let body = Bytes::copy_from_slice(&buf[FRAME_FIXED + ext_len..total - 4]);
        Ok((Self { version, kind, codec, flags, index, ext, body }, total))
    }
}

fn need(buf: &[u8], n: usize) -> Result<(), FormatError> {
    if buf.len() < n { Err(FormatError::Truncated { need: n, have: buf.len() }) } else { Ok(()) }
}
fn u16_at(b: &[u8], i: usize) -> u16 {
    u16::from_le_bytes(b[i..i + 2].try_into().unwrap())
}
fn u32_at(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

fn tlvs_len(tlvs: &[Tlv]) -> usize {
    tlvs.iter().map(|t| 6 + t.value.len()).sum()
}

fn put_tlvs(b: &mut BytesMut, tlvs: &[Tlv]) {
    for t in tlvs {
        b.put_u16_le(t.tag);
        b.put_u32_le(t.value.len() as u32);
        b.put_slice(&t.value);
    }
}

fn put_tlvs_counted(b: &mut BytesMut, tlvs: &[Tlv]) {
    b.put_u16_le(tlvs.len() as u16);
    put_tlvs(b, tlvs);
}

fn read_tlvs(mut buf: &[u8]) -> Result<Vec<Tlv>, FormatError> {
    let mut out = Vec::new();
    while !buf.is_empty() {
        let (t, n) = read_tlv(buf)?;
        out.push(t);
        buf = &buf[n..];
    }
    Ok(out)
}

fn read_tlvs_counted(buf: &[u8], mut pos: usize) -> Result<(Vec<Tlv>, usize), FormatError> {
    let count = buf.get(pos..pos + 2).map(|s| u16::from_le_bytes([s[0], s[1]])).unwrap_or(0) as usize;
    if buf.len() >= pos + 2 {
        pos += 2;
    }
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let (t, n) = read_tlv(&buf[pos..])?;
        out.push(t);
        pos += n;
    }
    Ok((out, pos))
}

fn read_tlv(buf: &[u8]) -> Result<(Tlv, usize), FormatError> {
    if buf.len() < 6 {
        return Err(FormatError::Malformed("short TLV".into()));
    }
    let tag = u16_at(buf, 0);
    let len = u32_at(buf, 2) as usize;
    let value = buf.get(6..6 + len).ok_or_else(|| FormatError::Malformed("TLV value".into()))?;
    Ok((Tlv { tag, value: Bytes::copy_from_slice(value) }, 6 + len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip_and_build_tag() {
        let mut h = FileHeader::new(FileKind::LogSegment, 42);
        h.ext.push(Tlv { tag: 7, value: Bytes::from_static(b"x") });
        let enc = h.encode();
        let (d, n) = FileHeader::decode(&enc).unwrap();
        assert_eq!(n, enc.len());
        assert_eq!(d, h);
        assert!(d.build.starts_with("doris-ingress "), "{}", d.build);
    }

    #[test]
    fn header_from_newer_writer_with_extra_fields_is_readable() {
        let h = FileHeader::new(FileKind::RaftMeta, 0);
        let enc = h.encode();
        // Simulate a newer writer that appended 8 bytes of new fixed fields before the CRC.
        let mut body = enc[..enc.len() - 4].to_vec();
        body.extend_from_slice(&[9; 8]);
        let len = (body.len() + 4) as u32;
        body[12..16].copy_from_slice(&len.to_le_bytes());
        let crc = crc32c::crc32c(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        let (d, n) = FileHeader::decode(&body).unwrap();
        assert_eq!(n, body.len());
        assert_eq!(d.kind, FileKind::RaftMeta);
    }

    #[test]
    fn header_requiring_newer_reader_is_rejected() {
        let mut h = FileHeader::new(FileKind::LogSegment, 1);
        h.min_reader_version = SUPPORTED_READER_VERSION + 1;
        assert!(matches!(FileHeader::decode(&h.encode()), Err(FormatError::Unsupported(_))));
    }

    #[test]
    fn frame_roundtrip_with_extensions() {
        let mut f = Frame::new(FrameKind::RaftEntry, 99, Bytes::from_static(b"hello"));
        f.ext.push(Tlv { tag: 1, value: Bytes::from_static(b"abc") });
        let enc = f.encode();
        assert_eq!(enc.len(), f.encoded_len());
        let (d, n) = Frame::decode(&enc).unwrap();
        assert_eq!((d, n), (f, enc.len()));
    }

    #[test]
    fn every_single_bit_flip_is_detected() {
        let enc = Frame::new(FrameKind::RaftEntry, 5, Bytes::from_static(b"payload bytes")).encode();
        for byte in 0..enc.len() {
            for bit in 0..8 {
                let mut bad = enc.to_vec();
                bad[byte] ^= 1 << bit;
                assert!(Frame::decode(&bad).is_err(), "flip at byte {byte} bit {bit} not detected");
            }
        }
    }

    #[test]
    fn torn_frame_is_reported_as_truncated() {
        let enc = Frame::new(FrameKind::RaftEntry, 5, Bytes::from(vec![1u8; 100])).encode();
        for cut in 0..enc.len() {
            assert!(matches!(Frame::decode(&enc[..cut]), Err(FormatError::Truncated { .. })), "cut {cut}");
        }
    }
}
