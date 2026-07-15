// Copyright ⓒ 2026 Samuel Jenkins
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The common CRC-framed record envelope shared by segment files, the
//! metadata WAL, snapshots and the manifest:
//!
//! ```text
//! magic 0x54 | version u8 | type u8 | flags u8 | payload_len u32 LE | crc32c u32 LE | payload
//! ```
//!
//! CRC32C (Castagnoli, the Kafka batch algorithm) covers the four header
//! bytes plus the payload. The reader's torn-write rule: a short header, bad
//! magic/version, an over-long payload, or a CRC mismatch means the file was
//! torn mid-append — truncate to the record's start and stop. A single
//! append-only writer makes any later bytes unreachable by construction.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc_fast::{CrcAlgorithm, Digest};

use crate::{Error, Result};

pub(crate) const MAGIC: u8 = 0x54;
pub(crate) const VERSION: u8 = 1;
pub(crate) const HEADER_LEN: usize = 12;

/// Record kinds share one namespace across all nvme file types so a frame
/// landing in the wrong file is caught by type, not just by parse failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FrameType {
    Batch = 1,
    Wal = 2,
    Snapshot = 3,
    Manifest = 4,
}

impl TryFrom<u8> for FrameType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Batch),
            2 => Ok(Self::Wal),
            3 => Ok(Self::Snapshot),
            4 => Ok(Self::Manifest),
            otherwise => Err(Error::Message(format!("nvme frame type: {otherwise}"))),
        }
    }
}

fn crc32c(header: &[u8; 4], payload: &[u8]) -> u32 {
    let mut digest = Digest::new(CrcAlgorithm::Crc32Iscsi);
    digest.update(header);
    digest.update(payload);
    digest.finalize() as u32
}

/// Frame a payload for appending.
pub(crate) fn encode(frame_type: FrameType, payload: &[u8]) -> Bytes {
    let header = [MAGIC, VERSION, frame_type as u8, 0];
    let mut framed = BytesMut::with_capacity(HEADER_LEN + payload.len());

    framed.put_slice(&header);
    framed.put_u32_le(payload.len() as u32);
    framed.put_u32_le(crc32c(&header, payload));
    framed.put_slice(payload);

    framed.freeze()
}

/// One decoded frame plus its size on disk.
#[derive(Clone, Debug)]
pub(crate) struct Frame {
    pub frame_type: FrameType,
    pub payload: Bytes,
}

impl Frame {
    pub(crate) fn size_on_disk(&self) -> usize {
        HEADER_LEN + self.payload.len()
    }
}

/// The reader's verdict on the bytes at one position.
#[derive(Debug)]
pub(crate) enum Decoded {
    Frame(Frame),
    /// End of well-formed data: everything from this position on is a torn
    /// tail (or clean EOF) and must be truncated away.
    Torn,
}

/// Decode the frame at the start of `bytes` (which may hold trailing data).
pub(crate) fn decode(bytes: &[u8]) -> Decoded {
    if bytes.len() < HEADER_LEN {
        return Decoded::Torn;
    }

    let mut cursor = bytes;
    let magic = cursor.get_u8();
    let version = cursor.get_u8();
    let frame_type = cursor.get_u8();
    let _flags = cursor.get_u8();
    let payload_len = cursor.get_u32_le() as usize;
    let crc = cursor.get_u32_le();

    if magic != MAGIC || version != VERSION {
        return Decoded::Torn;
    }

    let Ok(frame_type) = FrameType::try_from(frame_type) else {
        return Decoded::Torn;
    };

    if cursor.len() < payload_len {
        return Decoded::Torn;
    }

    let payload = &cursor[..payload_len];

    // CRC over the header bytes as stored (flags included), not as assumed.
    let header: [u8; 4] = bytes[..4].try_into().expect("four header bytes");

    if crc32c(&header, payload) != crc {
        return Decoded::Torn;
    }

    Decoded::Frame(Frame {
        frame_type,
        payload: Bytes::copy_from_slice(payload),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let framed = encode(FrameType::Batch, b"hello");

        match decode(&framed) {
            Decoded::Frame(frame) => {
                assert_eq!(FrameType::Batch, frame.frame_type);
                assert_eq!(&b"hello"[..], &frame.payload[..]);
                assert_eq!(framed.len(), frame.size_on_disk());
            }
            Decoded::Torn => panic!("expected frame"),
        }
    }

    #[test]
    fn truncation_at_every_byte_is_torn() {
        let framed = encode(FrameType::Wal, b"payload bytes here");

        for cut in 0..framed.len() {
            assert!(
                matches!(decode(&framed[..cut]), Decoded::Torn),
                "cut at {cut} must be torn"
            );
        }
    }

    #[test]
    fn corruption_of_any_byte_is_torn_or_shorter_frame() {
        let framed = encode(FrameType::Batch, b"sensitive");

        for i in 0..framed.len() {
            let mut corrupted = framed.to_vec();
            corrupted[i] ^= 0x01;

            match decode(&corrupted) {
                Decoded::Torn => {}
                Decoded::Frame(frame) => {
                    // A flipped bit inside payload_len could only produce a
                    // *valid* shorter/longer frame if the CRC also matched,
                    // which the CRC makes computationally negligible.
                    panic!("corruption at {i} decoded as {frame:?}");
                }
            }
        }
    }

    #[test]
    fn trailing_bytes_ignored() {
        let mut bytes = encode(FrameType::Batch, b"first").to_vec();
        bytes.extend_from_slice(&encode(FrameType::Batch, b"second"));

        match decode(&bytes) {
            Decoded::Frame(frame) => assert_eq!(&b"first"[..], &frame.payload[..]),
            Decoded::Torn => panic!("expected frame"),
        }
    }
}
