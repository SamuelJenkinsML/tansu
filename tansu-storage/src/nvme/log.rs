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

//! Segment files: `{base_offset:020}.seg`, a run of `FrameType::Batch`
//! frames whose payload is `base_offset i64 LE | raw deflated batch bytes`.
//! Sequential append by exactly one writer; reads are positioned (`pread`).

use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::{Buf, Bytes, BytesMut};
use tracing::{debug, warn};

use super::{
    FsyncMode,
    frame::{self, Decoded, FrameType},
    groupcommit::Flusher,
};
use crate::{Error, Result};

pub(crate) const SEGMENT_SUFFIX: &str = "seg";

pub(crate) fn segment_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{base_offset:020}.{SEGMENT_SUFFIX}"))
}

/// Frame a batch for the segment log.
pub(crate) fn encode_batch(base_offset: i64, batch: &Bytes) -> Bytes {
    let mut payload = BytesMut::with_capacity(8 + batch.len());
    payload.extend_from_slice(&base_offset.to_le_bytes());
    payload.extend_from_slice(batch);

    frame::encode(FrameType::Batch, &payload)
}

/// The active (tail) segment of one partition.
#[derive(Debug)]
pub(crate) struct SegmentWriter {
    pub base_offset: i64,
    /// Bytes appended so far (buffered writes included).
    pub position: u64,
    pub file: Arc<File>,
    pub flusher: Flusher,
}

impl SegmentWriter {
    pub(crate) fn create(
        dir: &Path,
        name: &str,
        base_offset: i64,
        mode: FsyncMode,
    ) -> Result<Self> {
        let path = segment_path(dir, base_offset);

        // Read as well as append: fetches of tail-cache-evicted batches
        // pread the active segment through this same handle.
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(&path)
            .map_err(|err| Error::Message(format!("nvme segment create {path:?}: {err}")))
            .map(Arc::new)?;

        debug!(?path, base_offset);

        let flusher = Flusher::spawn(format!("{name}-{base_offset}"), file.clone(), mode);

        Ok(Self {
            base_offset,
            position: 0,
            file,
            flusher,
        })
    }

    /// Append framed bytes; returns the frame's start position. Durability
    /// is the caller's job (submit to the flusher and await).
    pub(crate) fn append(&mut self, framed: &Bytes) -> Result<u64> {
        let position = self.position;

        (&*self.file)
            .write_all(framed)
            .map_err(|err| Error::Message(format!("nvme segment append: {err}")))?;

        self.position += framed.len() as u64;

        Ok(position)
    }
}

/// One recovered batch location.
#[derive(Clone, Debug)]
pub(crate) struct ScannedBatch {
    pub base_offset: i64,
    pub file_position: u64,
    pub len: u32,
    pub batch: Bytes,
}

/// Scan a segment file, yielding well-formed batches and the length of the
/// valid prefix. A torn tail (crash mid-append) ends the scan; the caller
/// truncates the file to `valid_len`.
pub(crate) fn scan_segment(path: &Path) -> Result<(Vec<ScannedBatch>, u64)> {
    let bytes = std::fs::read(path)
        .map_err(|err| Error::Message(format!("nvme segment read {path:?}: {err}")))?;

    let mut batches = vec![];
    let mut position = 0usize;

    while position < bytes.len() {
        match frame::decode(&bytes[position..]) {
            Decoded::Frame(frame) if frame.frame_type == FrameType::Batch => {
                let mut payload = frame.payload.clone();

                if payload.len() < 8 {
                    warn!(?path, position, "batch frame under 8 bytes");
                    break;
                }

                let base_offset = payload.get_i64_le();

                batches.push(ScannedBatch {
                    base_offset,
                    file_position: position as u64,
                    len: frame.size_on_disk() as u32,
                    batch: payload,
                });

                position += frame.size_on_disk();
            }

            Decoded::Frame(frame) => {
                warn!(?path, position, ?frame.frame_type, "foreign frame in segment");
                break;
            }

            Decoded::Torn => break,
        }
    }

    Ok((batches, position as u64))
}

/// Truncate a torn tail off a segment (crash recovery).
pub(crate) fn truncate_to(path: &Path, valid_len: u64) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|err| Error::Message(format!("nvme truncate open {path:?}: {err}")))?;

    file.set_len(valid_len)
        .and_then(|()| file.sync_all())
        .map_err(|err| Error::Message(format!("nvme truncate {path:?}: {err}")))
}

/// Positioned read of one frame; returns the raw batch bytes.
pub(crate) fn read_batch_at(file: &File, position: u64, len: u32) -> Result<Bytes> {
    let mut framed = vec![0u8; len as usize];

    file.read_exact_at(&mut framed, position)
        .map_err(|err| Error::Message(format!("nvme segment pread: {err}")))?;

    match frame::decode(&framed) {
        Decoded::Frame(frame) if frame.frame_type == FrameType::Batch => {
            let mut payload = frame.payload;

            if payload.len() < 8 {
                return Err(Error::Message("nvme segment pread: short payload".into()));
            }

            _ = payload.get_i64_le();
            Ok(payload)
        }

        _otherwise => Err(Error::Message(format!(
            "nvme segment pread: bad frame at {position}"
        ))),
    }
}

/// Base offsets of the segments in a partition directory, ascending.
pub(crate) fn segment_bases(dir: &Path) -> Result<Vec<i64>> {
    let mut bases = vec![];

    for entry in std::fs::read_dir(dir)
        .map_err(|err| Error::Message(format!("nvme read_dir {dir:?}: {err}")))?
    {
        let entry = entry.map_err(|err| Error::Message(format!("nvme read_dir: {err}")))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if let Some(stem) = name.strip_suffix(&format!(".{SEGMENT_SUFFIX}"))
            && let Ok(base) = stem.parse::<i64>()
        {
            bases.push(base);
        }
    }

    bases.sort_unstable();
    Ok(bases)
}
