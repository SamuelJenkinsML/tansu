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

//! The metadata write-ahead log: coordination mutations (producer ids,
//! transaction state, group offsets/state, topics, SCRAM, log starts) as
//! `FrameType::Wal` frames with serde-JSON payloads. Record data is NOT
//! here — producer sequences, txn produce ranges and watermarks are rebuilt
//! from the segment scan at boot.
//!
//! One WAL file per boot epoch (`wal/{seq:020}.wal`); a snapshot retires
//! every file at or below its sequence.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::debug;

use super::{
    FsyncMode,
    frame::{self, Decoded, FrameType},
    groupcommit::Flusher,
    state::{TxnCommitOffset, TxnProduceOffset},
};
use crate::{Error, GroupDetail, OffsetCommitRequest, Result, Version};

pub(crate) const WAL_SUFFIX: &str = "wal";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) enum WalRecord {
    TopicCreate {
        id: uuid::Uuid,
        topic: tansu_sans_io::create_topics_request::CreatableTopic,
    },
    TopicDelete {
        name: String,
    },
    ConfigAlter {
        name: String,
        changes: Vec<tansu_sans_io::incremental_alter_configs_request::AlterableConfig>,
    },
    PidAlloc {
        producer_id: i64,
        transaction_id: Option<String>,
        transaction_timeout_ms: i32,
    },
    EpochBump {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        transaction_timeout_ms: i32,
    },
    TxnBegin {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        started_at: SystemTime,
        partitions: Vec<(String, i32)>,
    },
    TxnPrepare {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        produced: Vec<(String, i32, TxnProduceOffset)>,
    },
    TxnTerminal {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        aborted: Vec<(String, i32, TxnProduceOffset)>,
    },
    TxnOffsets {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        group: String,
        offsets: Vec<(String, i32, TxnCommitOffset)>,
    },
    GroupOffsetCommit {
        group: String,
        offsets: Vec<(String, i32, OffsetCommitRequest)>,
    },
    GroupUpdate {
        group: String,
        detail: GroupDetail,
        version: Version,
    },
    GroupDelete {
        group: String,
    },
    ScramUpsert {
        user: String,
        mechanism: String,
        salt: Vec<u8>,
        iterations: i32,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    },
    ScramDelete {
        user: String,
        mechanism: String,
    },
    LogStartAdvance {
        topic: String,
        partition: i32,
        offset: i64,
    },
}

fn wal_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:020}.{WAL_SUFFIX}"))
}

#[derive(Debug)]
struct WalInner {
    file: Arc<File>,
    flusher: Flusher,
    bytes_since_open: u64,
}

/// The WAL writer for this boot epoch. Appends are serialized by the inner
/// mutex; durability acks ride the WAL's own group-commit flusher.
#[derive(Debug)]
pub(crate) struct Wal {
    pub seq: u64,
    path: PathBuf,
    inner: std::sync::Mutex<WalInner>,
}

impl Wal {
    /// Open a fresh WAL file at `seq` (one per boot epoch).
    pub(crate) fn create(dir: &Path, seq: u64, mode: FsyncMode) -> Result<Self> {
        let path = wal_path(dir, seq);

        let file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(&path)
            .map_err(|err| Error::Message(format!("nvme wal create {path:?}: {err}")))
            .map(Arc::new)?;

        debug!(?path, seq);

        let flusher = Flusher::spawn(format!("wal-{seq}"), file.clone(), mode);

        Ok(Self {
            seq,
            path,
            inner: std::sync::Mutex::new(WalInner {
                file,
                flusher,
                bytes_since_open: 0,
            }),
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Bytes appended to this WAL file (the snapshot-cadence signal).
    pub(crate) fn bytes_since_open(&self) -> Result<u64> {
        Ok(self.inner.lock()?.bytes_since_open)
    }

    /// Final fsync of this WAL file; its flusher exits after acking.
    pub(crate) fn seal(&self) -> Result<oneshot::Receiver<Result<()>>> {
        self.inner.lock()?.flusher.seal()
    }

    /// Append one record and return the durability ack to await, plus the
    /// framed record's size on disk.
    pub(crate) fn append(&self, record: &WalRecord) -> Result<(oneshot::Receiver<Result<()>>, u64)> {
        let payload = serde_json::to_vec(record)?;
        let framed = frame::encode(FrameType::Wal, &payload);

        let mut inner = self.inner.lock()?;

        (&*inner.file)
            .write_all(&framed)
            .map_err(|err| Error::Message(format!("nvme wal append: {err}")))?;

        inner.bytes_since_open += framed.len() as u64;

        Ok((inner.flusher.sync()?, framed.len() as u64))
    }
}

/// WAL sequences present in the directory, ascending.
pub(crate) fn wal_seqs(dir: &Path) -> Result<Vec<u64>> {
    let mut seqs = vec![];

    if !dir.is_dir() {
        return Ok(seqs);
    }

    for entry in
        std::fs::read_dir(dir).map_err(|err| Error::Message(format!("nvme wal dir: {err}")))?
    {
        let entry = entry.map_err(|err| Error::Message(format!("nvme wal dir: {err}")))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if let Some(stem) = name.strip_suffix(&format!(".{WAL_SUFFIX}"))
            && let Ok(seq) = stem.parse::<u64>()
        {
            seqs.push(seq);
        }
    }

    seqs.sort_unstable();
    Ok(seqs)
}

/// Replay one WAL file. A torn tail simply ends the file (single writer,
/// crash mid-append); anything decoded before it is applied.
pub(crate) fn replay(path: &Path, mut apply: impl FnMut(WalRecord)) -> Result<()> {
    let bytes =
        std::fs::read(path).map_err(|err| Error::Message(format!("nvme wal read: {err}")))?;

    let mut position = 0usize;

    while position < bytes.len() {
        match frame::decode(&bytes[position..]) {
            Decoded::Frame(frame) if frame.frame_type == FrameType::Wal => {
                let record: WalRecord = serde_json::from_slice(&frame.payload)?;
                apply(record);
                position += frame.size_on_disk();
            }

            _otherwise => {
                debug!(?path, position, "wal replay ends (torn tail)");
                break;
            }
        }
    }

    Ok(())
}
