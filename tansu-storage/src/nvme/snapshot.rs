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

//! Coordination-state snapshots: `snapshots/{wal_seq:020}.snap`, one
//! CRC-framed JSON document capturing everything the WAL would replay, so
//! WAL files at or below `wal_seq` can be deleted. Written atomically
//! (tmp + fsync + rename + directory fsync); the newest CRC-valid snapshot
//! wins at boot. Watermarks, producer sequences and txn produce ranges are
//! NOT here — the segment scan rebuilds them.
//!
//! Nested maps are flattened to vectors: serde_json cannot key maps by
//! integers, and JSON keeps the snapshot debuggable with plain tools.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::{
    frame::{self, Decoded, FrameType},
    partition::AbortedRange,
    state::{
        CoordState, ProducerDetail, TopicMetadata, Txn, TxnCommitOffset, TxnDetail,
        TxnProduceOffset,
    },
};
use crate::{Error, GroupDetail, OffsetCommitRequest, Result, ScramCredential, TxnState, Version};

pub(crate) const SNAPSHOT_SUFFIX: &str = "snap";
pub(crate) const FORMAT_VERSION: u32 = 1;

type SnapSequences = Vec<(i16, Vec<(String, Vec<(i32, i32)>)>)>;
type SnapProduces = Vec<(String, Vec<(i32, Option<TxnProduceOffset>)>)>;
type SnapOffsets = Vec<(String, Vec<(String, Vec<(i32, TxnCommitOffset)>)>)>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SnapProducer {
    pub id: i64,
    pub sequences: SnapSequences,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SnapTxnDetail {
    pub transaction_timeout_ms: i32,
    pub started_at: Option<SystemTime>,
    pub state: Option<TxnState>,
    pub produces: SnapProduces,
    pub offsets: SnapOffsets,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SnapTxn {
    pub id: String,
    pub producer: i64,
    pub epochs: Vec<(i16, SnapTxnDetail)>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SnapScram {
    pub user: String,
    pub mechanism: String,
    pub salt: Vec<u8>,
    pub iterations: i32,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SnapPartition {
    pub topic: String,
    pub partition: i32,
    pub log_start: i64,
    pub aborted: Vec<AbortedRange>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SnapshotDoc {
    pub format_version: u32,
    /// WAL files with seq <= this are captured by this snapshot.
    pub wal_seq: u64,
    pub producers: Vec<SnapProducer>,
    pub transactions: Vec<SnapTxn>,
    pub topics: Vec<TopicMetadata>,
    pub scram: Vec<SnapScram>,
    pub groups: Vec<(String, GroupDetail, Version)>,
    pub group_offsets: Vec<(String, String, i32, OffsetCommitRequest)>,
    pub partitions: Vec<SnapPartition>,
}

impl SnapshotDoc {
    pub(crate) fn from_state(
        wal_seq: u64,
        coord: &CoordState,
        groups: &std::collections::HashMap<String, (GroupDetail, Version)>,
        group_offsets: &BTreeMap<(String, crate::Topition), OffsetCommitRequest>,
        partitions: Vec<SnapPartition>,
    ) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            wal_seq,
            producers: coord
                .producers
                .iter()
                .map(|(id, detail)| SnapProducer {
                    id: *id,
                    sequences: detail
                        .sequences
                        .iter()
                        .map(|(epoch, topics)| {
                            (
                                *epoch,
                                topics
                                    .iter()
                                    .map(|(topic, partitions)| {
                                        (
                                            topic.clone(),
                                            partitions.iter().map(|(p, s)| (*p, *s)).collect(),
                                        )
                                    })
                                    .collect(),
                            )
                        })
                        .collect(),
                })
                .collect(),
            transactions: coord
                .transactions
                .iter()
                .map(|(id, txn)| SnapTxn {
                    id: id.clone(),
                    producer: txn.producer,
                    epochs: txn
                        .epochs
                        .iter()
                        .map(|(epoch, detail)| {
                            (
                                *epoch,
                                SnapTxnDetail {
                                    transaction_timeout_ms: detail.transaction_timeout_ms,
                                    started_at: detail.started_at,
                                    state: detail.state,
                                    produces: detail
                                        .produces
                                        .iter()
                                        .map(|(topic, partitions)| {
                                            (
                                                topic.clone(),
                                                partitions
                                                    .iter()
                                                    .map(|(p, range)| (*p, *range))
                                                    .collect(),
                                            )
                                        })
                                        .collect(),
                                    offsets: detail
                                        .offsets
                                        .iter()
                                        .map(|(group, topics)| {
                                            (
                                                group.clone(),
                                                topics
                                                    .iter()
                                                    .map(|(topic, partitions)| {
                                                        (
                                                            topic.clone(),
                                                            partitions
                                                                .iter()
                                                                .map(|(p, co)| (*p, co.clone()))
                                                                .collect(),
                                                        )
                                                    })
                                                    .collect(),
                                            )
                                        })
                                        .collect(),
                                },
                            )
                        })
                        .collect(),
                })
                .collect(),
            topics: coord.topics.values().cloned().collect(),
            scram: coord
                .scram
                .iter()
                .map(|((user, mechanism), credential)| SnapScram {
                    user: user.clone(),
                    mechanism: mechanism.clone(),
                    salt: credential.salt.to_vec(),
                    iterations: credential.iterations,
                    stored_key: credential.stored_key.to_vec(),
                    server_key: credential.server_key.to_vec(),
                })
                .collect(),
            groups: groups
                .iter()
                .map(|(group, (detail, version))| (group.clone(), detail.clone(), version.clone()))
                .collect(),
            group_offsets: group_offsets
                .iter()
                .map(|((group, topition), commit)| {
                    (
                        group.clone(),
                        topition.topic().to_owned(),
                        topition.partition(),
                        commit.clone(),
                    )
                })
                .collect(),
            partitions,
        }
    }

    /// Rebuild the coordination state this snapshot captured.
    pub(crate) fn coord_state(&self) -> CoordState {
        let mut coord = CoordState::default();

        for producer in &self.producers {
            let mut detail = ProducerDetail::default();

            for (epoch, topics) in &producer.sequences {
                let mut by_topic = BTreeMap::new();
                for (topic, partitions) in topics {
                    _ = by_topic.insert(topic.clone(), partitions.iter().copied().collect());
                }
                _ = detail.sequences.insert(*epoch, by_topic);
            }

            _ = coord.producers.insert(producer.id, detail);
        }

        for txn in &self.transactions {
            let mut epochs = BTreeMap::new();

            for (epoch, snap) in &txn.epochs {
                let mut produces = BTreeMap::new();
                for (topic, partitions) in &snap.produces {
                    _ = produces.insert(topic.clone(), partitions.iter().copied().collect());
                }

                let mut offsets = BTreeMap::new();
                for (group, topics) in &snap.offsets {
                    let mut by_topic = BTreeMap::new();
                    for (topic, partitions) in topics {
                        _ = by_topic.insert(topic.clone(), partitions.iter().cloned().collect());
                    }
                    _ = offsets.insert(group.clone(), by_topic);
                }

                _ = epochs.insert(
                    *epoch,
                    TxnDetail {
                        transaction_timeout_ms: snap.transaction_timeout_ms,
                        started_at: snap.started_at,
                        state: snap.state,
                        produces,
                        offsets,
                    },
                );
            }

            _ = coord.transactions.insert(
                txn.id.clone(),
                Txn {
                    producer: txn.producer,
                    epochs,
                },
            );
        }

        for metadata in &self.topics {
            _ = coord
                .topics
                .insert(metadata.topic.name.clone(), metadata.clone());
        }

        for scram in &self.scram {
            _ = coord.scram.insert(
                (scram.user.clone(), scram.mechanism.clone()),
                ScramCredential {
                    salt: scram.salt.clone().into(),
                    iterations: scram.iterations,
                    stored_key: scram.stored_key.clone().into(),
                    server_key: scram.server_key.clone().into(),
                },
            );
        }

        coord
    }
}

fn snapshot_path(dir: &Path, wal_seq: u64) -> PathBuf {
    dir.join(format!("{wal_seq:020}.{SNAPSHOT_SUFFIX}"))
}

/// Atomically persist a snapshot; returns its path.
pub(crate) fn write(dir: &Path, doc: &SnapshotDoc) -> Result<PathBuf> {
    std::fs::create_dir_all(dir)
        .map_err(|err| Error::Message(format!("nvme snapshot dir: {err}")))?;

    let payload = serde_json::to_vec(doc)?;
    let framed = frame::encode(FrameType::Snapshot, &payload);

    let tmp = dir.join(format!(".{:020}.tmp", doc.wal_seq));
    let path = snapshot_path(dir, doc.wal_seq);

    std::fs::write(&tmp, &framed)
        .and_then(|()| std::fs::File::open(&tmp).and_then(|file| file.sync_all()))
        .and_then(|()| std::fs::rename(&tmp, &path))
        .and_then(|()| std::fs::File::open(dir).and_then(|dir| dir.sync_all()))
        .map_err(|err| Error::Message(format!("nvme snapshot write: {err}")))?;

    debug!(?path);

    Ok(path)
}

/// Newest CRC-valid snapshot, if any.
pub(crate) fn load_latest(dir: &Path) -> Result<Option<SnapshotDoc>> {
    let mut seqs = vec![];

    if !dir.is_dir() {
        return Ok(None);
    }

    for entry in
        std::fs::read_dir(dir).map_err(|err| Error::Message(format!("nvme snapshot dir: {err}")))?
    {
        let entry = entry.map_err(|err| Error::Message(format!("nvme snapshot dir: {err}")))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if let Some(stem) = name.strip_suffix(&format!(".{SNAPSHOT_SUFFIX}"))
            && let Ok(seq) = stem.parse::<u64>()
        {
            seqs.push(seq);
        }
    }

    seqs.sort_unstable();

    for seq in seqs.iter().rev() {
        let path = snapshot_path(dir, *seq);
        let bytes = std::fs::read(&path)
            .map_err(|err| Error::Message(format!("nvme snapshot read: {err}")))?;

        match frame::decode(&bytes) {
            Decoded::Frame(frame) if frame.frame_type == FrameType::Snapshot => {
                match serde_json::from_slice::<SnapshotDoc>(&frame.payload) {
                    Ok(doc) if doc.format_version == FORMAT_VERSION => return Ok(Some(doc)),
                    Ok(doc) => {
                        return Err(Error::Message(format!(
                            "nvme snapshot format {} unsupported (want {FORMAT_VERSION}); \
                             refusing to boot rather than lose coordination state",
                            doc.format_version
                        )));
                    }
                    Err(err) => warn!(?path, ?err, "snapshot parse; trying older"),
                }
            }

            _otherwise => warn!(?path, "snapshot corrupt; trying older"),
        }
    }

    Ok(None)
}

/// Remove snapshots older than the newest two, and any stale tmp files.
pub(crate) fn prune(dir: &Path) -> Result<()> {
    let mut paths: Vec<(u64, PathBuf)> = vec![];

    for entry in
        std::fs::read_dir(dir).map_err(|err| Error::Message(format!("nvme snapshot dir: {err}")))?
    {
        let entry = entry.map_err(|err| Error::Message(format!("nvme snapshot dir: {err}")))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if name.starts_with('.') && name.ends_with(".tmp") {
            _ = std::fs::remove_file(entry.path());
        } else if let Some(stem) = name.strip_suffix(&format!(".{SNAPSHOT_SUFFIX}"))
            && let Ok(seq) = stem.parse::<u64>()
        {
            paths.push((seq, entry.path()));
        }
    }

    paths.sort_unstable_by_key(|(seq, _)| *seq);

    while paths.len() > 2 {
        let (_, path) = paths.remove(0);
        _ = std::fs::remove_file(&path).inspect_err(|err| warn!(?path, ?err));
    }

    Ok(())
}
