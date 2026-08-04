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

//! Boot: lock the data dir, load the newest snapshot, replay the WAL, scan
//! the segment tails, and hand back fully rebuilt state.
//!
//! Invariants restored here: everything acked was durable (WAL or segment
//! fsync), so replay+scan reproduces exactly the acked state plus possibly
//! some unacked writes (which clients never saw and will retry — rebuilt
//! producer sequences dedupe them). In-doubt (prepared) transactions are
//! listed for the engine to finish resolving once it can produce markers.

use std::{
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    path::Path,
    sync::Arc,
};

use tansu_sans_io::BatchAttribute;
use tracing::{debug, info, warn};

use super::{
    frame::{self, Decoded, FrameType},
    log,
    partition::{AbortedRange, BatchEntry, PartitionState},
    snapshot::{self, SnapshotDoc},
    state::{CoordState, ProducerDetail, Txn, TxnDetail, TxnProduceOffset},
    wal::{self, Wal, WalRecord},
};
use crate::{Error, GroupDetail, OffsetCommitRequest, Result, Topition, TxnState, Version};

pub(crate) const MANIFEST_FORMAT_VERSION: u32 = 1;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Manifest {
    format_version: u32,
    cluster_id: String,
    // Reserved for Phase 3 (per-partition ownership across brokers).
    #[serde(default)]
    leader_epochs: BTreeMap<String, i64>,
}

pub(crate) struct Recovered {
    pub coord: CoordState,
    pub groups: HashMap<String, (GroupDetail, Version)>,
    pub group_offsets: BTreeMap<(String, Topition), OffsetCommitRequest>,
    pub partitions: HashMap<Topition, Arc<PartitionState>>,
    pub wal: Wal,
    /// Held (locked) for the engine's lifetime: one process per data dir.
    pub lock: File,
    /// Prepared transactions awaiting resolution: (txn, pid, epoch, committed).
    pub in_doubt: Vec<(String, i64, i16, bool)>,
}

/// Per-partition state accumulated from snapshot + WAL before the scan.
#[derive(Debug, Default)]
struct Skeleton {
    log_start: i64,
    aborted: Vec<AbortedRange>,
}

pub(crate) async fn recover(
    root: &Path,
    cluster: &str,
    fsync: super::FsyncMode,
    tier: Option<&super::tier::TierStore>,
) -> Result<Recovered> {
    let topics_dir = root.join("topics");
    let wal_dir = root.join("wal");
    let snapshots_dir = root.join("snapshots");

    for dir in [root, &topics_dir, &wal_dir, &snapshots_dir] {
        std::fs::create_dir_all(dir)
            .map_err(|err| Error::Message(format!("nvme create {dir:?}: {err}")))?;
    }

    // One process per data dir, enforced for the process lifetime.
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join("LOCK"))
        .map_err(|err| Error::Message(format!("nvme LOCK open: {err}")))?;

    lock.try_lock().map_err(|err| {
        Error::Message(format!(
            "nvme data dir {root:?} is locked by another broker: {err}"
        ))
    })?;

    manifest(root, cluster)?;

    // Cold bootstrap: with no local snapshot but a tiered mirror, pull the
    // snapshot AND the mirrored WAL files above it — together they carry the
    // coordination state (topics, groups, producers, transactions) this
    // data dir has never seen, to within one tier interval.
    if let Some(tier) = tier
        && snapshot::load_latest(&snapshots_dir)?.is_none()
        && let Some((seq, framed)) = tier.latest_snapshot().await?
    {
        info!(seq, "cold bootstrap: downloading tiered snapshot");
        let path = snapshots_dir.join(format!("{seq:020}.{}", snapshot::SNAPSHOT_SUFFIX));
        std::fs::write(&path, &framed)
            .map_err(|err| Error::Message(format!("nvme tier snapshot restore: {err}")))?;

        for wal_seq in tier.wal_seqs().await? {
            if wal_seq > seq {
                info!(wal_seq, "cold bootstrap: downloading mirrored wal");
                let bytes = tier.wal(wal_seq).await?;
                std::fs::write(
                    wal_dir.join(format!("{wal_seq:020}.{}", wal::WAL_SUFFIX)),
                    &bytes,
                )
                .map_err(|err| Error::Message(format!("nvme tier wal restore: {err}")))?;
            }
        }
    }

    // Snapshot, then the WAL records it does not capture.
    let snapshot = snapshot::load_latest(&snapshots_dir)?;
    let snapshot_seq = snapshot.as_ref().map_or(0, |doc| doc.wal_seq);

    let mut coord = snapshot
        .as_ref()
        .map(SnapshotDoc::coord_state)
        .unwrap_or_default();

    let mut groups: HashMap<String, (GroupDetail, Version)> = snapshot
        .as_ref()
        .map(|doc| {
            doc.groups
                .iter()
                .map(|(group, detail, version)| (group.clone(), (detail.clone(), version.clone())))
                .collect()
        })
        .unwrap_or_default();

    let mut group_offsets: BTreeMap<(String, Topition), OffsetCommitRequest> = snapshot
        .as_ref()
        .map(|doc| {
            doc.group_offsets
                .iter()
                .map(|(group, topic, partition, commit)| {
                    (
                        (group.clone(), Topition::new(topic.clone(), *partition)),
                        commit.clone(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    let mut skeletons: HashMap<(String, i32), Skeleton> = snapshot
        .as_ref()
        .map(|doc| {
            doc.partitions
                .iter()
                .map(|snap| {
                    (
                        (snap.topic.clone(), snap.partition),
                        Skeleton {
                            log_start: snap.log_start,
                            aborted: snap.aborted.clone(),
                        },
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    let mut replayed = 0usize;
    let existing = wal::wal_seqs(&wal_dir)?;

    for seq in existing.iter().filter(|seq| **seq > snapshot_seq) {
        wal::replay(
            &wal_dir.join(format!("{seq:020}.{}", wal::WAL_SUFFIX)),
            |record| {
                replayed += 1;
                apply(
                    record,
                    &mut coord,
                    &mut groups,
                    &mut group_offsets,
                    &mut skeletons,
                );
            },
        )?;
    }

    let next_seq = existing.last().copied().unwrap_or(0).max(snapshot_seq) + 1;
    let wal = Wal::create(&wal_dir, next_seq, fsync)?;

    // Segment scan: rebuild indexes, watermarks, producer sequences and
    // open-transaction produce ranges from the batches themselves.
    let mut partitions = HashMap::new();

    let entries = std::fs::read_dir(&topics_dir)
        .map_err(|err| Error::Message(format!("nvme topics dir: {err}")))?;

    for entry in entries {
        let entry = entry.map_err(|err| Error::Message(format!("nvme topics dir: {err}")))?;

        let Ok(topition) = Topition::try_from(&entry) else {
            warn!(?entry, "unrecognized partition directory");
            continue;
        };

        let state = scan_partition(
            &entry.path(),
            &topition,
            skeletons
                .remove(&(topition.topic().to_owned(), topition.partition()))
                .unwrap_or_default(),
            &mut coord,
        )?;

        _ = partitions.insert(topition, Arc::new(state));
    }

    // Partitions known to the snapshot/WAL but with no segment directory
    // yet (created topics that never produced, or a cold bootstrap).
    for ((topic, partition), skeleton) in skeletons {
        let topition = Topition::new(topic, partition);
        let dir = topics_dir.join(std::path::PathBuf::from(&topition));

        std::fs::create_dir_all(&dir)
            .map_err(|err| Error::Message(format!("nvme create {dir:?}: {err}")))?;

        let state = PartitionState::new(dir);
        {
            let mut inner = state.inner.lock()?;
            inner.log_start = skeleton.log_start;
            inner.next_offset = skeleton.log_start;
            inner.durable_offset = skeleton.log_start;
            inner.aborted = skeleton.aborted;
        }

        _ = partitions.insert(topition, Arc::new(state));
    }

    // Ensure directories exist for every partition of every known topic.
    for metadata in coord.topics.clone().values() {
        for partition in 0..metadata.topic.num_partitions {
            let topition = Topition::new(metadata.topic.name.clone(), partition);

            if let std::collections::hash_map::Entry::Vacant(vacant) =
                partitions.entry(topition.clone())
            {
                let dir = topics_dir.join(std::path::PathBuf::from(&topition));
                std::fs::create_dir_all(&dir)
                    .map_err(|err| Error::Message(format!("nvme create {dir:?}: {err}")))?;
                _ = vacant.insert(Arc::new(PartitionState::new(dir)));
            }
        }
    }

    // Tiered segments absent locally: rebuild their indexes from the
    // sidecars — no record data is downloaded; those batches serve by
    // ranged GET until (if ever) rehydrated.
    if let Some(tier) = tier {
        for (topition, state) in &partitions {
            let remote = tier.segment_bases(topition).await?;

            if remote.is_empty() {
                continue;
            }

            let local: std::collections::BTreeSet<i64> = {
                let inner = state.inner.lock()?;
                log::segment_bases(&inner.dir)?.into_iter().collect()
            };

            let mut sidecars = vec![];
            for base in &remote {
                if !local.contains(base) {
                    sidecars.push((*base, tier.sidecar(topition, *base).await?));
                }
            }

            let txn_by_producer: HashMap<i64, String> = coord
                .transactions
                .iter()
                .map(|(id, txn)| (txn.producer, id.clone()))
                .collect();

            let mut inner = state.inner.lock()?;
            inner.uploaded = remote.iter().copied().collect();

            for (base, entries) in sidecars {
                for entry in entries {
                    let offset_end = entry.offset + i64::from(entry.last_offset_delta);

                    inner.index(
                        entry.offset,
                        BatchEntry {
                            segment_base: base,
                            position: entry.position,
                            len: entry.len,
                            tiered: true,
                            cached: None,
                            last_offset_delta: entry.last_offset_delta,
                            max_timestamp: entry.max_timestamp,
                            is_control: entry.is_control,
                            is_transactional: entry.is_transactional,
                            producer_id: entry.producer_id,
                            producer_epoch: entry.producer_epoch,
                            base_sequence: entry.base_sequence,
                        },
                        0,
                    );

                    inner.next_offset = inner.next_offset.max(offset_end + 1);

                    if entry.producer_id >= 0 && entry.base_sequence >= 0 {
                        let sequences = coord
                            .producers
                            .entry(entry.producer_id)
                            .or_default()
                            .sequences
                            .entry(entry.producer_epoch)
                            .or_default()
                            .entry(topition.topic().to_owned())
                            .or_default()
                            .entry(topition.partition())
                            .or_default();

                        *sequences =
                            (*sequences).max(entry.base_sequence + entry.last_offset_delta + 1);
                    }

                    if entry.is_transactional
                        && !entry.is_control
                        && let Some(transaction_id) = txn_by_producer.get(&entry.producer_id)
                        && let Some(txn) = coord.transactions.get_mut(transaction_id)
                        && txn.producer == entry.producer_id
                        && let Some(detail) = txn.epochs.get_mut(&entry.producer_epoch)
                        && detail.state == Some(TxnState::Begin)
                    {
                        merge_range(detail, topition, entry.offset, offset_end);
                    }
                }
            }

            inner.durable_offset = inner.next_offset;

            let log_start = inner.log_start;
            inner.log_start = 0;
            _ = inner.advance_log_start(log_start);

            debug!(
                ?topition,
                tiered = inner.uploaded.len(),
                next = inner.next_offset
            );
        }
    }

    // Open transactions (Begin or prepared) pin the last stable offset on
    // every partition they produced to.
    let mut in_doubt = vec![];

    for (transaction_id, txn) in &coord.transactions {
        for (epoch, detail) in &txn.epochs {
            let open = matches!(
                detail.state,
                Some(TxnState::Begin)
                    | Some(TxnState::PrepareCommit)
                    | Some(TxnState::PrepareAbort)
            );

            if !open {
                continue;
            }

            for (topic, by_partition) in &detail.produces {
                for (partition, range) in by_partition {
                    let Some(range) = range else { continue };
                    let topition = Topition::new(topic.clone(), *partition);

                    if let Some(state) = partitions.get(&topition) {
                        let mut inner = state.inner.lock()?;
                        _ = inner.open_txns.insert(
                            (transaction_id.clone(), txn.producer, *epoch),
                            range.offset_start,
                        );
                    }
                }
            }

            if let Some(state @ (TxnState::PrepareCommit | TxnState::PrepareAbort)) = detail.state {
                in_doubt.push((
                    transaction_id.clone(),
                    txn.producer,
                    *epoch,
                    state == TxnState::PrepareCommit,
                ));
            }
        }
    }

    info!(
        cluster,
        snapshot_seq,
        replayed,
        partitions = partitions.len(),
        transactions = coord.transactions.len(),
        in_doubt = in_doubt.len(),
        wal_seq = next_seq,
        "nvme recovery complete"
    );

    Ok(Recovered {
        coord,
        groups,
        group_offsets,
        partitions,
        wal,
        lock,
        in_doubt,
    })
}

fn manifest(root: &Path, cluster: &str) -> Result<()> {
    let path = root.join("MANIFEST");

    if path.exists() {
        let bytes = std::fs::read(&path)
            .map_err(|err| Error::Message(format!("nvme MANIFEST read: {err}")))?;

        match frame::decode(&bytes) {
            Decoded::Frame(frame) if frame.frame_type == FrameType::Manifest => {
                let manifest: Manifest = serde_json::from_slice(&frame.payload)?;

                if manifest.format_version != MANIFEST_FORMAT_VERSION {
                    return Err(Error::Message(format!(
                        "nvme MANIFEST format {}, want {MANIFEST_FORMAT_VERSION}",
                        manifest.format_version
                    )));
                }

                if manifest.cluster_id != cluster {
                    return Err(Error::Message(format!(
                        "nvme data dir belongs to cluster {:?}, not {cluster:?}",
                        manifest.cluster_id
                    )));
                }

                Ok(())
            }

            _otherwise => Err(Error::Message("nvme MANIFEST corrupt".into())),
        }
    } else {
        let manifest = Manifest {
            format_version: MANIFEST_FORMAT_VERSION,
            cluster_id: cluster.to_owned(),
            leader_epochs: BTreeMap::new(),
        };

        let framed = frame::encode(FrameType::Manifest, &serde_json::to_vec(&manifest)?);

        std::fs::write(&path, &framed)
            .and_then(|()| File::open(&path).and_then(|file| file.sync_all()))
            .and_then(|()| File::open(root).and_then(|dir| dir.sync_all()))
            .map_err(|err| Error::Message(format!("nvme MANIFEST write: {err}")))
    }
}

fn scan_partition(
    dir: &Path,
    topition: &Topition,
    skeleton: Skeleton,
    coord: &mut CoordState,
) -> Result<PartitionState> {
    let state = PartitionState::new(dir.to_path_buf());

    // Producer id -> transaction id, for rebuilding open-txn produce ranges.
    let txn_by_producer: HashMap<i64, String> = coord
        .transactions
        .iter()
        .map(|(id, txn)| (txn.producer, id.clone()))
        .collect();

    {
        let mut inner = state.inner.lock()?;
        inner.log_start = skeleton.log_start;
        inner.aborted = skeleton.aborted;

        for base in log::segment_bases(dir)? {
            let path = log::segment_path(dir, base);
            let file_len = std::fs::metadata(&path)
                .map_err(|err| Error::Message(format!("nvme segment stat: {err}")))?
                .len();

            let (batches, valid_len) = log::scan_segment(&path)?;

            if valid_len < file_len {
                warn!(?path, valid_len, file_len, "truncating torn segment tail");
                log::truncate_to(&path, valid_len)?;
            }

            for scanned in batches {
                let batch = tansu_sans_io::record::deflated::Batch::try_from(scanned.batch)
                    .map_err(|err| Error::Message(format!("nvme scan decode {path:?}: {err:?}")))?;

                let attributes = BatchAttribute::try_from(batch.attributes)?;
                let offset = scanned.base_offset;
                let offset_end = offset + i64::from(batch.last_offset_delta);

                inner.index(
                    offset,
                    BatchEntry {
                        segment_base: base,
                        position: scanned.file_position,
                        len: scanned.len,
                        tiered: false,
                        cached: None,
                        last_offset_delta: batch.last_offset_delta,
                        max_timestamp: batch.max_timestamp,
                        is_control: attributes.control,
                        is_transactional: attributes.transaction,
                        producer_id: batch.producer_id,
                        producer_epoch: batch.producer_epoch,
                        base_sequence: batch.base_sequence,
                    },
                    0,
                );

                inner.next_offset = inner.next_offset.max(offset_end + 1);

                // Idempotent-producer sequences continue from what's on disk.
                if batch.is_idempotent() {
                    let sequences = coord
                        .producers
                        .entry(batch.producer_id)
                        .or_default()
                        .sequences
                        .entry(batch.producer_epoch)
                        .or_default()
                        .entry(topition.topic().to_owned())
                        .or_default()
                        .entry(topition.partition())
                        .or_default();

                    *sequences =
                        (*sequences).max(batch.base_sequence + batch.last_offset_delta + 1);
                }

                // Produce ranges of still-open (Begin) transactions were not
                // in the WAL: rebuild them from the batches. Prepared txns'
                // ranges came from their WAL TxnPrepare records.
                if attributes.transaction
                    && !attributes.control
                    && let Some(transaction_id) = txn_by_producer.get(&batch.producer_id)
                    && let Some(txn) = coord.transactions.get_mut(transaction_id)
                    && txn.producer == batch.producer_id
                    && let Some(detail) = txn.epochs.get_mut(&batch.producer_epoch)
                    && detail.state == Some(TxnState::Begin)
                {
                    merge_range(detail, topition, offset, offset_end);
                }
            }
        }

        // Everything scanned survived the crash: it is durable now. A log
        // start past the scanned tail (all segments deleted by retention)
        // still positions the next offset.
        inner.next_offset = inner.next_offset.max(inner.log_start);
        inner.durable_offset = inner.next_offset;

        // Re-apply the recovered log start (prunes below-start entries the
        // scan re-indexed).
        let log_start = inner.log_start;
        inner.log_start = 0;
        _ = inner.advance_log_start(log_start);

        debug!(
            ?topition,
            next = inner.next_offset,
            log_start = inner.log_start,
            batches = inner.batches.len(),
            aborted = inner.aborted.len(),
        );
    }

    Ok(state)
}

fn merge_range(detail: &mut TxnDetail, topition: &Topition, offset: i64, offset_end: i64) {
    _ = detail
        .produces
        .entry(topition.topic().to_owned())
        .or_default()
        .entry(topition.partition())
        .and_modify(|entry| {
            let range = entry.get_or_insert(TxnProduceOffset {
                offset_start: offset,
                offset_end,
            });

            if offset_end > range.offset_end {
                range.offset_end = offset_end;
            }

            if offset < range.offset_start {
                range.offset_start = offset;
            }
        })
        .or_insert(Some(TxnProduceOffset {
            offset_start: offset,
            offset_end,
        }));
}

fn apply(
    record: WalRecord,
    coord: &mut CoordState,
    groups: &mut HashMap<String, (GroupDetail, Version)>,
    group_offsets: &mut BTreeMap<(String, Topition), OffsetCommitRequest>,
    skeletons: &mut HashMap<(String, i32), Skeleton>,
) {
    match record {
        WalRecord::TopicCreate { id, topic } => {
            _ = coord.topics.insert(
                topic.name.clone(),
                super::state::TopicMetadata { id, topic },
            );
        }

        WalRecord::TopicDelete { name } => {
            _ = coord.topics.remove(&name);
            skeletons.retain(|(topic, _), _| topic != &name);
            group_offsets.retain(|(_, topition), _| topition.topic() != name);
        }

        WalRecord::ConfigAlter { name, changes } => {
            _ = coord.alter_topic(&name, &changes);
        }

        WalRecord::PidAlloc {
            producer_id,
            transaction_id,
            transaction_timeout_ms,
        } => {
            let mut detail = ProducerDetail::default();
            _ = detail.sequences.insert(0, BTreeMap::new());
            _ = coord.producers.insert(producer_id, detail);

            if let Some(transaction_id) = transaction_id {
                let mut epochs = BTreeMap::new();
                _ = epochs.insert(
                    0,
                    TxnDetail {
                        transaction_timeout_ms,
                        ..Default::default()
                    },
                );

                _ = coord.transactions.insert(
                    transaction_id,
                    Txn {
                        producer: producer_id,
                        epochs,
                    },
                );
            }
        }

        WalRecord::EpochBump {
            transaction_id,
            producer_id,
            producer_epoch,
            transaction_timeout_ms,
        } => {
            if let Some(detail) = coord.producers.get_mut(&producer_id) {
                _ = detail.sequences.insert(producer_epoch, BTreeMap::new());
            }

            if let Some(txn) = coord.transactions.get_mut(&transaction_id) {
                _ = txn.epochs.insert(
                    producer_epoch,
                    TxnDetail {
                        transaction_timeout_ms,
                        ..Default::default()
                    },
                );
            }
        }

        WalRecord::TxnBegin {
            transaction_id,
            producer_id,
            producer_epoch,
            started_at,
            partitions,
        } => {
            if let Some(txn) = coord.transactions.get_mut(&transaction_id)
                && txn.producer == producer_id
                && let Some(detail) = txn.epochs.get_mut(&producer_epoch)
            {
                if detail
                    .state
                    .is_some_and(|state| matches!(state, TxnState::Committed | TxnState::Aborted))
                {
                    detail.produces.clear();
                    detail.offsets.clear();
                }

                for (topic, partition) in partitions {
                    _ = detail
                        .produces
                        .entry(topic)
                        .or_default()
                        .entry(partition)
                        .or_default();
                }

                detail.started_at = Some(started_at);
                detail.state = Some(TxnState::Begin);
            }
        }

        WalRecord::TxnPrepare {
            transaction_id,
            producer_id,
            producer_epoch,
            committed,
            produced,
        } => {
            if let Some(txn) = coord.transactions.get_mut(&transaction_id)
                && txn.producer == producer_id
                && let Some(detail) = txn.epochs.get_mut(&producer_epoch)
            {
                detail.state = Some(if committed {
                    TxnState::PrepareCommit
                } else {
                    TxnState::PrepareAbort
                });

                detail.produces.clear();
                for (topic, partition, range) in produced {
                    _ = detail
                        .produces
                        .entry(topic)
                        .or_default()
                        .insert(partition, Some(range));
                }
            }
        }

        WalRecord::TxnTerminal {
            transaction_id,
            producer_id,
            producer_epoch,
            committed,
            aborted,
        } => {
            if let Some(txn) = coord.transactions.get_mut(&transaction_id)
                && txn.producer == producer_id
                && let Some(detail) = txn.epochs.get_mut(&producer_epoch)
            {
                detail.state = Some(if committed {
                    TxnState::Committed
                } else {
                    TxnState::Aborted
                });
                detail.produces.clear();
                detail.offsets.clear();
                _ = detail.started_at.take();
            }

            for (topic, partition, range) in aborted {
                let skeleton = skeletons.entry((topic, partition)).or_default();

                let aborted = AbortedRange {
                    producer: producer_id,
                    offset_start: range.offset_start,
                    offset_end: range.offset_end,
                };

                // A snapshot may already hold this range (rotation raced the
                // partition-effect push): dedupe.
                if !skeleton.aborted.contains(&aborted) {
                    skeleton.aborted.push(aborted);
                }
            }
        }

        WalRecord::TxnOffsets {
            transaction_id,
            producer_id,
            producer_epoch,
            group,
            offsets,
        } => {
            if let Some(txn) = coord.transactions.get_mut(&transaction_id)
                && txn.producer == producer_id
                && let Some(detail) = txn.epochs.get_mut(&producer_epoch)
            {
                for (topic, partition, co) in offsets {
                    _ = detail
                        .offsets
                        .entry(group.clone())
                        .or_default()
                        .entry(topic)
                        .or_default()
                        .insert(partition, co);
                }
            }
        }

        WalRecord::GroupOffsetCommit { group, offsets } => {
            for (topic, partition, commit) in offsets {
                _ = group_offsets.insert((group.clone(), Topition::new(topic, partition)), commit);
            }
        }

        WalRecord::GroupUpdate {
            group,
            detail,
            version,
        } => {
            _ = groups.insert(group, (detail, version));
        }

        WalRecord::GroupDelete { group } => {
            _ = groups.remove(&group);
            group_offsets.retain(|(g, _), _| g != &group);
        }

        WalRecord::ScramUpsert {
            user,
            mechanism,
            salt,
            iterations,
            stored_key,
            server_key,
        } => {
            _ = coord.scram.insert(
                (user, mechanism),
                crate::ScramCredential {
                    salt: salt.into(),
                    iterations,
                    stored_key: stored_key.into(),
                    server_key: server_key.into(),
                },
            );
        }

        WalRecord::ScramDelete { user, mechanism } => {
            _ = coord.scram.remove(&(user, mechanism));
        }

        WalRecord::LogStartAdvance {
            topic,
            partition,
            offset,
        } => {
            skeletons.entry((topic, partition)).or_default().log_start = offset;
        }
    }
}
