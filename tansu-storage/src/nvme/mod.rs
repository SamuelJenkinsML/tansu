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

//! Local-NVMe append-log storage engine (`nvme://`).
//!
//! Per-partition segment logs + fine-grained in-memory coordination state,
//! durable via a metadata WAL and group-commit fsync; sealed segments tier to
//! an object store. Replaces the dynostore whole-object-rewrite pattern for
//! the local low-latency case: no global coordination object, no CAS loop —
//! the partition is the unit of ownership and parallelism.
//!
//! Semantics are ported from dynostore (the EOS reference backend), with two
//! deliberate divergences: transactions transition to their terminal state
//! immediately at txn_end (no cross-transaction resolution gate — the last
//! stable offset already provides read_committed ordering), and
//! `list_offsets` answers from the batch index (latest = high watermark /
//! last stable offset, timestamps from batch max timestamps).

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    path::PathBuf,
    str::FromStr,
    sync::RwLock,
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use tansu_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    fetch_response::AbortedTransaction,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{Record, deflated, inflated},
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use tansu_schema::Registry;
use tokio::time::Duration;
use tracing::{debug, error, warn};
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, TxnState, UpdateError, Version,
};

mod frame;
mod groupcommit;
mod log;
mod partition;
mod recovery;
mod snapshot;
mod state;
mod tier;
mod wal;

use partition::{AbortedRange, BatchEntry, PartitionState};
use state::{CoordState, ProducerDetail, Topic, Txn, TxnDetail};
use tier::TierStore;
use wal::{Wal, WalRecord};

/// When produce/commit acks: after group-commit fdatasync (`Always`, the
/// default and the only EOS-safe mode), or after the buffered write with a
/// background fsync every interval (`Interval`, for experiments only).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FsyncMode {
    Always,
    Interval(Duration),
}

/// Engine knobs, parsed from the `nvme://` URL query string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub fsync: FsyncMode,
    pub fetch_max_block: Duration,
    pub segment_bytes: u64,
    pub snapshot_wal_bytes: u64,
    pub tail_cache_bytes: u64,
    /// Refuse produces once segments+WAL exceed this (0 = unlimited).
    pub disk_budget_bytes: u64,
    /// Object-store tier for sealed segments and snapshot mirrors, e.g.
    /// `s3://bucket` (credentials/endpoint from the environment). None =
    /// local-only.
    pub tier: Option<String>,
    /// Uploader cadence when a tier is configured.
    pub tier_interval: Duration,
    /// Seal a non-empty active segment past this age so quiet partitions
    /// still tier (the durability window on node death).
    pub segment_age: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            fsync: FsyncMode::Always,
            fetch_max_block: Duration::from_millis(250),
            segment_bytes: 256 * 1024 * 1024,
            snapshot_wal_bytes: 64 * 1024 * 1024,
            tail_cache_bytes: 8 * 1024 * 1024,
            disk_budget_bytes: 0,
            tier: None,
            tier_interval: Duration::from_secs(5),
            segment_age: Duration::from_secs(300),
        }
    }
}

impl Config {
    /// Parse knobs from URL query pairs, e.g.
    /// `nvme:///data?fsync=always&fetch_max_block=100ms&segment_bytes=64M`
    /// (sizes use single-letter `human_units` suffixes: k, M, G).
    /// Unknown keys are ignored (the broker builder passes its own through).
    pub fn from_url(storage: &Url) -> Result<Self> {
        let mut config = Self::default();

        for (k, v) in storage.query_pairs() {
            match k.as_ref() {
                "fsync" => {
                    config.fsync = match v.as_ref() {
                        "always" => FsyncMode::Always,
                        interval => interval
                            .strip_prefix("interval:")
                            .ok_or_else(|| {
                                Error::Message(format!(
                                    "nvme fsync must be `always` or `interval:<duration>`, got: {v}"
                                ))
                            })
                            .and_then(|duration| {
                                human_units::Duration::from_str(duration).map_err(|_| {
                                    Error::Message(format!("nvme fsync interval: {duration}"))
                                })
                            })
                            .map(|duration| FsyncMode::Interval(duration.0))?,
                    }
                }

                "fetch_max_block" => {
                    config.fetch_max_block = human_units::Duration::from_str(v.as_ref())
                        .map(|duration| duration.0)
                        .map_err(|_| Error::Message(format!("nvme fetch_max_block: {v}")))?
                }

                "segment_bytes" => config.segment_bytes = parse_size(&k, &v)?,
                "snapshot_wal_bytes" => config.snapshot_wal_bytes = parse_size(&k, &v)?,
                "tail_cache_bytes" => config.tail_cache_bytes = parse_size(&k, &v)?,
                "disk_budget" => config.disk_budget_bytes = parse_size(&k, &v)?,
                "tier" => config.tier = Some(v.to_string()),
                "tier_interval" => {
                    config.tier_interval = human_units::Duration::from_str(v.as_ref())
                        .map(|duration| duration.0)
                        .map_err(|_| Error::Message(format!("nvme tier_interval: {v}")))?
                }
                "segment_age" => {
                    config.segment_age = human_units::Duration::from_str(v.as_ref())
                        .map(|duration| duration.0)
                        .map_err(|_| Error::Message(format!("nvme segment_age: {v}")))?
                }

                _otherwise => (),
            }
        }

        debug!(?config);

        Ok(config)
    }
}

fn parse_size(key: &str, value: &str) -> Result<u64> {
    human_units::Size::from_str(value)
        .map(|size| size.0)
        .map_err(|_| Error::Message(format!("nvme {key}: {value}")))
}

fn timestamp_to_system_time(timestamp_ms: i64) -> Option<SystemTime> {
    u64::try_from(timestamp_ms)
        .ok()
        .map(|millis| UNIX_EPOCH + StdDuration::from_millis(millis))
}

/// The `nvme://` storage engine.
#[derive(Debug)]
pub struct Engine {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    data_dir: PathBuf,
    schemas: Option<Registry>,
    config: Config,

    /// Producers, transactions, topics, SCRAM: small, fine-grained state
    /// mutated in sub-microsecond critical sections. Never held across I/O.
    coord: tokio::sync::Mutex<CoordState>,
    /// Per-partition log state; the map itself is read-mostly (written only
    /// by topic create/delete), each partition guards itself. Shared with
    /// the tier uploader task.
    partitions: std::sync::Arc<RwLock<HashMap<Topition, std::sync::Arc<PartitionState>>>>,
    /// Consumer-group state with optimistic-concurrency versions
    /// (the broker-side group coordinator drives this via update_group).
    groups: RwLock<HashMap<String, (GroupDetail, Version)>>,
    /// Committed consumer offsets, keyed by (group, topic partition).
    group_offsets: RwLock<BTreeMap<(String, Topition), OffsetCommitRequest>>,
    /// The metadata WAL for this boot epoch; swapped at snapshot time.
    /// Shared with the tier uploader (WAL mirroring).
    wal: std::sync::Arc<RwLock<Wal>>,
    /// Approximate bytes in segments + WAL, for the disk-budget guard.
    disk_usage: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// The object-store tier, when configured.
    tier: Option<std::sync::Arc<TierStore>>,
    /// The background uploader; aborted when the engine drops.
    tier_task: Option<tokio::task::JoinHandle<()>>,
    /// Exclusive data-dir lock, held for the engine's lifetime.
    _lock: std::fs::File,
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(task) = self.tier_task.take() {
            task.abort();
        }
    }
}

impl Engine {
    /// Recover (or initialize) the data dir and open the engine. The caller
    /// must run [`Engine::finish_boot`] before serving traffic.
    pub async fn open(
        cluster: impl Into<String>,
        node: i32,
        data_dir: impl Into<PathBuf>,
        advertised_listener: Url,
        schemas: Option<Registry>,
        config: Config,
    ) -> Result<Self> {
        let cluster = cluster.into();
        let data_dir = data_dir.into();

        let tier = config
            .tier
            .as_deref()
            .map(|tier| TierStore::open(tier, &cluster).map(std::sync::Arc::new))
            .transpose()?;

        let recovered =
            recovery::recover(&data_dir, &cluster, config.fsync, tier.as_deref()).await?;

        let partitions = std::sync::Arc::new(RwLock::new(recovered.partitions));
        let disk_usage = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let wal = std::sync::Arc::new(RwLock::new(recovered.wal));

        // The uploader tiers sealed segments (evicting uploaded ones when
        // the disk budget tightens) and mirrors the active WAL, so node
        // death loses at most one tier interval of coordination state.
        let tier_task = tier.as_ref().map(|tier| {
            tokio::spawn(tier::tiering_loop(
                partitions.clone(),
                tier.clone(),
                disk_usage.clone(),
                config.disk_budget_bytes,
                config.tier_interval,
                config.segment_age,
                wal.clone(),
            ))
        });

        let engine = Self {
            cluster,
            node,
            advertised_listener,
            data_dir,
            schemas,
            config,
            coord: tokio::sync::Mutex::new(recovered.coord),
            partitions,
            groups: RwLock::new(recovered.groups),
            group_offsets: RwLock::new(recovered.group_offsets),
            wal,
            disk_usage,
            tier,
            tier_task,
            _lock: recovered.lock,
        };

        // Seed the disk-budget accounting from the recovered indexes
        // (tiered entries hold no local bytes).
        let mut usage = 0u64;
        for state in engine.partitions.read()?.values() {
            let inner = state.inner.lock()?;
            usage += inner
                .batches
                .values()
                .filter(|entry| !entry.tiered)
                .map(|entry| u64::from(entry.len))
                .sum::<u64>();
        }
        engine
            .disk_usage
            .store(usage, std::sync::atomic::Ordering::Relaxed);

        engine.finish_boot(recovered.in_doubt).await?;

        Ok(engine)
    }

    /// Complete recovery: finish resolving prepared transactions (writing
    /// any missing end-txn markers), then snapshot so the replayed WAL
    /// files retire.
    async fn finish_boot(&self, in_doubt: Vec<(String, i64, i16, bool)>) -> Result<()> {
        for (transaction_id, producer_id, producer_epoch, committed) in in_doubt {
            let produced: Vec<(Topition, bool)> = {
                let coord = self.coord.lock().await;

                coord
                    .produced(&transaction_id, producer_epoch)
                    .into_iter()
                    .map(|(topic, partition, range)| {
                        let topition = Topition::new(topic, partition);
                        let marker_present = self
                            .partition_if_exists(&topition)
                            .ok()
                            .flatten()
                            .and_then(|state| {
                                state.inner.lock().ok().map(|inner| {
                                    inner.batches.range(range.offset_start..).any(|(_, entry)| {
                                        entry.is_control && entry.producer_id == producer_id
                                    })
                                })
                            })
                            .unwrap_or(false);

                        (topition, marker_present)
                    })
                    .collect()
            };

            for (topition, marker_present) in &produced {
                if !marker_present {
                    debug!(
                        transaction_id,
                        ?topition,
                        committed,
                        "boot: writing missing marker"
                    );
                    _ = self
                        .produce_marker(
                            &transaction_id,
                            producer_id,
                            producer_epoch,
                            committed,
                            topition,
                        )
                        .await
                        .inspect_err(|err| error!(?err, transaction_id, ?topition))?;
                }
            }

            // Resolution (or deferral behind a still-open peer) through the
            // normal path; phase 1 is a no-op on a prepared txn.
            _ = self
                .txn_end(&transaction_id, producer_id, producer_epoch, committed)
                .await
                .inspect_err(|err| error!(?err, transaction_id))?;
        }

        self.snapshot_now().await
    }

    /// Rotate the WAL and persist a snapshot capturing everything up to the
    /// rotated file, then retire old WAL files and snapshots.
    async fn snapshot_now(&self) -> Result<()> {
        let wal_dir = self.data_dir.join("wal");
        let snapshots_dir = self.data_dir.join("snapshots");

        // Hold coord across rotation + capture so no WAL record lands in
        // both the retired files and the snapshot. (Group/offset writes take
        // their own locks inside the capture; skew there is tolerated —
        // replay of a duplicated record is idempotent.)
        let coord = self.coord.lock().await;

        let old = {
            let mut wal = self.wal.write()?;
            let next = Wal::create(&wal_dir, wal.seq + 1, self.config.fsync)?;
            std::mem::replace(&mut *wal, next)
        };

        let retired_seq = old.seq;

        let partitions: Vec<snapshot::SnapPartition> = {
            let partitions = self.partitions.read()?;
            let mut snaps = Vec::with_capacity(partitions.len());

            for (topition, state) in partitions.iter() {
                let inner = state.inner.lock()?;
                snaps.push(snapshot::SnapPartition {
                    topic: topition.topic().to_owned(),
                    partition: topition.partition(),
                    log_start: inner.log_start,
                    aborted: inner.aborted.clone(),
                });
            }

            snaps
        };

        let doc = {
            let groups = self.groups.read()?;
            let group_offsets = self.group_offsets.read()?;

            snapshot::SnapshotDoc::from_state(
                retired_seq,
                &coord,
                &groups,
                &group_offsets,
                partitions,
            )
        };

        drop(coord);

        let seal = old.seal()?;
        seal.await
            .map_err(|_| Error::Message("nvme wal seal dropped".into()))??;

        let written = snapshot::write(&snapshots_dir, &doc)?;

        // Mirror the snapshot to the tier: the coordination state a cold
        // bootstrap starts from. Mirrored WALs it captures then retire.
        if let Some(ref tier) = self.tier {
            let framed = std::fs::read(&written)
                .map(Bytes::from)
                .map_err(|err| Error::Message(format!("nvme snapshot reread: {err}")))?;

            _ = tier
                .upload_snapshot(retired_seq, framed)
                .await
                .inspect_err(|err| warn!(?err, "tier snapshot mirror"));

            _ = tier
                .delete_wals_below(retired_seq)
                .await
                .inspect_err(|err| warn!(?err, "tier wal retire"));
        }

        for seq in wal::wal_seqs(&wal_dir)? {
            if seq <= retired_seq {
                let path = wal_dir.join(format!("{seq:020}.{}", wal::WAL_SUFFIX));
                let reclaimed = std::fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
                _ = std::fs::remove_file(&path).inspect_err(|err| warn!(seq, ?err, "wal retire"));
                _ = self
                    .disk_usage
                    .fetch_sub(reclaimed, std::sync::atomic::Ordering::Relaxed);
            }
        }

        snapshot::prune(&snapshots_dir)?;

        debug!(retired_seq, "snapshot complete");

        Ok(())
    }

    /// Kafka-style deletion retention: drop whole sealed segments whose
    /// newest record is older than `retention.ms`, or oldest-first while the
    /// partition exceeds `retention.bytes`; the log start advances to the
    /// next segment boundary. The active segment is never deleted.
    async fn apply_retention(&self, now: SystemTime) -> Result<()> {
        const DEFAULT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;

        let retention: HashMap<String, (i64, i64)> = {
            let coord = self.coord.lock().await;

            coord
                .topics
                .iter()
                .map(|(name, metadata)| {
                    let mut ms = DEFAULT_RETENTION_MS;
                    let mut bytes = -1i64;

                    for config in metadata.topic.configs.as_deref().unwrap_or_default() {
                        match (config.name.as_str(), config.value.as_deref()) {
                            ("retention.ms", Some(value)) => ms = value.parse().unwrap_or(ms),
                            ("retention.bytes", Some(value)) => {
                                bytes = value.parse().unwrap_or(bytes)
                            }
                            _otherwise => {}
                        }
                    }

                    (name.clone(), (ms, bytes))
                })
                .collect()
        };

        let now_ms = now
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as i64)
            .unwrap_or(0);

        let partitions: Vec<(Topition, std::sync::Arc<PartitionState>)> = self
            .partitions
            .read()?
            .iter()
            .map(|(topition, state)| (topition.clone(), state.clone()))
            .collect();

        for (topition, state) in partitions {
            let Some((ms, bytes)) = retention.get(topition.topic()).copied() else {
                continue;
            };

            if ms < 0 && bytes < 0 {
                continue;
            }

            let (new_start, files): (i64, Vec<PathBuf>) = {
                let mut inner = state.inner.lock()?;

                let active = inner.writer.as_ref().map(|writer| writer.base_offset);

                // Sealed-segment stats from the index: newest timestamp and
                // on-disk bytes per segment.
                let mut segments: BTreeMap<i64, (i64, u64)> = BTreeMap::new();

                for entry in inner.batches.values() {
                    if Some(entry.segment_base) == active {
                        continue;
                    }

                    let segment = segments.entry(entry.segment_base).or_insert((i64::MIN, 0));
                    segment.0 = segment.0.max(entry.max_timestamp);
                    segment.1 += u64::from(entry.len);
                }

                let mut total: u64 = segments.values().map(|(_, bytes)| bytes).sum::<u64>()
                    + inner.writer.as_ref().map_or(0, |writer| writer.position);

                let bases: Vec<i64> = segments.keys().copied().collect();
                let mut new_start = inner.log_start;

                for (index, base) in bases.iter().enumerate() {
                    let (max_ts, seg_bytes) = segments[base];

                    // Deleting a segment moves the log start to where the
                    // next one (or the active tail) begins.
                    let boundary = bases
                        .get(index + 1)
                        .copied()
                        .or(active)
                        .unwrap_or(inner.next_offset);

                    let expired = ms >= 0 && max_ts >= 0 && max_ts < now_ms - ms;
                    let oversize = bytes >= 0 && total > bytes as u64;

                    if expired || oversize {
                        new_start = new_start.max(boundary);
                        total = total.saturating_sub(seg_bytes);
                    } else {
                        break;
                    }
                }

                if new_start <= inner.log_start {
                    continue;
                }

                _ = inner.advance_log_start(new_start);

                let mut files = vec![];
                for base in log::segment_bases(&inner.dir)? {
                    if base < new_start && Some(base) != active {
                        _ = inner.read_files.remove(&base);
                        files.push(log::segment_path(&inner.dir, base));
                    }
                }

                (inner.log_start, files)
            };

            if files.is_empty() {
                continue;
            }

            for path in &files {
                let reclaimed = std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
                _ = std::fs::remove_file(path)
                    .inspect_err(|err| warn!(?path, ?err, "retention delete"));
                _ = self
                    .disk_usage
                    .fetch_sub(reclaimed, std::sync::atomic::Ordering::Relaxed);
            }

            debug!(?topition, new_start, deleted = files.len(), "retention");

            self.wal_append(WalRecord::LogStartAdvance {
                topic: topition.topic().to_owned(),
                partition: topition.partition(),
                offset: new_start,
            })
            .await?;

            if let Some(ref tier) = self.tier {
                _ = tier
                    .delete_segments_below(&topition, new_start)
                    .await
                    .inspect_err(|err| warn!(?err, ?topition, "tier retention"));
            }
        }

        Ok(())
    }

    /// Append one WAL record and wait until it is durable.
    async fn wal_append(&self, record: WalRecord) -> Result<()> {
        let ack = {
            let wal = self.wal.read()?;
            let (ack, bytes) = wal.append(&record)?;
            _ = self
                .disk_usage
                .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
            ack
        };

        ack.await
            .map_err(|_| Error::Message("nvme wal flusher dropped".into()))?
    }

    /// The partition's shared state, created on demand: like dynostore,
    /// produce to a topic without metadata still allocates log state.
    fn partition(&self, topition: &Topition) -> Result<std::sync::Arc<PartitionState>> {
        if let Some(partition) = self.partitions.read()?.get(topition) {
            return Ok(partition.clone());
        }

        let dir = self.data_dir.join("topics").join(PathBuf::from(topition));

        std::fs::create_dir_all(&dir)
            .map_err(|err| Error::Message(format!("nvme create {dir:?}: {err}")))?;

        let mut partitions = self.partitions.write()?;
        Ok(partitions
            .entry(topition.clone())
            .or_insert_with(|| std::sync::Arc::new(PartitionState::new(dir)))
            .clone())
    }

    fn partition_if_exists(
        &self,
        topition: &Topition,
    ) -> Result<Option<std::sync::Arc<PartitionState>>> {
        Ok(self.partitions.read()?.get(topition).cloned())
    }

    /// Produce a control (end-txn marker) batch to one partition, dynostore
    /// txn_end marker construction.
    async fn produce_marker(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        topition: &Topition,
    ) -> Result<i64> {
        let control_batch: Bytes = if committed {
            ControlBatch::default().commit().try_into()?
        } else {
            ControlBatch::default().abort().try_into()?
        };

        let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

        let batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(control_batch.into())
                    .value(end_transaction_marker.into()),
            )
            .attributes(
                BatchAttribute::default()
                    .control(true)
                    .transaction(true)
                    .into(),
            )
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_sequence(-1)
            .build()
            .and_then(TryInto::try_into)?;

        self.produce(Some(transaction_id), topition, batch).await
    }

    fn txn_add_partitions_response(
        topics: &[tansu_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic],
        error_code: ErrorCode,
    ) -> TxnAddPartitionsResponse {
        let mut results = vec![];

        for topic in topics {
            let mut results_by_partition = vec![];

            for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                results_by_partition.push(
                    AddPartitionsToTxnPartitionResult::default()
                        .partition_index(*partition_index)
                        .partition_error_code(error_code.into()),
                );
            }

            results.push(
                AddPartitionsToTxnTopicResult::default()
                    .name(topic.name.clone())
                    .results_by_partition(Some(results_by_partition)),
            )
        }

        TxnAddPartitionsResponse::VersionZeroToThree(results)
    }

    fn txn_offset_commit_response_error(
        offsets: &TxnOffsetCommitRequest,
        error_code: ErrorCode,
    ) -> Vec<TxnOffsetCommitResponseTopic> {
        let mut responses = vec![];

        for topic in &offsets.topics {
            let mut partition_responses = vec![];

            if let Some(partitions) = topic.partitions.as_deref() {
                for partition in partitions {
                    partition_responses.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(error_code.into()),
                    );
                }
            }

            responses.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name.to_string())
                    .partitions(Some(partition_responses)),
            );
        }

        responses
    }

    fn metadata_response_topic(&self, metadata: &state::TopicMetadata) -> MetadataResponseTopic {
        let error_code = ErrorCode::None.into();
        let partitions = Some(
            (0..metadata.topic.num_partitions)
                .map(|partition_index| {
                    let replica_nodes = Some(vec![
                        self.node;
                        metadata.topic.replication_factor.max(0) as usize
                    ]);

                    MetadataResponsePartition::default()
                        .error_code(error_code)
                        .partition_index(partition_index)
                        .leader_id(self.node)
                        .leader_epoch(Some(0))
                        .replica_nodes(replica_nodes.clone())
                        .isr_nodes(replica_nodes)
                        .offline_replicas(Some([].into()))
                })
                .collect(),
        );

        MetadataResponseTopic::default()
            .error_code(error_code)
            .name(Some(metadata.topic.name.clone()))
            .topic_id(Some(metadata.id.into_bytes()))
            .is_internal(Some(false))
            .partitions(partitions)
            .topic_authorized_operations(Some(-2147483648))
    }
}

#[async_trait]
impl Storage for Engine {
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        Ok(())
    }

    async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        let id = {
            let mut coord = self.coord.lock().await;

            if coord.topics.contains_key(topic.name.as_str()) {
                return Err(Error::Api(ErrorCode::TopicAlreadyExists));
            }

            let id = Uuid::now_v7();
            _ = coord.topics.insert(
                topic.name.clone(),
                state::TopicMetadata {
                    id,
                    topic: topic.clone(),
                },
            );

            id
        };

        for partition in 0..topic.num_partitions {
            _ = self.partition(&Topition::new(topic.name.as_str(), partition))?;
        }

        self.wal_append(WalRecord::TopicCreate {
            id,
            topic: topic.clone(),
        })
        .await?;

        Ok(id)
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let response = AlterConfigsResourceResponse::default()
            .error_code(ErrorCode::None.into())
            .error_message(Some("".into()))
            .resource_type(resource.resource_type)
            .resource_name(resource.resource_name.clone());

        if let ConfigResource::Topic = ConfigResource::from(resource.resource_type) {
            let changes = resource.configs.clone().unwrap_or_default();

            self.coord
                .lock()
                .await
                .alter_topic(resource.resource_name.as_str(), &changes)?;

            self.wal_append(WalRecord::ConfigAlter {
                name: resource.resource_name.clone(),
                changes,
            })
            .await?;
        }

        Ok(response)
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let known: std::collections::BTreeSet<Topic> = {
            let coord = self.coord.lock().await;
            coord.topics.keys().cloned().collect()
        };

        let mut results = vec![];

        for topic in topics {
            let mut partition_results = vec![];

            for partition in topic.partitions.as_deref().unwrap_or_default() {
                let topition = Topition::new(topic.name.as_str(), partition.partition_index);

                let result = if !known.contains(topic.name.as_str()) {
                    DeleteRecordsPartitionResult::default()
                        .partition_index(partition.partition_index)
                        .low_watermark(-1)
                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                } else if let Some(state) = self.partition_if_exists(&topition)? {
                    let advanced = {
                        let mut inner = state.inner.lock()?;

                        // -1 means truncate to the high watermark.
                        let target = if partition.offset < 0 {
                            inner.high_watermark()
                        } else {
                            partition.offset
                        };

                        if target > inner.high_watermark() {
                            None
                        } else {
                            Some(inner.advance_log_start(target))
                        }
                    };

                    match advanced {
                        None => DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(-1)
                            .error_code(ErrorCode::OffsetOutOfRange.into()),

                        Some(low_watermark) => {
                            self.wal_append(WalRecord::LogStartAdvance {
                                topic: topic.name.clone(),
                                partition: partition.partition_index,
                                offset: low_watermark,
                            })
                            .await?;

                            DeleteRecordsPartitionResult::default()
                                .partition_index(partition.partition_index)
                                .low_watermark(low_watermark)
                                .error_code(ErrorCode::None.into())
                        }
                    }
                } else {
                    DeleteRecordsPartitionResult::default()
                        .partition_index(partition.partition_index)
                        .low_watermark(-1)
                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                };

                partition_results.push(result);
            }

            results.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_results)),
            );
        }

        Ok(results)
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let name = {
            let mut coord = self.coord.lock().await;

            let Some(name) = coord
                .topic_metadata(topic)
                .map(|metadata| metadata.topic.name.clone())
            else {
                return Ok(ErrorCode::UnknownTopicOrPartition);
            };

            _ = coord.topics.remove(name.as_str());

            name
        };

        // Remove the log directories too: recovery must not resurrect a
        // deleted topic's data from its segments.
        let removed: Vec<_> = {
            let mut partitions = self.partitions.write()?;
            let removed = partitions
                .iter()
                .filter(|(topition, _)| topition.topic() == name)
                .map(|(topition, state)| (topition.clone(), state.clone()))
                .collect();
            partitions.retain(|topition, _| topition.topic() != name);
            removed
        };

        for (_, state) in removed {
            let (dir, bytes) = {
                let inner = state.inner.lock()?;
                let bytes = inner
                    .batches
                    .values()
                    .map(|entry| u64::from(entry.len))
                    .sum::<u64>();
                (inner.dir.clone(), bytes)
            };

            _ = std::fs::remove_dir_all(&dir).inspect_err(|err| warn!(?dir, ?err));
            _ = self
                .disk_usage
                .fetch_sub(bytes, std::sync::atomic::Ordering::Relaxed);
        }

        self.group_offsets
            .write()?
            .retain(|(_, topition), _| topition.topic() != name);

        self.wal_append(WalRecord::TopicDelete { name }).await?;

        Ok(ErrorCode::None)
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(self.node)
                .host(
                    self.advertised_listener
                        .host_str()
                        .unwrap_or("0.0.0.0")
                        .into(),
                )
                .port(self.advertised_listener.port().unwrap_or(9092).into())
                .rack(None),
        ])
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let attributes = BatchAttribute::try_from(deflated.attributes)?;

        // Disk-budget guard: refuse rather than fill the volume — before the
        // sequence bump, so a refused produce can be retried verbatim.
        // Retention and topic deletion reclaim budget.
        if self.config.disk_budget_bytes > 0
            && self.disk_usage.load(std::sync::atomic::Ordering::Relaxed)
                >= self.config.disk_budget_bytes
        {
            warn!(
                ?topition,
                budget = self.config.disk_budget_bytes,
                "disk budget exhausted"
            );
            return Err(Error::Api(ErrorCode::KafkaStorageError));
        }

        // Coordination checks: idempotent sequence validation (bumping the
        // stored sequence) and, for transactional data batches, an open-txn
        // check so a swept transaction cannot keep producing.
        if deflated.is_idempotent() || (transaction_id.is_some() && attributes.transaction) {
            let mut coord = self.coord.lock().await;

            if deflated.is_idempotent() {
                coord
                    .validate_sequence(
                        deflated.producer_id,
                        deflated.producer_epoch,
                        deflated.base_sequence,
                        deflated.last_offset_delta,
                        topition.topic(),
                        topition.partition(),
                    )
                    .inspect_err(|err| {
                        if !matches!(
                            err,
                            Error::Api(ErrorCode::OutOfOrderSequenceNumber)
                                | Error::Api(ErrorCode::DuplicateSequenceNumber)
                        ) {
                            error!(?err, transaction_id, ?topition);
                        }
                    })?;
            }

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
                && !attributes.control
                && let Some(transaction) = coord.transactions.get(transaction_id)
                && let Some(txn_detail) = transaction.epochs.get(&deflated.producer_epoch)
                && txn_detail.state != Some(TxnState::Begin)
            {
                return Err(Error::Api(ErrorCode::InvalidTxnState));
            }
        }

        if let Some(ref registry) = self.schemas
            && !attributes.control
        {
            let inflated = inflated::Batch::try_from(&deflated)?;
            registry
                .validate(topition.topic(), &inflated)
                .await
                .inspect_err(|err| debug!(?err))?;
        }

        let partition = self.partition(topition)?;

        let producer_id = deflated.producer_id;
        let producer_epoch = deflated.producer_epoch;
        let base_sequence = deflated.base_sequence;
        let last_offset_delta = deflated.last_offset_delta;
        let max_timestamp = deflated.max_timestamp;

        let batch_bytes = Bytes::from(deflated);

        let (offset, offset_end, ack) = {
            let mut inner = partition.inner.lock()?;

            let offset = inner.next_offset;
            let offset_end = offset + i64::from(last_offset_delta);
            inner.next_offset = offset_end + 1;

            let framed = log::encode_batch(offset, &batch_bytes);

            // Create the active segment on first produce (one per boot
            // epoch) and rotate it at the size threshold; the sealed file
            // moves to the read handles.
            if inner
                .writer
                .as_ref()
                .is_none_or(|writer| writer.position >= self.config.segment_bytes)
            {
                if let Some(old) = inner.writer.take() {
                    _ = old.flusher.seal();
                    _ = inner.read_files.insert(old.base_offset, old.file);
                }

                let name = format!("{}-{}", topition.topic(), topition.partition());
                let writer =
                    log::SegmentWriter::create(&inner.dir, &name, offset, self.config.fsync)?;
                inner.writer = Some(writer);
            }

            let writer = inner.writer.as_mut().expect("active segment");
            let segment_base = writer.base_offset;
            let position = writer.append(&framed)?;
            let ack = writer.flusher.sync()?;

            _ = self
                .disk_usage
                .fetch_add(framed.len() as u64, std::sync::atomic::Ordering::Relaxed);

            inner.index(
                offset,
                BatchEntry {
                    segment_base,
                    position,
                    len: framed.len() as u32,
                    tiered: false,
                    cached: Some(batch_bytes),
                    last_offset_delta,
                    max_timestamp,
                    is_control: attributes.control,
                    is_transactional: attributes.transaction,
                    producer_id,
                    producer_epoch,
                    base_sequence,
                },
                self.config.tail_cache_bytes,
            );

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
                && !attributes.control
            {
                _ = inner
                    .open_txns
                    .entry((transaction_id.to_owned(), producer_id, producer_epoch))
                    .or_insert(offset);
            }

            (offset, offset_end, ack)
        };

        // Ack only after the group-commit fsync: the high watermark must
        // never expose data a crash would lose.
        ack.await
            .map_err(|_| Error::Message("nvme segment flusher dropped".into()))??;

        {
            let mut inner = partition.inner.lock()?;
            inner.durable_offset = inner.durable_offset.max(offset_end + 1);
        }

        if let Some(transaction_id) = transaction_id
            && attributes.transaction
        {
            let mut coord = self.coord.lock().await;
            coord.record_txn_produce(
                transaction_id,
                producer_epoch,
                topition.topic(),
                topition.partition(),
                offset,
                offset_end,
                attributes.control,
            )?;
        }

        partition.notify.notify_waiters();

        Ok(offset)
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        _min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let Some(partition) = self.partition_if_exists(topition)? else {
            return Ok(vec![]);
        };

        let deadline = tokio::time::Instant::now() + max_wait.min(self.config.fetch_max_block);

        enum Pending {
            Cached(i64, Bytes),
            Disk(i64, std::sync::Arc<std::fs::File>, u64, u32),
            Remote(i64, i64, u64, u32),
        }

        loop {
            let notified = partition.notify.notified();
            tokio::pin!(notified);

            let pending: Vec<Pending> = {
                let mut inner = partition.inner.lock()?;

                let limit = match isolation {
                    IsolationLevel::ReadCommitted => inner.last_stable(),
                    IsolationLevel::ReadUncommitted => inner.high_watermark(),
                };

                if offset < limit {
                    let mut wanted: Vec<(i64, BatchEntry)> = vec![];

                    // The fetch offset can fall inside a batch that starts
                    // before it; Kafka returns that batch whole, leaving the
                    // client to skip records below the fetch offset.
                    if let Some((base, entry)) = inner.batches.range(..offset).next_back()
                        && !inner.batches.contains_key(&offset)
                        && *base + i64::from(entry.last_offset_delta) >= offset
                        && *base >= inner.log_start
                    {
                        wanted.push((*base, entry.clone()));
                    }

                    let mut budget = u64::from(max_bytes);
                    let mut stop = false;

                    let in_range: Vec<(i64, BatchEntry)> = inner
                        .batches
                        .range(offset..limit)
                        .map(|(base, entry)| (*base, entry.clone()))
                        .collect();

                    for (base, entry) in in_range {
                        if stop {
                            break;
                        }

                        let size = u64::from(entry.len);
                        wanted.push((base, entry));

                        // At least one batch is always returned; stop once
                        // the size budget is exceeded (dynostore semantics).
                        if size > budget {
                            stop = true;
                        }
                        budget = budget.saturating_sub(size);
                    }

                    let mut pending = Vec::with_capacity(wanted.len());

                    for (base, entry) in wanted {
                        match (entry.cached, entry.tiered) {
                            (Some(bytes), _) => pending.push(Pending::Cached(base, bytes)),
                            (None, true) => pending.push(Pending::Remote(
                                base,
                                entry.segment_base,
                                entry.position,
                                entry.len,
                            )),
                            (None, false) => {
                                let file = inner.read_handle(entry.segment_base)?;
                                pending.push(Pending::Disk(base, file, entry.position, entry.len));
                            }
                        }
                    }

                    pending
                } else {
                    vec![]
                }
            };

            if !pending.is_empty() {
                let mut batches = Vec::with_capacity(pending.len());

                for item in pending {
                    let (base_offset, bytes) = match item {
                        Pending::Cached(base, bytes) => (base, bytes),
                        Pending::Disk(base, file, position, len) => {
                            (base, log::read_batch_at(&file, position, len)?)
                        }
                        Pending::Remote(base, segment_base, position, len) => {
                            let tier = self.tier.as_ref().ok_or_else(|| {
                                Error::Message("nvme: tiered batch without a tier".into())
                            })?;

                            (
                                base,
                                tier.read_batch(topition, segment_base, position, len)
                                    .await?,
                            )
                        }
                    };

                    let mut batch = deflated::Batch::try_from(bytes)?;
                    batch.base_offset = base_offset;
                    batches.push(batch);
                }

                return Ok(batches);
            }

            // Nothing readable at this offset yet: bounded wait for the
            // durable high watermark (or the last stable offset) to advance.
            tokio::select! {
                _ = &mut notified => continue,
                _ = tokio::time::sleep_until(deadline) => return Ok(vec![]),
            }
        }
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let Some(partition) = self.partition_if_exists(topition)? else {
            return Ok(OffsetStage::default());
        };

        let inner = partition.inner.lock()?;

        Ok(OffsetStage {
            last_stable: inner.last_stable(),
            high_watermark: inner.high_watermark(),
            log_start: inner.log_start,
        })
    }

    async fn aborted_transactions(
        &self,
        topition: &Topition,
        fetch_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        let Some(partition) = self.partition_if_exists(topition)? else {
            return Ok(vec![]);
        };

        let inner = partition.inner.lock()?;

        let mut aborted: Vec<_> = inner
            .aborted
            .iter()
            .filter(|range| range.offset_end >= fetch_offset)
            .map(|range| {
                AbortedTransaction::default()
                    .producer_id(range.producer)
                    .first_offset(range.offset_start)
            })
            .collect();

        // read_committed clients expect ascending first_offset order.
        aborted.sort_by_key(|aborted| aborted.first_offset);

        Ok(aborted)
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let mut responses = vec![];

        for (topition, offset_request) in offsets {
            let response = if let Some(partition) = self.partition_if_exists(topition)? {
                let inner = partition.inner.lock()?;

                let limit = match isolation_level {
                    IsolationLevel::ReadCommitted => inner.last_stable(),
                    IsolationLevel::ReadUncommitted => inner.high_watermark(),
                };

                match offset_request {
                    ListOffset::Earliest => {
                        let timestamp = inner
                            .batches
                            .range(inner.log_start..)
                            .next()
                            .and_then(|(_, entry)| timestamp_to_system_time(entry.max_timestamp));

                        ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: Some(inner.log_start),
                            timestamp,
                        }
                    }

                    ListOffset::Latest => {
                        let timestamp = inner
                            .batches
                            .range(..limit)
                            .next_back()
                            .and_then(|(_, entry)| timestamp_to_system_time(entry.max_timestamp));

                        ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: Some(limit),
                            timestamp,
                        }
                    }

                    ListOffset::Timestamp(want) => {
                        // The earliest batch whose max timestamp reaches the
                        // requested time.
                        let found = inner
                            .batches
                            .range(inner.log_start..limit)
                            .find(|(_, entry)| {
                                timestamp_to_system_time(entry.max_timestamp)
                                    .is_some_and(|timestamp| timestamp >= *want)
                            })
                            .map(|(base, entry)| (*base, entry.max_timestamp));

                        // No matching batch (or an empty partition) answers
                        // offset 0 with no timestamp, as dynostore does.
                        ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: found.map(|(base, _)| base).or(Some(0)),
                            timestamp: found
                                .and_then(|(_, timestamp)| timestamp_to_system_time(timestamp)),
                        }
                    }
                }
            } else {
                ListOffsetResponse {
                    error_code: ErrorCode::None,
                    offset: Some(0),
                    ..Default::default()
                }
            };

            responses.push((topition.to_owned(), response));
        }

        Ok(responses)
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        _retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let known: std::collections::BTreeSet<Topic> = {
            let coord = self.coord.lock().await;
            coord.topics.keys().cloned().collect()
        };

        let mut responses = vec![];
        let mut accepted = vec![];

        {
            let mut group_offsets = self.group_offsets.write()?;

            for (topition, offset_commit) in offsets {
                if known.contains(topition.topic()) {
                    _ = group_offsets.insert(
                        (group_id.to_owned(), topition.to_owned()),
                        offset_commit.to_owned(),
                    );
                    accepted.push((
                        topition.topic().to_owned(),
                        topition.partition(),
                        offset_commit.to_owned(),
                    ));
                    responses.push((topition.to_owned(), ErrorCode::None));
                } else {
                    responses.push((topition.to_owned(), ErrorCode::UnknownTopicOrPartition));
                }
            }
        }

        if !accepted.is_empty() {
            self.wal_append(WalRecord::GroupOffsetCommit {
                group: group_id.to_owned(),
                offsets: accepted,
            })
            .await?;
        }

        Ok(responses)
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let mut responses = BTreeMap::new();

        if let Some(group_id) = group_id {
            let group_offsets = self.group_offsets.read()?;

            for topition in topics {
                let offset = group_offsets
                    .get(&(group_id.to_owned(), topition.to_owned()))
                    .map_or(-1, |commit| commit.offset);

                _ = responses.insert(topition.to_owned(), offset);
            }
        }

        Ok(responses)
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let group_offsets = self.group_offsets.read()?;

        Ok(group_offsets
            .iter()
            .filter(|((group, _), _)| group == group_id)
            .map(|((_, topition), commit)| (topition.to_owned(), commit.offset))
            .collect())
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let brokers = vec![
            MetadataResponseBroker::default()
                .node_id(self.node)
                .host(
                    self.advertised_listener
                        .host_str()
                        .unwrap_or("0.0.0.0")
                        .into(),
                )
                .port(self.advertised_listener.port().unwrap_or(9092).into())
                .rack(None),
        ];

        let coord = self.coord.lock().await;

        let responses = match topics {
            Some(topics) if !topics.is_empty() => topics
                .iter()
                .map(|topic| match coord.topic_metadata(topic) {
                    Some(metadata) => self.metadata_response_topic(metadata),

                    None => MetadataResponseTopic::default()
                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                        .name(match topic {
                            TopicId::Name(name) => Some(name.into()),
                            TopicId::Id(_) => None,
                        })
                        .topic_id(Some(match topic {
                            TopicId::Name(_) => NULL_TOPIC_ID,
                            TopicId::Id(id) => id.into_bytes(),
                        }))
                        .is_internal(Some(false))
                        .partitions(Some([].into()))
                        .topic_authorized_operations(Some(-2147483648)),
                })
                .collect(),

            _ => coord
                .topics
                .values()
                .map(|metadata| self.metadata_response_topic(metadata))
                .collect(),
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let record = WalRecord::ScramUpsert {
            user: user.to_owned(),
            mechanism: format!("{mechanism:?}"),
            salt: credential.salt.to_vec(),
            iterations: credential.iterations,
            stored_key: credential.stored_key.to_vec(),
            server_key: credential.server_key.to_vec(),
        };

        {
            let mut coord = self.coord.lock().await;
            _ = coord
                .scram
                .insert((user.to_owned(), format!("{mechanism:?}")), credential);
        }

        self.wal_append(record).await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        {
            let mut coord = self.coord.lock().await;
            _ = coord
                .scram
                .remove(&(user.to_owned(), format!("{mechanism:?}")));
        }

        self.wal_append(WalRecord::ScramDelete {
            user: user.to_owned(),
            mechanism: format!("{mechanism:?}"),
        })
        .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let coord = self.coord.lock().await;
        Ok(coord
            .scram
            .get(&(user.to_owned(), format!("{mechanism:?}")))
            .cloned())
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let error_code = ErrorCode::None;

        let base = DescribeConfigsResult::default()
            .error_code(error_code.into())
            .error_message(Some(error_code.to_string()))
            .resource_type(i8::from(resource))
            .resource_name(name.into());

        if let ConfigResource::Topic = resource {
            let coord = self.coord.lock().await;

            if let Some(metadata) = coord.topic_metadata(&TopicId::Name(name.into())) {
                return Ok(base.configs(
                    metadata
                        .topic
                        .configs
                        .as_ref()
                        .map(|configs| {
                            configs
                                .iter()
                                .map(|config| {
                                    DescribeConfigsResourceResult::default()
                                        .name(config.name.clone())
                                        .value(config.value.clone())
                                        .read_only(false)
                                        .is_default(None)
                                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                                        .is_sensitive(false)
                                        .synonyms(Some([].into()))
                                        .config_type(Some(ConfigType::String.into()))
                                        .documentation(Some("".into()))
                                })
                                .collect()
                        })
                        .or(Some(vec![])),
                ));
            }
        }

        Ok(base.configs(Some(vec![])))
    }

    async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let mut group_ids: std::collections::BTreeSet<String> =
            self.groups.read()?.keys().cloned().collect();

        group_ids.extend(
            self.group_offsets
                .read()?
                .keys()
                .map(|(group, _)| group.clone()),
        );

        Ok(group_ids
            .into_iter()
            .map(|group_id| {
                ListedGroup::default()
                    .group_id(group_id)
                    .protocol_type("consumer".into())
                    .group_state(Some("Unknown".into()))
                    .group_type(Some("classic".into()))
            })
            .collect())
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let mut deleted = vec![];

            {
                let mut groups = self.groups.write()?;
                let mut group_offsets = self.group_offsets.write()?;

                for group_id in group_ids {
                    let had_group_state = groups.remove(group_id).is_some();

                    let before = group_offsets.len();
                    group_offsets.retain(|(group, _), _| group != group_id);
                    let deleted_committed_offsets = before != group_offsets.len();

                    if had_group_state || deleted_committed_offsets {
                        deleted.push(group_id.clone());
                    }

                    results.push(
                        DeletableGroupResult::default()
                            .group_id(group_id.into())
                            .error_code(
                                if had_group_state || deleted_committed_offsets {
                                    ErrorCode::None
                                } else {
                                    ErrorCode::GroupIdNotFound
                                }
                                .into(),
                            ),
                    );
                }
            }

            for group in deleted {
                self.wal_append(WalRecord::GroupDelete { group }).await?;
            }
        }

        Ok(results)
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let groups = self.groups.read()?;

            for group_id in group_ids {
                results.push(NamedGroupDetail::found(
                    group_id.into(),
                    groups
                        .get(group_id)
                        .map(|(detail, _)| detail.clone())
                        .unwrap_or_default(),
                ));
            }
        }

        Ok(results)
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let _ = (partition_limit, cursor);

        let coord = self.coord.lock().await;

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            match coord.topic_metadata(topic) {
                Some(metadata) => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .name(Some(metadata.topic.name.clone()))
                        .topic_id(metadata.id.into_bytes())
                        .is_internal(false)
                        .partitions(Some(
                            (0..metadata.topic.num_partitions)
                                .map(|partition_index| {
                                    let replicas = Some(vec![
                                        self.node;
                                        metadata.topic.replication_factor.max(0)
                                            as usize
                                    ]);

                                    DescribeTopicPartitionsResponsePartition::default()
                                        .error_code(ErrorCode::None.into())
                                        .partition_index(partition_index)
                                        .leader_id(self.node)
                                        .leader_epoch(0)
                                        .replica_nodes(replicas.clone())
                                        .isr_nodes(replicas)
                                        .eligible_leader_replicas(Some(vec![]))
                                        .last_known_elr(Some(vec![]))
                                        .offline_replicas(Some(vec![]))
                                })
                                .collect(),
                        ))
                        .topic_authorized_operations(-2147483648),
                ),

                None => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                        .name(match topic {
                            TopicId::Name(name) => Some(name.into()),
                            TopicId::Id(_) => None,
                        })
                        .topic_id(match topic {
                            TopicId::Name(_) => NULL_TOPIC_ID,
                            TopicId::Id(id) => id.into_bytes(),
                        })
                        .is_internal(false)
                        .partitions(Some([].into()))
                        .topic_authorized_operations(-2147483648),
                ),
            }
        }

        Ok(responses)
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let updated = {
            let mut groups = self
                .groups
                .write()
                .map_err(|_| UpdateError::Error(Error::Poison))?;

            match (groups.get(group_id), version) {
                (None, None) => {
                    let version = Version::from(&Uuid::now_v7());
                    _ = groups.insert(group_id.to_owned(), (detail.clone(), version.clone()));
                    version
                }

                (None, Some(_)) => {
                    return Err(UpdateError::Error(Error::Message(format!(
                        "group not found: {group_id}"
                    ))));
                }

                (Some((current, stored)), None) => {
                    return Err(UpdateError::Outdated {
                        current: Box::new(current.clone()),
                        version: stored.clone(),
                    });
                }

                (Some((_, stored)), Some(version)) if stored == &version => {
                    let version = Version::from(&Uuid::now_v7());
                    _ = groups.insert(group_id.to_owned(), (detail.clone(), version.clone()));
                    version
                }

                (Some((current, stored)), Some(_)) => {
                    return Err(UpdateError::Outdated {
                        current: Box::new(current.clone()),
                        version: stored.clone(),
                    });
                }
            }
        };

        self.wal_append(WalRecord::GroupUpdate {
            group: group_id.to_owned(),
            detail,
            version: updated.clone(),
        })
        .await
        .map_err(UpdateError::Error)?;

        Ok(updated)
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        enum InitProducer {
            Completed(ProducerIdResponse),
            NeedToRollback {
                producer_id: i64,
                producer_epoch: i16,
            },
        }

        if let Some(transaction_id) = transaction_id {
            let (outcome, wal_record) = {
                let mut coord = self.coord.lock().await;

                match (producer_id, producer_epoch) {
                    (Some(-1), Some(-1)) => match coord.transactions.get(transaction_id) {
                        None => {
                            let id = coord.producers.last_key_value().map_or(1, |(k, _v)| k + 1);

                            let mut pd = ProducerDetail::default();
                            _ = pd.sequences.insert(0, BTreeMap::new());
                            _ = coord.producers.insert(id, pd);

                            let mut epochs = BTreeMap::new();
                            _ = epochs.insert(
                                0,
                                TxnDetail {
                                    transaction_timeout_ms,
                                    ..Default::default()
                                },
                            );

                            _ = coord.transactions.insert(
                                transaction_id.to_owned(),
                                Txn {
                                    producer: id,
                                    epochs,
                                },
                            );

                            (
                                InitProducer::Completed(ProducerIdResponse {
                                    id,
                                    epoch: 0,
                                    error: ErrorCode::None,
                                }),
                                Some(WalRecord::PidAlloc {
                                    producer_id: id,
                                    transaction_id: Some(transaction_id.to_owned()),
                                    transaction_timeout_ms,
                                }),
                            )
                        }

                        Some(txn) => {
                            let producer = txn.producer;
                            let last = txn
                                .epochs
                                .last_key_value()
                                .map(|(epoch, detail)| (*epoch, detail.state));

                            match last {
                                Some((current_epoch, Some(TxnState::Begin))) => (
                                    InitProducer::NeedToRollback {
                                        producer_id: producer,
                                        producer_epoch: current_epoch,
                                    },
                                    None,
                                ),

                                Some((current_epoch, _)) => {
                                    let epoch = current_epoch + 1;

                                    if let Some(pd) = coord.producers.get_mut(&producer) {
                                        _ = pd.sequences.insert(epoch, BTreeMap::new());
                                    }

                                    if let Some(txn) = coord.transactions.get_mut(transaction_id) {
                                        _ = txn.epochs.insert(
                                            epoch,
                                            TxnDetail {
                                                transaction_timeout_ms,
                                                ..Default::default()
                                            },
                                        );
                                    }

                                    (
                                        InitProducer::Completed(ProducerIdResponse {
                                            id: producer,
                                            epoch,
                                            error: ErrorCode::None,
                                        }),
                                        Some(WalRecord::EpochBump {
                                            transaction_id: transaction_id.to_owned(),
                                            producer_id: producer,
                                            producer_epoch: epoch,
                                            transaction_timeout_ms,
                                        }),
                                    )
                                }

                                None => (
                                    InitProducer::Completed(ProducerIdResponse {
                                        id: -1,
                                        epoch: -1,
                                        error: ErrorCode::UnknownServerError,
                                    }),
                                    None,
                                ),
                            }
                        }
                    },

                    (producer, epoch) => {
                        error!(?producer, ?epoch);
                        (
                            InitProducer::Completed(ProducerIdResponse {
                                id: -1,
                                epoch: -1,
                                error: ErrorCode::UnknownServerError,
                            }),
                            None,
                        )
                    }
                }
            };

            if let Some(record) = wal_record {
                self.wal_append(record).await?;
            }

            match outcome {
                InitProducer::Completed(completed) => Ok(completed),
                InitProducer::NeedToRollback {
                    producer_id: rollback_producer_id,
                    producer_epoch: rollback_producer_epoch,
                } => {
                    let error_code = self
                        .txn_end(
                            transaction_id,
                            rollback_producer_id,
                            rollback_producer_epoch,
                            false,
                        )
                        .await?;

                    debug!(rollback_producer_id, rollback_producer_epoch, ?error_code);

                    if error_code == ErrorCode::None {
                        self.init_producer(
                            Some(transaction_id),
                            transaction_timeout_ms,
                            producer_id,
                            producer_epoch,
                        )
                        .await
                    } else {
                        Ok(ProducerIdResponse {
                            id: -1,
                            epoch: -1,
                            error: ErrorCode::UnknownServerError,
                        })
                    }
                }
            }
        } else {
            let response = {
                let mut coord = self.coord.lock().await;

                match (producer_id, producer_epoch) {
                    (Some(-1), Some(-1)) => {
                        let producer = coord.producers.last_key_value().map_or(1, |(k, _v)| k + 1);

                        let epoch = 0;
                        let mut pd = ProducerDetail::default();
                        _ = pd.sequences.insert(epoch, BTreeMap::new());
                        _ = coord.producers.insert(producer, pd);

                        ProducerIdResponse {
                            id: producer,
                            epoch,
                            ..Default::default()
                        }
                    }

                    (producer, epoch) => {
                        error!(?producer, ?epoch);
                        ProducerIdResponse {
                            id: -1,
                            epoch: -1,
                            error: ErrorCode::UnknownServerError,
                        }
                    }
                }
            };

            if response.error == ErrorCode::None {
                self.wal_append(WalRecord::PidAlloc {
                    producer_id: response.id,
                    transaction_id: None,
                    transaction_timeout_ms: 0,
                })
                .await?;
            }

            Ok(response)
        }
    }

    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                ref topics,
            } => {
                let started_at = SystemTime::now();

                let (results, partitions) = {
                    let mut coord = self.coord.lock().await;

                    let txn_detail =
                        match coord.txn_detail_mut(&transaction_id, producer_id, producer_epoch) {
                            Ok(txn_detail) => txn_detail,
                            Err(Error::Api(error_code)) => {
                                return Ok(Self::txn_add_partitions_response(topics, error_code));
                            }
                            Err(otherwise) => return Err(otherwise),
                        };

                    // A terminal txn at this epoch being re-begun (classic
                    // clients reuse the epoch across transactions) must not
                    // leak its previous incarnation's payload into the new one.
                    if txn_detail.state.is_some_and(|state| {
                        matches!(state, TxnState::Committed | TxnState::Aborted)
                    }) {
                        txn_detail.produces.clear();
                        txn_detail.offsets.clear();
                    }

                    let mut results = vec![];
                    let mut partitions = vec![];

                    for topic in topics {
                        let mut results_by_partition = vec![];

                        for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                            _ = txn_detail
                                .produces
                                .entry(topic.name.clone())
                                .or_default()
                                .entry(*partition_index)
                                .or_default();

                            partitions.push((topic.name.clone(), *partition_index));

                            results_by_partition.push(
                                AddPartitionsToTxnPartitionResult::default()
                                    .partition_index(*partition_index)
                                    .partition_error_code(i16::from(ErrorCode::None)),
                            );
                        }

                        results.push(
                            AddPartitionsToTxnTopicResult::default()
                                .name(topic.name.clone())
                                .results_by_partition(Some(results_by_partition)),
                        )
                    }

                    txn_detail.started_at = Some(started_at);
                    txn_detail.state = Some(TxnState::Begin);

                    (results, partitions)
                };

                self.wal_append(WalRecord::TxnBegin {
                    transaction_id,
                    producer_id,
                    producer_epoch,
                    started_at,
                    partitions,
                })
                .await?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
            }

            TxnAddPartitionsRequest::VersionFourPlus { .. } => {
                Err(Error::Api(ErrorCode::UnsupportedVersion))
            }
        }
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let (responses, stored) = {
            let mut coord = self.coord.lock().await;

            let txn_detail = match coord.txn_detail_mut(
                &offsets.transaction_id,
                offsets.producer_id,
                offsets.producer_epoch,
            ) {
                Ok(txn_detail) => txn_detail,
                Err(Error::Api(error_code)) => {
                    return Ok(Self::txn_offset_commit_response_error(&offsets, error_code));
                }
                Err(otherwise) => return Err(otherwise),
            };

            let mut responses = vec![];
            let mut stored = vec![];

            for topic in &offsets.topics {
                let mut partition_responses = vec![];

                if let Some(partitions) = topic.partitions.as_deref() {
                    for partition in partitions {
                        let co = state::TxnCommitOffset {
                            committed_offset: partition.committed_offset,
                            leader_epoch: partition.committed_leader_epoch,
                            metadata: partition.committed_metadata.clone(),
                        };

                        _ = txn_detail
                            .offsets
                            .entry(offsets.group_id.clone())
                            .or_default()
                            .entry(topic.name.clone())
                            .or_default()
                            .insert(partition.partition_index, co.clone());

                        stored.push((topic.name.clone(), partition.partition_index, co));

                        partition_responses.push(
                            TxnOffsetCommitResponsePartition::default()
                                .partition_index(partition.partition_index)
                                .error_code(ErrorCode::None.into()),
                        );
                    }
                }

                responses.push(
                    TxnOffsetCommitResponseTopic::default()
                        .name(topic.name.to_string())
                        .partitions(Some(partition_responses)),
                );
            }

            (responses, stored)
        };

        self.wal_append(WalRecord::TxnOffsets {
            transaction_id: offsets.transaction_id.clone(),
            producer_id: offsets.producer_id,
            producer_epoch: offsets.producer_epoch,
            group: offsets.group_id.clone(),
            offsets: stored,
        })
        .await?;

        Ok(responses)
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        // Phase 1: validate and move Begin to PrepareCommit/PrepareAbort,
        // collecting the partitions this txn produced to. The prepared
        // ranges are WAL'd before any marker is written, so a crash
        // mid-phase-2 recovers as an in-doubt txn with known ranges.
        let (transitioned, produced, prepared_ranges): (
            bool,
            Vec<Topition>,
            Vec<(String, i32, state::TxnProduceOffset)>,
        ) = {
            let mut coord = self.coord.lock().await;

            let txn_detail = coord.txn_detail_mut(transaction_id, producer_id, producer_epoch)?;

            let mut produced = vec![];
            let mut prepared_ranges = vec![];
            let transitioned = txn_detail.state == Some(TxnState::Begin);

            if transitioned {
                txn_detail.state = Some(if committed {
                    TxnState::PrepareCommit
                } else {
                    TxnState::PrepareAbort
                });

                for (topic, partitions) in &txn_detail.produces {
                    for (partition, offset_range) in partitions {
                        if let Some(range) = offset_range {
                            produced.push(Topition::new(topic.to_owned(), *partition));
                            prepared_ranges.push((topic.clone(), *partition, *range));
                        }
                    }
                }
            }

            (transitioned, produced, prepared_ranges)
        };

        if transitioned {
            self.wal_append(WalRecord::TxnPrepare {
                transaction_id: transaction_id.to_owned(),
                producer_id,
                producer_epoch,
                committed,
                produced: prepared_ranges,
            })
            .await?;
        }

        // Phase 2: write the end-txn control marker to every produced
        // partition through the normal produce path (extends each range so
        // read_committed skips past the marker). No coordination lock held;
        // markers ride each partition's own append stream.
        for topition in &produced {
            _ = self
                .produce_marker(
                    transaction_id,
                    producer_id,
                    producer_epoch,
                    committed,
                    topition,
                )
                .await
                .inspect_err(|err| {
                    error!(?err, ?topition, producer_id, producer_epoch, committed)
                })?;
        }

        // Phase 3: resolution, dynostore semantics with the terminal-overlap
        // fix. A transaction only transitions once no overlapping open
        // (Begin) transaction remains on its partitions; a deferred txn stays
        // prepared — still pinning the last stable offset — and is resolved
        // by whichever overlapping txn_end completes last.
        struct TxnEffects {
            transaction: String,
            producer_id: i64,
            producer_epoch: i16,
            aborting: bool,
            ranges: Vec<(Topition, state::TxnProduceOffset)>,
            offsets: Vec<(String, Topition, state::TxnCommitOffset)>,
        }

        let effects: Vec<TxnEffects> = {
            let mut coord = self.coord.lock().await;

            // Re-validate: phase 2 released the lock.
            _ = coord.txn_detail_mut(transaction_id, producer_id, producer_epoch)?;

            let overlaps =
                coord.overlapping_transactions(transaction_id, producer_id, producer_epoch);

            if !overlaps.iter().all(|txn| txn.state.is_prepared()) {
                debug!(
                    transaction_id,
                    producer_id,
                    producer_epoch,
                    ?overlaps,
                    "deferred"
                );
                return Ok(ErrorCode::None);
            }

            let mut txn_refs = overlaps;
            txn_refs.push(state::TxnRef {
                transaction: transaction_id.to_owned(),
                producer_id,
                producer_epoch,
                state: if committed {
                    TxnState::PrepareCommit
                } else {
                    TxnState::PrepareAbort
                },
            });

            let mut effects = Vec::with_capacity(txn_refs.len());

            for txn_ref in txn_refs {
                let Some(txn) = coord.transactions.get_mut(&txn_ref.transaction) else {
                    continue;
                };

                let Some(txn_detail) = txn.epochs.get_mut(&txn_ref.producer_epoch) else {
                    continue;
                };

                let terminal = match txn_detail.state {
                    None | Some(TxnState::PrepareCommit) => TxnState::Committed,
                    Some(TxnState::PrepareAbort) => TxnState::Aborted,
                    otherwise => {
                        warn!(
                            transaction = txn_ref.transaction,
                            producer = txn_ref.producer_id,
                            epoch = txn_ref.producer_epoch,
                            ?otherwise,
                        );
                        continue;
                    }
                };

                let ranges: Vec<(Topition, state::TxnProduceOffset)> = txn_detail
                    .produces
                    .iter()
                    .flat_map(|(topic, partitions)| {
                        partitions.iter().filter_map(|(partition, range)| {
                            range.map(|range| (Topition::new(topic.to_owned(), *partition), range))
                        })
                    })
                    .collect();

                // Offset application follows the requested direction (a
                // never-begun offsets-only txn transitions Committed either
                // way, but only a commit request applies its offsets).
                let offsets: Vec<(String, Topition, state::TxnCommitOffset)> =
                    if txn_ref.state == TxnState::PrepareCommit {
                        txn_detail
                            .offsets
                            .iter()
                            .flat_map(|(group, topics)| {
                                topics.iter().flat_map(|(topic, partitions)| {
                                    partitions.iter().map(|(partition, co)| {
                                        (
                                            group.clone(),
                                            Topition::new(topic.to_owned(), *partition),
                                            co.clone(),
                                        )
                                    })
                                })
                            })
                            .collect()
                    } else {
                        vec![]
                    };

                txn_detail.state = Some(terminal);
                txn_detail.produces.clear();
                txn_detail.offsets.clear();
                _ = txn_detail.started_at.take();

                effects.push(TxnEffects {
                    transaction: txn_ref.transaction,
                    producer_id: txn_ref.producer_id,
                    producer_epoch: txn_ref.producer_epoch,
                    aborting: terminal == TxnState::Aborted,
                    ranges,
                    offsets,
                });
            }

            effects
        };

        // Terminal transitions are durable before their effects apply: one
        // record per resolved txn, one group-committed fsync for the lot.
        let mut last_ack = None;

        for effect in &effects {
            let (ack, bytes) = {
                let wal = self.wal.read()?;
                wal.append(&WalRecord::TxnTerminal {
                    transaction_id: effect.transaction.clone(),
                    producer_id: effect.producer_id,
                    producer_epoch: effect.producer_epoch,
                    committed: !effect.aborting,
                    aborted: if effect.aborting {
                        effect
                            .ranges
                            .iter()
                            .map(|(topition, range)| {
                                (topition.topic().to_owned(), topition.partition(), *range)
                            })
                            .collect()
                    } else {
                        vec![]
                    },
                })?
            };

            _ = self
                .disk_usage
                .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);

            last_ack = Some(ack);
        }

        if let Some(ack) = last_ack {
            ack.await
                .map_err(|_| Error::Message("nvme wal flusher dropped".into()))??;
        }

        // Per-partition: release each resolved txn's LSO pin; on abort,
        // publish its ranges into the partition's aborted index. Waking
        // read_committed fetch waiters happens once per touched partition.
        for effect in &effects {
            let open_key = (
                effect.transaction.clone(),
                effect.producer_id,
                effect.producer_epoch,
            );

            for (topition, range) in &effect.ranges {
                if let Some(partition) = self.partition_if_exists(topition)? {
                    {
                        let mut inner = partition.inner.lock()?;

                        _ = inner.open_txns.remove(&open_key);

                        if effect.aborting {
                            let aborted = AbortedRange {
                                producer: effect.producer_id,
                                offset_start: range.offset_start,
                                offset_end: range.offset_end,
                            };

                            // A snapshot capturing this push can coexist with
                            // its TxnTerminal in the next WAL: replay dedupes.
                            if !inner.aborted.contains(&aborted) {
                                inner.aborted.push(aborted);
                            }
                        }
                    }

                    partition.notify.notify_waiters();
                }
            }
        }

        // Committed transactional offsets become regular group commits.
        let mut by_group: BTreeMap<String, Vec<(Topition, OffsetCommitRequest)>> = BTreeMap::new();

        for effect in effects {
            for (group, topition, co) in effect.offsets {
                by_group.entry(group).or_default().push((
                    topition,
                    OffsetCommitRequest::default().offset(co.committed_offset),
                ));
            }
        }

        for (group, offsets) in by_group {
            _ = self.offset_commit(&group, None, &offsets[..]).await?;
        }

        Ok(ErrorCode::None)
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.apply_retention(now).await?;

        // Snapshot when the WAL has grown, retiring the replay chain.
        let bytes = {
            let wal = self.wal.read()?;
            wal.bytes_since_open()?
        };

        if bytes > 0 {
            self.snapshot_now().await?;
        }

        Ok(())
    }

    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        let expired: Vec<(String, i64, i16)> = {
            let coord = self.coord.lock().await;

            let mut expired = Vec::new();

            for (transaction_id, txn) in &coord.transactions {
                for (epoch, detail) in &txn.epochs {
                    if matches!(
                        detail.state,
                        Some(TxnState::Committed) | Some(TxnState::Aborted)
                    ) {
                        continue;
                    }

                    if let Some(started_at) = detail.started_at {
                        let timeout =
                            Duration::from_millis(detail.transaction_timeout_ms.max(0) as u64);

                        if now
                            .duration_since(started_at)
                            .map(|elapsed| elapsed > timeout)
                            .unwrap_or(false)
                        {
                            expired.push((transaction_id.clone(), txn.producer, *epoch));
                        }
                    }
                }
            }

            expired
        };

        // Abort each timed-out transaction through the normal end path.
        for (transaction_id, producer_id, producer_epoch) in expired {
            if let Err(err) = self
                .txn_end(&transaction_id, producer_id, producer_epoch, false)
                .await
            {
                error!(
                    ?err,
                    transaction_id, producer_id, producer_epoch, "sweep abort failed"
                );
            }
        }

        Ok(())
    }

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    async fn ping(&self) -> Result<()> {
        if self.data_dir.is_dir() {
            Ok(())
        } else {
            Err(Error::Message(format!(
                "nvme data dir is not a directory: {}",
                self.data_dir.display()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use tansu_sans_io::record::Record;

    use super::*;

    async fn open(dir: &std::path::Path) -> Result<Engine> {
        Engine::open(
            "test-cluster",
            111,
            dir,
            Url::parse("tcp://127.0.0.1:9092").expect("url"),
            None,
            Config::default(),
        )
        .await
    }

    fn batch(
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        transactional: bool,
        value: &str,
    ) -> Result<deflated::Batch> {
        let mut attributes = BatchAttribute::default();
        if transactional {
            attributes = attributes.transaction(true);
        }

        inflated::Batch::builder()
            .record(Record::builder().value(Bytes::copy_from_slice(value.as_bytes()).into()))
            .attributes(attributes.into())
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_sequence(base_sequence)
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
    }

    fn topic(name: &str, partitions: i32) -> CreatableTopic {
        CreatableTopic::default()
            .name(name.into())
            .num_partitions(partitions)
            .replication_factor(0)
            .assignments(Some([].into()))
            .configs(Some([].into()))
    }

    #[tokio::test]
    async fn recovery_round_trip() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let topition = Topition::new("recovered", 0);

        {
            let engine = open(dir.path()).await?;

            _ = engine.create_topic(topic("recovered", 1), false).await?;

            // Plain idempotent producer.
            let pid = engine.init_producer(None, 0, Some(-1), Some(-1)).await?;
            for sequence in 0..3 {
                _ = engine
                    .produce(
                        None,
                        &topition,
                        batch(pid.id, pid.epoch, sequence, false, "plain")?,
                    )
                    .await?;
            }

            // A committed transaction, then an aborted one at the same epoch.
            let txn = engine
                .init_producer(Some("txn-a"), 10_000, Some(-1), Some(-1))
                .await?;

            for (committed, base_sequence) in [(true, 0), (false, 1)] {
                _ = engine
                    .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                        transaction_id: "txn-a".into(),
                        producer_id: txn.id,
                        producer_epoch: txn.epoch,
                        topics: [
                            tansu_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic::default()
                                .name("recovered".into())
                                .partitions(Some([0].into())),
                        ]
                        .into(),
                    })
                    .await?;

                _ = engine
                    .produce(
                        Some("txn-a"),
                        &topition,
                        batch(txn.id, txn.epoch, base_sequence, true, "txn")?,
                    )
                    .await?;

                assert_eq!(
                    ErrorCode::None,
                    engine
                        .txn_end("txn-a", txn.id, txn.epoch, committed)
                        .await?
                );
            }

            // Consumer offsets survive too.
            _ = engine
                .offset_commit(
                    "group-a",
                    None,
                    &[(topition.clone(), OffsetCommitRequest::default().offset(2))],
                )
                .await?;

            let stage = engine.offset_stage(&topition).await?;
            assert_eq!(7, stage.high_watermark()); // 3 plain + (1+marker) x 2
            assert_eq!(stage.high_watermark(), stage.last_stable());
        }

        // Reopen: snapshot + WAL + segment scan must reproduce everything.
        let engine = open(dir.path()).await?;

        let stage = engine.offset_stage(&topition).await?;
        assert_eq!(7, stage.high_watermark());
        assert_eq!(stage.high_watermark(), stage.last_stable());
        assert_eq!(0, stage.log_start());

        let batches = engine
            .fetch(
                &topition,
                0,
                1,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(100),
            )
            .await?;
        assert_eq!(7, batches.len());
        assert_eq!(
            (0..7).collect::<Vec<i64>>(),
            batches
                .iter()
                .map(|batch| batch.base_offset)
                .collect::<Vec<_>>()
        );

        let aborted = engine.aborted_transactions(&topition, 0).await?;
        assert_eq!(1, aborted.len());
        assert_eq!(5, aborted[0].first_offset); // 3 plain + txn(3) + marker(4)

        let offsets = engine
            .offset_fetch(
                Some("group-a"),
                std::slice::from_ref(&topition),
                Some(false),
            )
            .await?;
        assert_eq!(Some(&2), offsets.get(&topition));

        // Sequences recovered: replaying an old sequence is a duplicate.
        let duplicate = engine
            .produce(None, &topition, batch(1, 0, 0, false, "dup")?)
            .await;
        assert!(matches!(
            duplicate,
            Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
        ));

        // And the log continues where it left off.
        let offset = engine
            .produce(None, &topition, batch(1, 0, 3, false, "after")?)
            .await?;
        assert_eq!(7, offset);

        Ok(())
    }

    #[tokio::test]
    async fn torn_tails_are_truncated() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let topition = Topition::new("torn", 0);

        {
            let engine = open(dir.path()).await?;
            _ = engine.create_topic(topic("torn", 1), false).await?;

            let pid = engine.init_producer(None, 0, Some(-1), Some(-1)).await?;
            for sequence in 0..3 {
                _ = engine
                    .produce(
                        None,
                        &topition,
                        batch(pid.id, pid.epoch, sequence, false, "keep")?,
                    )
                    .await?;
            }
        }

        // Tear the segment and WAL tails as a crash mid-append would.
        let partition_dir = dir.path().join("topics").join("torn-0000000000");
        for base in log::segment_bases(&partition_dir)? {
            let path = log::segment_path(&partition_dir, base);
            let mut bytes = std::fs::read(&path).expect("segment");
            bytes.extend_from_slice(&[0x54, 0x01, 0x01, 0x00, 0xff, 0xff]);
            std::fs::write(&path, &bytes).expect("torn segment");
        }

        for seq in wal::wal_seqs(&dir.path().join("wal"))? {
            let path = dir.path().join("wal").join(format!("{seq:020}.wal"));
            let mut bytes = std::fs::read(&path).expect("wal");
            bytes.extend_from_slice(b"torn");
            std::fs::write(&path, &bytes).expect("torn wal");
        }

        let engine = open(dir.path()).await?;

        let stage = engine.offset_stage(&topition).await?;
        assert_eq!(3, stage.high_watermark());

        let batches = engine
            .fetch(
                &topition,
                0,
                1,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(100),
            )
            .await?;
        assert_eq!(3, batches.len());

        Ok(())
    }

    #[tokio::test]
    async fn retention_deletes_sealed_segments() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let topition = Topition::new("retained", 0);

        let engine = Engine::open(
            "test-cluster",
            111,
            dir.path(),
            Url::parse("tcp://127.0.0.1:9092").expect("url"),
            None,
            Config {
                // Tiny segments: every batch seals the previous segment.
                segment_bytes: 64,
                ..Config::default()
            },
        )
        .await?;

        _ = engine
            .create_topic(
                topic("retained", 1).configs(Some(
                    [
                        tansu_sans_io::create_topics_request::CreatableTopicConfig::default()
                            .name("retention.ms".into())
                            .value(Some("1000".into())),
                    ]
                    .into(),
                )),
                false,
            )
            .await?;

        let pid = engine.init_producer(None, 0, Some(-1), Some(-1)).await?;
        for sequence in 0..5 {
            _ = engine
                .produce(
                    None,
                    &topition,
                    batch(pid.id, pid.epoch, sequence, false, "expiring")?,
                )
                .await?;
        }

        let partition_dir = dir.path().join("topics").join("retained-0000000000");
        assert!(log::segment_bases(&partition_dir)?.len() >= 4);

        // Everything sealed is far past retention; the active tail stays.
        engine
            .maintain(SystemTime::now() + StdDuration::from_secs(3600))
            .await?;

        let stage = engine.offset_stage(&topition).await?;
        assert_eq!(5, stage.high_watermark());
        assert_eq!(4, stage.log_start());
        assert_eq!(1, log::segment_bases(&partition_dir)?.len());

        let batches = engine
            .fetch(
                &topition,
                0,
                1,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(100),
            )
            .await?;
        assert_eq!(1, batches.len());
        assert_eq!(4, batches[0].base_offset);

        // Recovery with a log start beyond deleted segments.
        drop(engine);
        let engine = open(dir.path()).await?;

        let stage = engine.offset_stage(&topition).await?;
        assert_eq!(5, stage.high_watermark());
        assert_eq!(4, stage.log_start());

        Ok(())
    }

    #[tokio::test]
    async fn concurrent_producers_get_dense_unique_offsets() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let topition = Topition::new("dense", 0);

        let engine = std::sync::Arc::new(open(dir.path()).await?);
        _ = engine.create_topic(topic("dense", 1), false).await?;

        const TASKS: usize = 8;
        const EACH: usize = 25;

        let mut handles = vec![];

        for task in 0..TASKS {
            let engine = engine.clone();
            let topition = topition.clone();

            handles.push(tokio::spawn(async move {
                let mut offsets = vec![];

                for i in 0..EACH {
                    let batch = batch(-1, -1, -1, false, &format!("t{task}-{i}")).expect("batch");
                    offsets.push(
                        engine
                            .produce(None, &topition, batch)
                            .await
                            .expect("produce"),
                    );
                }

                offsets
            }));
        }

        let mut all = vec![];
        for handle in handles {
            all.extend(handle.await.expect("join"));
        }

        all.sort_unstable();
        let expected: Vec<i64> = (0..(TASKS * EACH) as i64).collect();
        assert_eq!(expected, all, "offsets must be dense and unique");

        let stage = engine.offset_stage(&topition).await?;
        assert_eq!((TASKS * EACH) as i64, stage.high_watermark());
        assert_eq!(stage.high_watermark(), stage.last_stable());

        Ok(())
    }

    /// Requires MinIO with a `tansu-nvme-test` bucket and AWS_* env
    /// pointing at it (the Thyme compose stack provides both):
    ///
    /// AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
    /// AWS_ENDPOINT=http://localhost:9000 AWS_ALLOW_HTTP=true \
    /// AWS_DEFAULT_REGION=us-east-1 \
    /// cargo test -p tansu-storage --lib nvme::tests::tiering -- --ignored
    #[tokio::test]
    #[ignore = "needs MinIO (AWS_* env + tansu-nvme-test bucket)"]
    async fn tiering_cold_bootstrap_serves_from_object_store() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let topition = Topition::new("tiered", 0);
        let unique = Uuid::now_v7();

        let config = Config {
            segment_bytes: 64, // every batch seals a segment
            tier: Some(format!("s3://tansu-nvme-test/{unique}")),
            tier_interval: Duration::from_millis(200),
            ..Config::default()
        };

        {
            let engine = Engine::open(
                "test-cluster",
                111,
                dir.path(),
                Url::parse("tcp://127.0.0.1:9092").expect("url"),
                None,
                config.clone(),
            )
            .await?;

            _ = engine.create_topic(topic("tiered", 1), false).await?;

            let pid = engine.init_producer(None, 0, Some(-1), Some(-1)).await?;
            for sequence in 0..5 {
                _ = engine
                    .produce(
                        None,
                        &topition,
                        batch(pid.id, pid.epoch, sequence, false, &format!("v{sequence}"))?,
                    )
                    .await?;
            }

            // Let the uploader tier the sealed segments, then mirror the
            // snapshot (which finish_boot wrote before the topic existed).
            tokio::time::sleep(Duration::from_secs(2)).await;
            engine.snapshot_now().await?;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Simulated node death: the local NVMe is gone entirely.
        std::fs::remove_dir_all(dir.path()).expect("remove data dir");
        std::fs::create_dir_all(dir.path()).expect("recreate data dir");

        let engine = Engine::open(
            "test-cluster",
            111,
            dir.path(),
            Url::parse("tcp://127.0.0.1:9092").expect("url"),
            None,
            config,
        )
        .await?;

        let stage = engine.offset_stage(&topition).await?;

        // The active (unsealed) tail at death was never tiered: the durable
        // contract on node loss is "everything up to the last sealed
        // segment". With 64-byte segments only the newest batch can be
        // un-sealed and un-uploaded.
        assert!(
            stage.high_watermark() >= 4,
            "at least the sealed segments recover, got {}",
            stage.high_watermark()
        );

        let batches = engine
            .fetch(
                &topition,
                0,
                1,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(100),
            )
            .await?;

        assert_eq!(stage.high_watermark() as usize, batches.len());
        assert_eq!(0, batches[0].base_offset);

        // And the log continues past the recovered watermark.
        let pid = engine.init_producer(None, 0, Some(-1), Some(-1)).await?;
        let offset = engine
            .produce(
                None,
                &topition,
                batch(pid.id, pid.epoch, 0, false, "after")?,
            )
            .await?;
        assert_eq!(stage.high_watermark(), offset);

        Ok(())
    }

    /// See tiering_cold_bootstrap_serves_from_object_store for the env.
    #[tokio::test]
    #[ignore = "needs MinIO (AWS_* env + tansu-nvme-test bucket)"]
    async fn tiering_evicts_under_disk_budget_and_reads_through() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let topition = Topition::new("evicted", 0);
        let unique = Uuid::now_v7();

        let engine = Engine::open(
            "test-cluster",
            111,
            dir.path(),
            Url::parse("tcp://127.0.0.1:9092").expect("url"),
            None,
            Config {
                segment_bytes: 64,
                tail_cache_bytes: 0,
                disk_budget_bytes: 4096,
                tier: Some(format!("s3://tansu-nvme-test/{unique}")),
                tier_interval: Duration::from_millis(100),
                ..Config::default()
            },
        )
        .await?;

        _ = engine.create_topic(topic("evicted", 1), false).await?;

        let pid = engine.init_producer(None, 0, Some(-1), Some(-1)).await?;
        for sequence in 0..30 {
            _ = engine
                .produce(
                    None,
                    &topition,
                    batch(pid.id, pid.epoch, sequence, false, "spill me to s3")?,
                )
                .await?;

            // Pace produces across uploader ticks so eviction keeps the
            // usage under budget as it climbs.
            if sequence % 3 == 2 {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        }

        // Uploader tiers the sealed segments then evicts under the budget.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let partition_dir = dir.path().join("topics").join("evicted-0000000000");
        let (tiered, local_files) = {
            let partitions = engine.partitions.read()?;
            let state = partitions.get(&topition).expect("partition");
            let inner = state.inner.lock()?;
            (
                inner.batches.values().filter(|entry| entry.tiered).count(),
                log::segment_bases(&partition_dir)?.len(),
            )
        };

        assert!(tiered > 0, "eviction must have tiered some batches");
        assert!(local_files < 30, "some local segments must be gone");

        // Every batch still readable: hot from disk, cold via ranged GET.
        let batches = engine
            .fetch(
                &topition,
                0,
                1,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(100),
            )
            .await?;
        assert_eq!(30, batches.len());

        Ok(())
    }

    #[tokio::test]
    async fn disk_budget_refuses_then_reclaims() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let topition = Topition::new("budget", 0);

        let engine = Engine::open(
            "test-cluster",
            111,
            dir.path(),
            Url::parse("tcp://127.0.0.1:9092").expect("url"),
            None,
            Config {
                segment_bytes: 128,
                disk_budget_bytes: 2048,
                ..Config::default()
            },
        )
        .await?;

        _ = engine
            .create_topic(
                topic("budget", 1).configs(Some(
                    [
                        tansu_sans_io::create_topics_request::CreatableTopicConfig::default()
                            .name("retention.ms".into())
                            .value(Some("1000".into())),
                    ]
                    .into(),
                )),
                false,
            )
            .await?;

        let pid = engine.init_producer(None, 0, Some(-1), Some(-1)).await?;

        let mut refused = None;
        for sequence in 0..200 {
            match engine
                .produce(
                    None,
                    &topition,
                    batch(pid.id, pid.epoch, sequence, false, "filling the disk")?,
                )
                .await
            {
                Ok(_) => {}
                Err(Error::Api(ErrorCode::KafkaStorageError)) => {
                    refused = Some(sequence);
                    break;
                }
                Err(otherwise) => return Err(otherwise),
            }
        }

        let refused = refused.expect("budget must refuse eventually");
        assert!(refused > 4, "some produces must land first");

        // Retention reclaims budget; produce works again.
        engine
            .maintain(SystemTime::now() + StdDuration::from_secs(3600))
            .await?;

        _ = engine
            .produce(
                None,
                &topition,
                batch(pid.id, pid.epoch, refused, false, "after reclaim")?,
            )
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn fetch_reads_evicted_batches_from_disk() -> Result<()> {
        let dir = tempfile::tempdir().expect("tempdir");
        let topition = Topition::new("cold", 0);

        let engine = Engine::open(
            "test-cluster",
            111,
            dir.path(),
            Url::parse("tcp://127.0.0.1:9092").expect("url"),
            None,
            Config {
                // No tail cache: every fetch preads the segment, including
                // the active one (the librdkafka 0041 regression).
                tail_cache_bytes: 0,
                ..Config::default()
            },
        )
        .await?;

        _ = engine.create_topic(topic("cold", 1), false).await?;

        let pid = engine.init_producer(None, 0, Some(-1), Some(-1)).await?;
        for sequence in 0..4 {
            _ = engine
                .produce(
                    None,
                    &topition,
                    batch(pid.id, pid.epoch, sequence, false, "cold read")?,
                )
                .await?;
        }

        let batches = engine
            .fetch(
                &topition,
                0,
                1,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(100),
            )
            .await?;

        assert_eq!(4, batches.len());
        assert_eq!(0, batches[0].base_offset);

        Ok(())
    }

    #[test]
    fn config_defaults_from_bare_url() -> Result<()> {
        let url = Url::parse("nvme:///var/lib/tansu")?;
        assert_eq!(Config::default(), Config::from_url(&url)?);
        Ok(())
    }

    #[test]
    fn config_parses_knobs_and_ignores_unknown_keys() -> Result<()> {
        let url = Url::parse(
            "nvme:///data?fsync=interval:5ms&fetch_max_block=100ms\
             &segment_bytes=64M&snapshot_wal_bytes=1M&tail_cache_bytes=2M\
             &maintenance_interval=5m",
        )?;

        let config = Config::from_url(&url)?;

        assert_eq!(FsyncMode::Interval(Duration::from_millis(5)), config.fsync);
        assert_eq!(Duration::from_millis(100), config.fetch_max_block);
        assert_eq!(64 * 1024 * 1024, config.segment_bytes);
        assert_eq!(1024 * 1024, config.snapshot_wal_bytes);
        assert_eq!(2 * 1024 * 1024, config.tail_cache_bytes);

        Ok(())
    }

    #[test]
    fn config_rejects_bad_fsync() {
        let url = Url::parse("nvme:///data?fsync=sometimes").expect("url");
        assert!(Config::from_url(&url).is_err());
    }
}
