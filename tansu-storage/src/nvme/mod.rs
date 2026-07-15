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

mod partition;
mod state;

use partition::{AbortedRange, BatchEntry, PartitionState};
use state::{CoordState, ProducerDetail, Topic, Txn, TxnDetail};

/// When produce/commit acks: after group-commit fdatasync (`Always`, the
/// default and the only EOS-safe mode), or after the buffered write with a
/// background fsync every interval (`Interval`, for experiments only).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FsyncMode {
    Always,
    Interval(Duration),
}

/// Engine knobs, parsed from the `nvme://` URL query string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Config {
    pub fsync: FsyncMode,
    pub fetch_max_block: Duration,
    pub segment_bytes: u64,
    pub snapshot_wal_bytes: u64,
    pub tail_cache_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            fsync: FsyncMode::Always,
            fetch_max_block: Duration::from_millis(250),
            segment_bytes: 256 * 1024 * 1024,
            snapshot_wal_bytes: 64 * 1024 * 1024,
            tail_cache_bytes: 8 * 1024 * 1024,
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
    // Segment/WAL root from M2; ping checks it exists meanwhile.
    data_dir: PathBuf,
    schemas: Option<Registry>,
    config: Config,

    /// Producers, transactions, topics, SCRAM: small, fine-grained state
    /// mutated in sub-microsecond critical sections. Never held across I/O.
    coord: tokio::sync::Mutex<CoordState>,
    /// Per-partition log state; the map itself is read-mostly (written only
    /// by topic create/delete), each partition guards itself.
    partitions: RwLock<HashMap<Topition, std::sync::Arc<PartitionState>>>,
    /// Consumer-group state with optimistic-concurrency versions
    /// (the broker-side group coordinator drives this via update_group).
    groups: RwLock<HashMap<String, (GroupDetail, Version)>>,
    /// Committed consumer offsets, keyed by (group, topic partition).
    group_offsets: RwLock<BTreeMap<(String, Topition), OffsetCommitRequest>>,
}

impl Engine {
    pub fn new(cluster: impl Into<String>, node: i32, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            cluster: cluster.into(),
            node,
            advertised_listener: Url::parse("tcp://127.0.0.1:9092")
                .expect("default advertised listener"),
            data_dir: data_dir.into(),
            schemas: None,
            config: Config::default(),
            coord: tokio::sync::Mutex::new(CoordState::default()),
            partitions: RwLock::new(HashMap::new()),
            groups: RwLock::new(HashMap::new()),
            group_offsets: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn advertised_listener(self, advertised_listener: Url) -> Self {
        Self {
            advertised_listener,
            ..self
        }
    }

    pub fn schemas(self, schemas: Option<Registry>) -> Self {
        Self { schemas, ..self }
    }

    pub fn config(self, config: Config) -> Self {
        Self { config, ..self }
    }

    /// The partition's shared state, created on demand: like dynostore,
    /// produce to a topic without metadata still allocates log state.
    fn partition(&self, topition: &Topition) -> Result<std::sync::Arc<PartitionState>> {
        if let Some(partition) = self.partitions.read()?.get(topition) {
            return Ok(partition.clone());
        }

        let mut partitions = self.partitions.write()?;
        Ok(partitions
            .entry(topition.clone())
            .or_insert_with(|| std::sync::Arc::new(PartitionState::default()))
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
                    let replica_nodes =
                        Some(vec![self.node; metadata.topic.replication_factor.max(0) as usize]);

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
    async fn register_broker(
        &self,
        _broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
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

        {
            let mut partitions = self.partitions.write()?;
            for partition in 0..topic.num_partitions {
                _ = partitions
                    .entry(Topition::new(topic.name.as_str(), partition))
                    .or_insert_with(|| std::sync::Arc::new(PartitionState::default()));
            }
        }

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
            self.coord.lock().await.alter_topic(
                resource.resource_name.as_str(),
                resource.configs.as_deref().unwrap_or_default(),
            )?;
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
                    let mut inner = state.inner.lock()?;

                    // -1 means truncate to the high watermark.
                    let target = if partition.offset < 0 {
                        inner.high_watermark()
                    } else {
                        partition.offset
                    };

                    if target > inner.high_watermark() {
                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(-1)
                            .error_code(ErrorCode::OffsetOutOfRange.into())
                    } else {
                        let low_watermark = inner.advance_log_start(target);

                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(low_watermark)
                            .error_code(ErrorCode::None.into())
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

        self.partitions
            .write()?
            .retain(|topition, _| topition.topic() != name);

        self.group_offsets
            .write()?
            .retain(|(_, topition), _| topition.topic() != name);

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
        let last_offset_delta = deflated.last_offset_delta;
        let max_timestamp = deflated.max_timestamp;

        let (offset, offset_end) = {
            let mut inner = partition.inner.lock()?;

            let offset = inner.next_offset;
            let offset_end = offset + i64::from(last_offset_delta);
            inner.next_offset = offset_end + 1;

            _ = inner.batches.insert(
                offset,
                BatchEntry {
                    last_offset_delta,
                    max_timestamp,
                    is_control: attributes.control,
                    encoded: Bytes::from(deflated),
                },
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

            (offset, offset_end)
        };

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

        let deadline = tokio::time::Instant::now()
            + max_wait.min(self.config.fetch_max_block);

        loop {
            let notified = partition.notify.notified();
            tokio::pin!(notified);

            let encoded: Vec<(i64, Bytes)> = {
                let inner = partition.inner.lock()?;

                let limit = match isolation {
                    IsolationLevel::ReadCommitted => inner.last_stable(),
                    IsolationLevel::ReadUncommitted => inner.high_watermark(),
                };

                if offset < limit {
                    let mut wanted: Vec<(i64, Bytes)> = vec![];

                    // The fetch offset can fall inside a batch that starts
                    // before it; Kafka returns that batch whole, leaving the
                    // client to skip records below the fetch offset.
                    if let Some((base, entry)) = inner.batches.range(..offset).next_back()
                        && !inner.batches.contains_key(&offset)
                        && *base + i64::from(entry.last_offset_delta) >= offset
                        && *base >= inner.log_start
                    {
                        wanted.push((*base, entry.encoded.clone()));
                    }

                    let mut budget = u64::from(max_bytes);

                    for (base, entry) in inner.batches.range(offset..limit) {
                        wanted.push((*base, entry.encoded.clone()));

                        let size = entry.encoded.len() as u64;
                        if size > budget {
                            break;
                        }
                        budget = budget.saturating_sub(size);
                    }

                    wanted
                } else {
                    vec![]
                }
            };

            if !encoded.is_empty() {
                let mut batches = Vec::with_capacity(encoded.len());

                for (base_offset, bytes) in encoded {
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
        let mut group_offsets = self.group_offsets.write()?;

        for (topition, offset_commit) in offsets {
            if known.contains(topition.topic()) {
                _ = group_offsets.insert(
                    (group_id.to_owned(), topition.to_owned()),
                    offset_commit.to_owned(),
                );
                responses.push((topition.to_owned(), ErrorCode::None));
            } else {
                responses.push((topition.to_owned(), ErrorCode::UnknownTopicOrPartition));
            }
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
        let mut coord = self.coord.lock().await;
        _ = coord
            .scram
            .insert((user.to_owned(), format!("{mechanism:?}")), credential);
        Ok(())
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let mut coord = self.coord.lock().await;
        _ = coord.scram.remove(&(user.to_owned(), format!("{mechanism:?}")));
        Ok(())
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
            let mut groups = self.groups.write()?;
            let mut group_offsets = self.group_offsets.write()?;

            for group_id in group_ids {
                let had_group_state = groups.remove(group_id).is_some();

                let before = group_offsets.len();
                group_offsets.retain(|(group, _), _| group != group_id);
                let deleted_committed_offsets = before != group_offsets.len();

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
        let mut groups = self
            .groups
            .write()
            .map_err(|_| UpdateError::Error(Error::Poison))?;

        match (groups.get(group_id), version) {
            (None, None) => {
                let version = Version::from(&Uuid::now_v7());
                _ = groups.insert(group_id.to_owned(), (detail, version.clone()));
                Ok(version)
            }

            (None, Some(_)) => Err(UpdateError::Error(Error::Message(format!(
                "group not found: {group_id}"
            )))),

            (Some((current, stored)), None) => Err(UpdateError::Outdated {
                current: Box::new(current.clone()),
                version: stored.clone(),
            }),

            (Some((_, stored)), Some(version)) if stored == &version => {
                let version = Version::from(&Uuid::now_v7());
                _ = groups.insert(group_id.to_owned(), (detail, version.clone()));
                Ok(version)
            }

            (Some((current, stored)), Some(_)) => Err(UpdateError::Outdated {
                current: Box::new(current.clone()),
                version: stored.clone(),
            }),
        }
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
            NeedToRollback { producer_id: i64, producer_epoch: i16 },
        }

        if let Some(transaction_id) = transaction_id {
            let outcome = {
                let mut coord = self.coord.lock().await;

                match (producer_id, producer_epoch) {
                    (Some(-1), Some(-1)) => {
                        match coord.transactions.get(transaction_id) {
                            None => {
                                let id = coord
                                    .producers
                                    .last_key_value()
                                    .map_or(1, |(k, _v)| k + 1);

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

                                _ = coord
                                    .transactions
                                    .insert(transaction_id.to_owned(), Txn { producer: id, epochs });

                                InitProducer::Completed(ProducerIdResponse {
                                    id,
                                    epoch: 0,
                                    error: ErrorCode::None,
                                })
                            }

                            Some(txn) => {
                                let producer = txn.producer;
                                let last = txn
                                    .epochs
                                    .last_key_value()
                                    .map(|(epoch, detail)| (*epoch, detail.state));

                                match last {
                                    Some((current_epoch, state))
                                        if state == Some(TxnState::Begin) =>
                                    {
                                        InitProducer::NeedToRollback {
                                            producer_id: producer,
                                            producer_epoch: current_epoch,
                                        }
                                    }

                                    Some((current_epoch, _)) => {
                                        let epoch = current_epoch + 1;

                                        if let Some(pd) = coord.producers.get_mut(&producer) {
                                            _ = pd.sequences.insert(epoch, BTreeMap::new());
                                        }

                                        if let Some(txn) =
                                            coord.transactions.get_mut(transaction_id)
                                        {
                                            _ = txn.epochs.insert(
                                                epoch,
                                                TxnDetail {
                                                    transaction_timeout_ms,
                                                    ..Default::default()
                                                },
                                            );
                                        }

                                        InitProducer::Completed(ProducerIdResponse {
                                            id: producer,
                                            epoch,
                                            error: ErrorCode::None,
                                        })
                                    }

                                    None => InitProducer::Completed(ProducerIdResponse {
                                        id: -1,
                                        epoch: -1,
                                        error: ErrorCode::UnknownServerError,
                                    }),
                                }
                            }
                        }
                    }

                    (producer, epoch) => {
                        error!(?producer, ?epoch);
                        InitProducer::Completed(ProducerIdResponse {
                            id: -1,
                            epoch: -1,
                            error: ErrorCode::UnknownServerError,
                        })
                    }
                }
            };

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
            let mut coord = self.coord.lock().await;

            match (producer_id, producer_epoch) {
                (Some(-1), Some(-1)) => {
                    let producer = coord
                        .producers
                        .last_key_value()
                        .map_or(1, |(k, _v)| k + 1);

                    let epoch = 0;
                    let mut pd = ProducerDetail::default();
                    _ = pd.sequences.insert(epoch, BTreeMap::new());
                    _ = coord.producers.insert(producer, pd);

                    Ok(ProducerIdResponse {
                        id: producer,
                        epoch,
                        ..Default::default()
                    })
                }

                (producer, epoch) => {
                    error!(?producer, ?epoch);
                    Ok(ProducerIdResponse {
                        id: -1,
                        epoch: -1,
                        error: ErrorCode::UnknownServerError,
                    })
                }
            }
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
                let mut coord = self.coord.lock().await;

                let txn_detail =
                    match coord.txn_detail_mut(&transaction_id, producer_id, producer_epoch) {
                        Ok(txn_detail) => txn_detail,
                        Err(Error::Api(error_code)) => {
                            return Ok(Self::txn_add_partitions_response(topics, error_code));
                        }
                        Err(otherwise) => return Err(otherwise),
                    };

                // A terminal txn at this epoch being re-begun (classic clients
                // reuse the epoch across transactions) must not leak its
                // previous incarnation's payload into the new one.
                if txn_detail.state.is_some_and(|state| {
                    matches!(state, TxnState::Committed | TxnState::Aborted)
                }) {
                    txn_detail.produces.clear();
                    txn_detail.offsets.clear();
                }

                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];

                    for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                        _ = txn_detail
                            .produces
                            .entry(topic.name.clone())
                            .or_default()
                            .entry(*partition_index)
                            .or_default();

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

                txn_detail.started_at = Some(SystemTime::now());
                txn_detail.state = Some(TxnState::Begin);

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

        for topic in &offsets.topics {
            let mut partition_responses = vec![];

            if let Some(partitions) = topic.partitions.as_deref() {
                for partition in partitions {
                    _ = txn_detail
                        .offsets
                        .entry(offsets.group_id.clone())
                        .or_default()
                        .entry(topic.name.clone())
                        .or_default()
                        .insert(
                            partition.partition_index,
                            state::TxnCommitOffset {
                                committed_offset: partition.committed_offset,
                                leader_epoch: partition.committed_leader_epoch,
                                metadata: partition.committed_metadata.clone(),
                            },
                        );

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
        // collecting the partitions this txn produced to.
        let produced: Vec<Topition> = {
            let mut coord = self.coord.lock().await;

            let txn_detail =
                coord.txn_detail_mut(transaction_id, producer_id, producer_epoch)?;

            let mut produced = vec![];

            if txn_detail.state == Some(TxnState::Begin) {
                txn_detail.state = Some(if committed {
                    TxnState::PrepareCommit
                } else {
                    TxnState::PrepareAbort
                });

                for (topic, partitions) in &txn_detail.produces {
                    for (partition, offset_range) in partitions {
                        if offset_range.is_some() {
                            produced.push(Topition::new(topic.to_owned(), *partition));
                        }
                    }
                }
            }

            produced
        };

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
                debug!(transaction_id, producer_id, producer_epoch, ?overlaps, "deferred");
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
                            range.map(|range| {
                                (Topition::new(topic.to_owned(), *partition), range)
                            })
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
                            inner.aborted.push(AbortedRange {
                                producer: effect.producer_id,
                                offset_start: range.offset_start,
                                offset_end: range.offset_end,
                            });
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

    async fn maintain(&self, _now: SystemTime) -> Result<()> {
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
                error!(?err, transaction_id, producer_id, producer_epoch, "sweep abort failed");
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
    use super::*;

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
