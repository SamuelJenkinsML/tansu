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

use std::{collections::BTreeMap, fmt::Debug, path::PathBuf, str::FromStr, time::SystemTime};

use async_trait::async_trait;
use tansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_response::AbortedTransaction,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    record::deflated,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tansu_schema::Registry;
use tokio::time::Duration;
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version,
};

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

/// The `nvme://` storage engine.
#[derive(Debug)]
pub struct Engine {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    data_dir: PathBuf,
    // Read from M1 (in-memory engine) onwards.
    #[allow(dead_code)]
    schemas: Option<Registry>,
    #[allow(dead_code)]
    config: Config,
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
}

fn unimplemented(method: &str) -> Error {
    Error::Message(format!("nvme storage engine: {method} is not implemented"))
}

#[async_trait]
impl Storage for Engine {
    async fn register_broker(
        &self,
        _broker_registration: BrokerRegistrationRequest,
    ) -> Result<()> {
        Ok(())
    }

    async fn create_topic(&self, _topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        Err(unimplemented("create_topic"))
    }

    async fn incremental_alter_resource(
        &self,
        _resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        Err(unimplemented("incremental_alter_resource"))
    }

    async fn delete_records(
        &self,
        _topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        Err(unimplemented("delete_records"))
    }

    async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
        Err(unimplemented("delete_topic"))
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        Err(unimplemented("brokers"))
    }

    async fn produce(
        &self,
        _transaction_id: Option<&str>,
        _topition: &Topition,
        _batch: deflated::Batch,
    ) -> Result<i64> {
        Err(unimplemented("produce"))
    }

    async fn fetch(
        &self,
        _topition: &'_ Topition,
        _offset: i64,
        _min_bytes: u32,
        _max_bytes: u32,
        _isolation: IsolationLevel,
        _max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        Err(unimplemented("fetch"))
    }

    async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
        Err(unimplemented("offset_stage"))
    }

    async fn aborted_transactions(
        &self,
        _topition: &Topition,
        _fetch_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        Err(unimplemented("aborted_transactions"))
    }

    async fn list_offsets(
        &self,
        _isolation_level: IsolationLevel,
        _offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        Err(unimplemented("list_offsets"))
    }

    async fn offset_commit(
        &self,
        _group_id: &str,
        _retention_time_ms: Option<Duration>,
        _offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        Err(unimplemented("offset_commit"))
    }

    async fn offset_fetch(
        &self,
        _group_id: Option<&str>,
        _topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        Err(unimplemented("offset_fetch"))
    }

    async fn committed_offset_topitions(&self, _group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        Err(unimplemented("committed_offset_topitions"))
    }

    async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        Err(unimplemented("metadata"))
    }

    async fn upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Err(unimplemented("upsert_user_scram_credential"))
    }

    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Err(unimplemented("delete_user_scram_credential"))
    }

    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Err(unimplemented("user_scram_credential"))
    }

    async fn describe_config(
        &self,
        _name: &str,
        _resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        Err(unimplemented("describe_config"))
    }

    async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        Err(unimplemented("list_groups"))
    }

    async fn delete_groups(
        &self,
        _group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        Err(unimplemented("delete_groups"))
    }

    async fn describe_groups(
        &self,
        _group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        Err(unimplemented("describe_groups"))
    }

    async fn describe_topic_partitions(
        &self,
        _topics: Option<&[TopicId]>,
        _partition_limit: i32,
        _cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        Err(unimplemented("describe_topic_partitions"))
    }

    async fn update_group(
        &self,
        _group_id: &str,
        _detail: GroupDetail,
        _version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        Err(UpdateError::Error(unimplemented("update_group")))
    }

    async fn init_producer(
        &self,
        _transaction_id: Option<&str>,
        _transaction_timeout_ms: i32,
        _producer_id: Option<i64>,
        _producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        Err(unimplemented("init_producer"))
    }

    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Err(unimplemented("txn_add_offsets"))
    }

    async fn txn_add_partitions(
        &self,
        _partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        Err(unimplemented("txn_add_partitions"))
    }

    async fn txn_offset_commit(
        &self,
        _offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        Err(unimplemented("txn_offset_commit"))
    }

    async fn txn_end(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _committed: bool,
    ) -> Result<ErrorCode> {
        Err(unimplemented("txn_end"))
    }

    async fn maintain(&self, _now: SystemTime) -> Result<()> {
        Ok(())
    }

    async fn maintain_transactions(&self, _now: SystemTime) -> Result<()> {
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
