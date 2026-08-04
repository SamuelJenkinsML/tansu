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

//! Coordination state: producers, transactions, topics and SCRAM credentials.
//!
//! Everything here is plain in-memory data guarded by the engine's single
//! coordination mutex; durability comes from the metadata WAL + snapshots
//! (both replayed at boot), never from rewriting this state to disk inline.

use std::{collections::BTreeMap, time::SystemTime};

use serde::{Deserialize, Serialize};
use tansu_sans_io::{
    OpType,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    incremental_alter_configs_request::AlterableConfig,
};
use uuid::Uuid;

use crate::{Error, Result, ScramCredential, TopicId, TxnState};

pub(crate) type Group = String;
pub(crate) type Offset = i64;
pub(crate) type Partition = i32;
pub(crate) type ProducerEpoch = i16;
pub(crate) type ProducerId = i64;
pub(crate) type Sequence = i32;
pub(crate) type Topic = String;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ProducerDetail {
    pub sequences: BTreeMap<ProducerEpoch, BTreeMap<Topic, BTreeMap<Partition, Sequence>>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct TxnProduceOffset {
    pub offset_start: Offset,
    pub offset_end: Offset,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct TxnCommitOffset {
    pub committed_offset: Offset,
    pub leader_epoch: Option<i32>,
    pub metadata: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct TxnDetail {
    pub transaction_timeout_ms: i32,
    pub started_at: Option<SystemTime>,
    pub state: Option<TxnState>,
    pub produces: BTreeMap<Topic, BTreeMap<Partition, Option<TxnProduceOffset>>>,
    pub offsets: BTreeMap<Group, BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Txn {
    pub producer: ProducerId,
    pub epochs: BTreeMap<ProducerEpoch, TxnDetail>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct TopicMetadata {
    pub id: Uuid,
    pub topic: CreatableTopic,
}

/// A reference to one transaction incarnation, as returned by the overlap
/// scan in txn_end's resolution gate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TxnRef {
    pub transaction: String,
    pub producer_id: ProducerId,
    pub producer_epoch: ProducerEpoch,
    pub state: TxnState,
}

// No serde: ScramCredential holds raw Bytes; the M2 snapshot serializes the
// serde-able component types individually.
#[derive(Clone, Debug, Default)]
pub(crate) struct CoordState {
    pub producers: BTreeMap<ProducerId, ProducerDetail>,
    pub transactions: BTreeMap<String, Txn>,
    pub topics: BTreeMap<Topic, TopicMetadata>,
    pub scram: BTreeMap<(String, String), ScramCredential>,
}

impl CoordState {
    pub(crate) fn topic_metadata(&self, topic: &TopicId) -> Option<&TopicMetadata> {
        match topic {
            TopicId::Name(name) => self.topics.get(name.as_str()),
            TopicId::Id(id) => self.topics.values().find(|metadata| &metadata.id == id),
        }
    }

    /// Locate the transaction, validating producer id and (current) epoch,
    /// mirroring the dynostore validation order: TransactionalIdNotFound,
    /// UnknownProducerId, then ProducerFenced for a missing or stale epoch.
    pub(crate) fn txn_detail_mut(
        &mut self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<&mut TxnDetail> {
        let Some(transaction) = self.transactions.get_mut(transaction_id) else {
            return Err(Error::Api(
                tansu_sans_io::ErrorCode::TransactionalIdNotFound,
            ));
        };

        if transaction.producer != producer_id {
            return Err(Error::Api(tansu_sans_io::ErrorCode::UnknownProducerId));
        }

        let Some((current_epoch, _)) = transaction.epochs.last_key_value() else {
            return Err(Error::Api(tansu_sans_io::ErrorCode::ProducerFenced));
        };

        if current_epoch != &producer_epoch {
            return Err(Error::Api(tansu_sans_io::ErrorCode::ProducerFenced));
        }

        Ok(transaction
            .epochs
            .get_mut(&producer_epoch)
            .expect("epoch present: checked above"))
    }

    /// Idempotent-producer sequence validation, bumping the stored sequence
    /// by the batch's record count on success (dynostore produce semantics).
    pub(crate) fn validate_sequence(
        &mut self,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        base_sequence: Sequence,
        last_offset_delta: i32,
        topic: &str,
        partition: Partition,
    ) -> Result<()> {
        use tansu_sans_io::ErrorCode;

        let Some(pd) = self.producers.get_mut(&producer_id) else {
            return Err(Error::Api(ErrorCode::UnknownProducerId));
        };

        let Some(mut current) = pd.sequences.last_entry() else {
            return Err(Error::Api(ErrorCode::UnknownServerError));
        };

        if current.key() != &producer_epoch {
            return Err(Error::Api(ErrorCode::ProducerFenced));
        }

        let sequence = current
            .get_mut()
            .entry(topic.to_owned())
            .or_default()
            .entry(partition)
            .or_default();

        if *sequence < base_sequence {
            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
        } else if *sequence > base_sequence {
            Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
        } else {
            *sequence += last_offset_delta + 1;
            Ok(())
        }
    }

    /// Merge a transactional produce into the txn's per-partition offset
    /// range. The txn must be in Begin: without this guard a concurrently
    /// swept (timed-out) txn would leak ranges into its next incarnation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_txn_produce(
        &mut self,
        transaction_id: &str,
        producer_epoch: ProducerEpoch,
        topic: &str,
        partition: Partition,
        offset_start: Offset,
        offset_end: Offset,
        is_control: bool,
    ) -> Result<()> {
        let Some(transaction) = self.transactions.get_mut(transaction_id) else {
            return Ok(());
        };

        let Some(txn_detail) = transaction.epochs.get_mut(&producer_epoch) else {
            return Ok(());
        };

        // Control (end-txn marker) batches are produced while the txn is in
        // PrepareCommit/PrepareAbort and extend the range so read_committed
        // consumers skip past the marker; data batches require an open txn.
        if !is_control && txn_detail.state != Some(TxnState::Begin) {
            return Err(Error::Api(tansu_sans_io::ErrorCode::InvalidTxnState));
        }

        _ = txn_detail
            .produces
            .entry(topic.to_owned())
            .or_default()
            .entry(partition)
            .and_modify(|entry| {
                let range = entry.get_or_insert(TxnProduceOffset {
                    offset_start,
                    offset_end,
                });

                if offset_end > range.offset_end {
                    range.offset_end = offset_end;
                }
            })
            .or_insert(Some(TxnProduceOffset {
                offset_start,
                offset_end,
            }));

        Ok(())
    }

    /// The produced offset ranges of one transaction incarnation.
    pub(crate) fn produced(
        &self,
        transaction_id: &str,
        producer_epoch: ProducerEpoch,
    ) -> Vec<(Topic, Partition, TxnProduceOffset)> {
        self.transactions
            .get(transaction_id)
            .and_then(|txn| txn.epochs.get(&producer_epoch))
            .map(|txn_detail| {
                txn_detail
                    .produces
                    .iter()
                    .flat_map(|(topic, partitions)| {
                        partitions.iter().filter_map(|(partition, range)| {
                            range.map(|range| (topic.clone(), *partition, range))
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Transactions whose produced ranges overlap the candidate's on any
    /// partition, ported from the fixed dynostore `overlapping_transactions`:
    /// terminal (Committed/Aborted) txns are excluded — they can never become
    /// prepared, and counting them deadlocks the resolution gate in txn_end.
    pub(crate) fn overlapping_transactions(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Vec<TxnRef> {
        let candidates: BTreeMap<(Topic, Partition), TxnProduceOffset> = self
            .produced(transaction_id, producer_epoch)
            .into_iter()
            .map(|(topic, partition, range)| ((topic, partition), range))
            .collect();

        let mut overlapping = Vec::new();

        'candidates: for (candidate_id, txn) in self.transactions.iter() {
            for (epoch, txn_detail) in txn.epochs.iter() {
                if transaction_id == candidate_id
                    && producer_id == txn.producer
                    && producer_epoch == *epoch
                {
                    continue;
                }

                let Some(state) = txn_detail.state else {
                    continue;
                };

                if matches!(state, TxnState::Committed | TxnState::Aborted) {
                    continue;
                }

                for (topic, partitions) in txn_detail.produces.iter() {
                    for (partition, offset_range) in partitions.iter() {
                        let Some(offset_range) = offset_range else {
                            continue;
                        };

                        if let Some(candidate) = candidates.get(&(topic.clone(), *partition))
                            && offset_range.offset_start < candidate.offset_end
                        {
                            overlapping.push(TxnRef {
                                transaction: candidate_id.to_owned(),
                                producer_id: txn.producer,
                                producer_epoch: *epoch,
                                state,
                            });

                            continue 'candidates;
                        }
                    }
                }
            }
        }

        overlapping
    }

    /// Apply incremental topic-config changes (Set/Delete/Append/Subtract on
    /// comma-separated lists), dynostore `Meta::alter_topic` semantics.
    pub(crate) fn alter_topic(&mut self, topic: &str, changes: &[AlterableConfig]) -> Result<()> {
        if let Some(metadata) = self.topics.get_mut(topic) {
            let mut configuration = metadata
                .topic
                .configs
                .as_deref()
                .unwrap_or_default()
                .iter()
                .fold(BTreeMap::new(), |mut acc, item| {
                    _ = acc.insert(item.name.clone(), item.value.clone());
                    acc
                });

            for change in changes {
                match OpType::try_from(change.config_operation)? {
                    OpType::Set => {
                        _ = configuration.insert(change.name.clone(), change.value.clone());
                    }
                    OpType::Delete => {
                        _ = configuration.remove(change.name.as_str());
                    }
                    OpType::Append => {
                        let appended = change.value.as_deref().unwrap_or_default();

                        _ = configuration
                            .entry(change.name.clone())
                            .and_modify(|value| match value {
                                Some(current) if !current.is_empty() => {
                                    if !current.split(',').any(|item| item == appended) {
                                        *current = format!("{current},{appended}");
                                    }
                                }
                                _ => *value = Some(appended.to_owned()),
                            })
                            .or_insert_with(|| Some(appended.to_owned()));
                    }
                    OpType::Subtract => {
                        let subtracted = change.value.as_deref().unwrap_or_default();

                        if let Some(Some(current)) = configuration.get_mut(change.name.as_str()) {
                            *current = current
                                .split(',')
                                .filter(|item| *item != subtracted)
                                .collect::<Vec<_>>()
                                .join(",");
                        }
                    }
                }
            }

            _ = metadata
                .topic
                .configs
                .replace(
                    configuration
                        .into_iter()
                        .fold(Vec::new(), |mut acc, (key, value)| {
                            acc.push(CreatableTopicConfig::default().name(key).value(value));
                            acc
                        }),
                );
        }

        Ok(())
    }
}
