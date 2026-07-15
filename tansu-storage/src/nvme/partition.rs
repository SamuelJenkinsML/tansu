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

//! Per-partition log state: the batch index, watermarks, the open-transaction
//! set that pins the last stable offset, and the aborted-transaction index.
//!
//! The partition is the unit of ownership and parallelism: everything here is
//! guarded by this partition's own lock, so producers to different partitions
//! never contend. In M1 the encoded batches live in the index itself; M2
//! moves the bytes into segment files and this index keeps their locations.

use std::collections::BTreeMap;

use bytes::Bytes;
use tokio::sync::Notify;

use super::state::{Offset, ProducerEpoch, ProducerId};

/// Key identifying one transaction incarnation on this partition.
pub(crate) type OpenTxnKey = (String, ProducerId, ProducerEpoch);

#[derive(Clone, Debug)]
pub(crate) struct BatchEntry {
    /// The deflated batch exactly as received on the wire (client-side
    /// base_offset); rewritten to the assigned base offset at fetch.
    pub encoded: Bytes,
    pub last_offset_delta: i32,
    pub max_timestamp: i64,
    /// Read by M2 recovery (marker scan); not on the serve path.
    #[allow(dead_code)]
    pub is_control: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AbortedRange {
    pub producer: ProducerId,
    pub offset_start: Offset,
    pub offset_end: Offset,
}

#[derive(Debug, Default)]
pub(crate) struct PartitionInner {
    pub log_start: Offset,
    /// Next offset to assign; equals the high watermark.
    pub next_offset: Offset,
    pub batches: BTreeMap<Offset, BatchEntry>,
    /// Open transactions and the first offset each produced here. The last
    /// stable offset is the minimum; entries are removed (and, on abort,
    /// moved into `aborted`) at the terminal transition in txn_end.
    pub open_txns: BTreeMap<OpenTxnKey, Offset>,
    /// Aborted-transaction index for read_committed fetches, GC'd as
    /// log_start advances past a range.
    pub aborted: Vec<AbortedRange>,
}

impl PartitionInner {
    pub(crate) fn high_watermark(&self) -> Offset {
        self.next_offset
    }

    pub(crate) fn last_stable(&self) -> Offset {
        self.open_txns
            .values()
            .copied()
            .min()
            .unwrap_or(self.next_offset)
    }

    /// Advance the log start offset (delete-records / retention), dropping
    /// batches and aborted-index entries that end below it. A batch
    /// straddling the new start is kept whole, as in Kafka.
    pub(crate) fn advance_log_start(&mut self, offset: Offset) -> Offset {
        let clamped = offset.clamp(self.log_start, self.high_watermark());
        self.log_start = clamped;

        let keep = self.batches.split_off(&clamped);
        let straddling: Vec<_> = self
            .batches
            .iter()
            .filter(|(base, entry)| **base + Offset::from(entry.last_offset_delta) >= clamped)
            .map(|(base, entry)| (*base, entry.clone()))
            .collect();

        self.batches = keep;
        for (base, entry) in straddling {
            _ = self.batches.insert(base, entry);
        }

        self.aborted.retain(|range| range.offset_end >= clamped);

        self.log_start
    }
}

#[derive(Debug, Default)]
pub(crate) struct PartitionState {
    pub inner: std::sync::Mutex<PartitionInner>,
    /// Woken on durable high-watermark or last-stable-offset advance, for
    /// bounded fetch long-polling.
    pub notify: Notify,
}
