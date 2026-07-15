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
//! never contend. Batch bytes live in segment files; the index holds their
//! locations plus a bounded tail cache of recently produced batches. The high
//! watermark only advances after the batch's group-commit fsync acks, so
//! readers can never observe data a crash would lose.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs::File,
    path::PathBuf,
    sync::Arc,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use super::log::SegmentWriter;
use super::state::{Offset, ProducerEpoch, ProducerId};

/// Key identifying one transaction incarnation on this partition.
pub(crate) type OpenTxnKey = (String, ProducerId, ProducerEpoch);

#[derive(Clone, Debug)]
pub(crate) struct BatchEntry {
    /// Location of the frame in its segment.
    pub segment_base: i64,
    pub position: u64,
    pub len: u32,
    /// Raw batch bytes while within the tail-cache budget.
    pub cached: Option<Bytes>,
    pub last_offset_delta: i32,
    pub max_timestamp: i64,
    pub is_control: bool,
    pub producer_id: ProducerId,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AbortedRange {
    pub producer: ProducerId,
    pub offset_start: Offset,
    pub offset_end: Offset,
}

#[derive(Debug)]
pub(crate) struct PartitionInner {
    /// This partition's directory of segment files.
    pub dir: PathBuf,
    pub log_start: Offset,
    /// Next offset to assign (appended, not necessarily durable).
    pub next_offset: Offset,
    /// First offset NOT yet durable: the high watermark readers see.
    pub durable_offset: Offset,
    pub batches: BTreeMap<Offset, BatchEntry>,
    /// Open transactions and the first offset each produced here. The last
    /// stable offset is the minimum; entries are removed (and, on abort,
    /// moved into `aborted`) at the terminal transition in txn_end.
    pub open_txns: BTreeMap<OpenTxnKey, Offset>,
    /// Aborted-transaction index for read_committed fetches, GC'd as
    /// log_start advances past a range.
    pub aborted: Vec<AbortedRange>,
    /// The active segment; created on first produce (a fresh segment per
    /// boot epoch — recovery never re-opens a tail for append).
    pub writer: Option<SegmentWriter>,
    /// Read handles for positioned reads, one per segment, opened on demand.
    pub read_files: HashMap<i64, Arc<File>>,
    /// Tail-cache bookkeeping: offsets whose entries may still hold bytes.
    pub cache_queue: VecDeque<Offset>,
    pub cached_bytes: u64,
}

impl PartitionInner {
    pub(crate) fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            log_start: 0,
            next_offset: 0,
            durable_offset: 0,
            batches: BTreeMap::new(),
            open_txns: BTreeMap::new(),
            aborted: Vec::new(),
            writer: None,
            read_files: HashMap::new(),
            cache_queue: VecDeque::new(),
            cached_bytes: 0,
        }
    }

    pub(crate) fn high_watermark(&self) -> Offset {
        self.durable_offset
    }

    pub(crate) fn last_stable(&self) -> Offset {
        self.open_txns
            .values()
            .copied()
            .min()
            .unwrap_or(self.durable_offset)
            .min(self.durable_offset)
    }

    /// Record a produced batch in the index and the tail cache.
    pub(crate) fn index(&mut self, offset: Offset, entry: BatchEntry, cache_budget: u64) {
        if let Some(ref bytes) = entry.cached {
            self.cached_bytes += bytes.len() as u64;
            self.cache_queue.push_back(offset);
        }

        _ = self.batches.insert(offset, entry);

        while self.cached_bytes > cache_budget {
            let Some(oldest) = self.cache_queue.pop_front() else {
                break;
            };

            if let Some(entry) = self.batches.get_mut(&oldest)
                && let Some(bytes) = entry.cached.take()
            {
                self.cached_bytes -= bytes.len() as u64;
            }
        }
    }

    /// A positioned-read handle for one segment, opened on demand. The
    /// active segment reads through the writer's own handle.
    pub(crate) fn read_handle(&mut self, segment_base: Offset) -> crate::Result<Arc<File>> {
        if let Some(ref writer) = self.writer
            && writer.base_offset == segment_base
        {
            return Ok(writer.file.clone());
        }

        if let Some(file) = self.read_files.get(&segment_base) {
            return Ok(file.clone());
        }

        let path = super::log::segment_path(&self.dir, segment_base);
        let file = File::open(&path)
            .map(Arc::new)
            .map_err(|err| crate::Error::Message(format!("nvme open {path:?}: {err}")))?;

        _ = self.read_files.insert(segment_base, file.clone());

        Ok(file)
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

        for (base, entry) in self.batches.iter() {
            if entry.cached.is_some()
                && *base + Offset::from(entry.last_offset_delta) < clamped
                && let Some(ref bytes) = entry.cached
            {
                self.cached_bytes = self.cached_bytes.saturating_sub(bytes.len() as u64);
            }
        }

        self.batches = keep;
        for (base, entry) in straddling {
            _ = self.batches.insert(base, entry);
        }

        self.aborted.retain(|range| range.offset_end >= clamped);

        self.log_start
    }
}

#[derive(Debug)]
pub(crate) struct PartitionState {
    pub inner: std::sync::Mutex<PartitionInner>,
    /// Woken on durable high-watermark or last-stable-offset advance, for
    /// bounded fetch long-polling.
    pub notify: Notify,
}

impl PartitionState {
    pub(crate) fn new(dir: PathBuf) -> Self {
        Self {
            inner: std::sync::Mutex::new(PartitionInner::new(dir)),
            notify: Notify::new(),
        }
    }
}
