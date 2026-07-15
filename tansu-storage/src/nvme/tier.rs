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

//! Object-store tiering: sealed segments (verbatim) plus a small index
//! sidecar per segment, and a mirror of each coordination snapshot. The
//! object store is durability, retention and bootstrap; local NVMe stays the
//! hot path. With a sidecar, a cold boot rebuilds every index without
//! downloading a byte of record data — tiered batches are then served by
//! ranged GETs.
//!
//! ```text
//! {prefix}/topics/{topic}-{partition:010}/segments/{base:020}.seg
//! {prefix}/topics/{topic}-{partition:010}/segments/{base:020}.idx
//! {prefix}/snapshots/{wal_seq:020}.snap
//! ```

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use futures::StreamExt;
use object_store::{DynObjectStore, ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path};
use opentelemetry::metrics::Gauge;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;
use tracing::{debug, warn};
use url::Url;

use super::{
    frame::{self, Decoded, FrameType},
    log,
    partition::PartitionState,
};
use crate::{Error, METER, Result, Topition};

static TIER_UPLOAD_LAG: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_nvme_tier_upload_lag_bytes")
        .with_description("sealed segment bytes not yet uploaded to the tier")
        .build()
});

/// One batch's location and recovery metadata, mirrored from the local
/// index so a cold boot never has to scan the segment bytes.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(crate) struct SidecarEntry {
    pub offset: i64,
    pub position: u64,
    pub len: u32,
    pub last_offset_delta: i32,
    pub max_timestamp: i64,
    pub is_control: bool,
    pub is_transactional: bool,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

#[derive(Debug)]
pub(crate) struct TierStore {
    store: Arc<DynObjectStore>,
    prefix: String,
}

impl TierStore {
    /// `tier=s3://bucket[/prefix]` — credentials/endpoint from the
    /// environment, exactly like the s3:// storage engine (MinIO included).
    pub(crate) fn open(tier: &str, cluster: &str) -> Result<Self> {
        let url = Url::parse(tier).map_err(|err| Error::Message(format!("nvme tier: {err}")))?;

        match url.scheme() {
            "s3" => {
                let bucket = url.host_str().unwrap_or("tansu");

                let store = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map(|store| Arc::new(store) as Arc<DynObjectStore>)
                    .map_err(|err| Error::Message(format!("nvme tier s3: {err}")))?;

                let base = url.path().trim_matches('/');
                let prefix = if base.is_empty() {
                    format!("clusters/{cluster}/nvme")
                } else {
                    format!("{base}/clusters/{cluster}/nvme")
                };

                Ok(Self { store, prefix })
            }

            otherwise => Err(Error::Message(format!(
                "nvme tier scheme unsupported: {otherwise}"
            ))),
        }
    }

    fn segment_dir(&self, topition: &Topition) -> String {
        format!(
            "{}/topics/{}-{:0>10}/segments",
            self.prefix,
            topition.topic(),
            topition.partition()
        )
    }

    fn segment_path(&self, topition: &Topition, base: i64) -> Path {
        Path::from(format!("{}/{base:020}.seg", self.segment_dir(topition)))
    }

    fn sidecar_path(&self, topition: &Topition, base: i64) -> Path {
        Path::from(format!("{}/{base:020}.idx", self.segment_dir(topition)))
    }

    fn snapshot_path(&self, wal_seq: u64) -> Path {
        Path::from(format!("{}/snapshots/{wal_seq:020}.snap", self.prefix))
    }

    /// Upload one sealed segment verbatim plus its index sidecar.
    pub(crate) async fn upload_segment(
        &self,
        topition: &Topition,
        base: i64,
        segment: Bytes,
        entries: &[SidecarEntry],
    ) -> Result<()> {
        let sidecar = frame::encode(FrameType::Manifest, &serde_json::to_vec(entries)?);

        _ = self
            .store
            .put(&self.segment_path(topition, base), segment.into())
            .await
            .map_err(|err| Error::Message(format!("nvme tier put seg: {err}")))?;

        // Sidecar second: its presence marks the pair complete.
        _ = self
            .store
            .put(&self.sidecar_path(topition, base), sidecar.into())
            .await
            .map_err(|err| Error::Message(format!("nvme tier put idx: {err}")))?;

        debug!(?topition, base, "tiered");

        Ok(())
    }

    /// Segment bases with a complete (sidecar-present) upload.
    pub(crate) async fn segment_bases(&self, topition: &Topition) -> Result<Vec<i64>> {
        let prefix = Path::from(self.segment_dir(topition));
        let mut stream = self.store.list(Some(&prefix));
        let mut bases = vec![];

        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|err| Error::Message(format!("nvme tier list: {err}")))?;

            if let Some(name) = meta.location.parts().next_back()
                && let Some(stem) = name.as_ref().strip_suffix(".idx")
                && let Ok(base) = stem.parse::<i64>()
            {
                bases.push(base);
            }
        }

        bases.sort_unstable();
        Ok(bases)
    }

    pub(crate) async fn sidecar(
        &self,
        topition: &Topition,
        base: i64,
    ) -> Result<Vec<SidecarEntry>> {
        let bytes = self
            .store
            .get(&self.sidecar_path(topition, base))
            .await
            .map_err(|err| Error::Message(format!("nvme tier get idx: {err}")))?
            .bytes()
            .await
            .map_err(|err| Error::Message(format!("nvme tier read idx: {err}")))?;

        match frame::decode(&bytes) {
            Decoded::Frame(frame) if frame.frame_type == FrameType::Manifest => {
                serde_json::from_slice(&frame.payload).map_err(Into::into)
            }
            _otherwise => Err(Error::Message(format!(
                "nvme tier sidecar corrupt: {topition:?} {base}"
            ))),
        }
    }

    /// Ranged read of one framed batch from a tiered segment.
    pub(crate) async fn read_batch(
        &self,
        topition: &Topition,
        base: i64,
        position: u64,
        len: u32,
    ) -> Result<Bytes> {
        let range = position..(position + u64::from(len));

        let framed = self
            .store
            .get_range(&self.segment_path(topition, base), range)
            .await
            .map_err(|err| Error::Message(format!("nvme tier get_range: {err}")))?;

        match frame::decode(&framed) {
            Decoded::Frame(frame) if frame.frame_type == FrameType::Batch => {
                let mut payload = frame.payload;
                if payload.len() < 8 {
                    return Err(Error::Message("nvme tier: short batch payload".into()));
                }
                _ = bytes::Buf::get_i64_le(&mut payload);
                Ok(payload)
            }
            _otherwise => Err(Error::Message(format!(
                "nvme tier: bad frame {topition:?} {base}@{position}"
            ))),
        }
    }

    pub(crate) async fn upload_snapshot(&self, wal_seq: u64, framed: Bytes) -> Result<()> {
        self.store
            .put(&self.snapshot_path(wal_seq), framed.into())
            .await
            .map(|_| ())
            .map_err(|err| Error::Message(format!("nvme tier put snap: {err}")))
    }

    /// The newest mirrored snapshot, if any.
    pub(crate) async fn latest_snapshot(&self) -> Result<Option<(u64, Bytes)>> {
        let prefix = Path::from(format!("{}/snapshots", self.prefix));
        let mut stream = self.store.list(Some(&prefix));
        let mut newest: Option<u64> = None;

        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|err| Error::Message(format!("nvme tier list: {err}")))?;

            if let Some(name) = meta.location.parts().next_back()
                && let Some(stem) = name.as_ref().strip_suffix(".snap")
                && let Ok(seq) = stem.parse::<u64>()
            {
                newest = Some(newest.map_or(seq, |max: u64| max.max(seq)));
            }
        }

        let Some(seq) = newest else {
            return Ok(None);
        };

        let bytes = self
            .store
            .get(&self.snapshot_path(seq))
            .await
            .map_err(|err| Error::Message(format!("nvme tier get snap: {err}")))?
            .bytes()
            .await
            .map_err(|err| Error::Message(format!("nvme tier read snap: {err}")))?;

        Ok(Some((seq, bytes)))
    }

    /// Delete tiered segments wholly below the retained log start: a
    /// segment goes when the NEXT segment starts at or below the log start,
    /// so a straddling segment always survives. The sidecar goes first —
    /// its absence marks the pair incomplete for bootstrap.
    pub(crate) async fn delete_segments_below(
        &self,
        topition: &Topition,
        log_start: i64,
    ) -> Result<()> {
        let bases = self.segment_bases(topition).await?;

        for (index, base) in bases.iter().enumerate() {
            if bases.get(index + 1).is_none_or(|next| *next > log_start) {
                break;
            }

            for path in [
                self.sidecar_path(topition, *base),
                self.segment_path(topition, *base),
            ] {
                _ = self
                    .store
                    .delete(&path)
                    .await
                    .inspect_err(|err| tracing::warn!(?path, ?err, "tier retention"));
            }

            debug!(?topition, base, "tier retention");
        }

        Ok(())
    }
}

type Partitions = Arc<RwLock<HashMap<Topition, Arc<PartitionState>>>>;

/// The background uploader: tier sealed segments, publish the upload-lag
/// gauge, and evict uploaded segments while the disk budget is tight.
pub(crate) async fn tiering_loop(
    partitions: Partitions,
    tier: Arc<TierStore>,
    disk_usage: Arc<AtomicU64>,
    disk_budget: u64,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        _ = ticker.tick().await;

        if let Err(err) = tick(&partitions, &tier, &disk_usage, disk_budget).await {
            warn!(?err, "tiering tick");
        }
    }
}

async fn tick(
    partitions: &Partitions,
    tier: &TierStore,
    disk_usage: &AtomicU64,
    disk_budget: u64,
) -> Result<()> {
    let list: Vec<(Topition, Arc<PartitionState>)> = partitions
        .read()?
        .iter()
        .map(|(topition, state)| (topition.clone(), state.clone()))
        .collect();

    let mut lag = 0u64;

    for (topition, state) in &list {
        // Sealed, local, not-yet-uploaded segments and their sidecars.
        let jobs: Vec<(i64, std::path::PathBuf, Vec<SidecarEntry>)> = {
            let inner = state.inner.lock()?;
            let active = inner.writer.as_ref().map(|writer| writer.base_offset);

            let mut jobs = vec![];

            for base in log::segment_bases(&inner.dir)? {
                if Some(base) == active || inner.uploaded.contains(&base) {
                    continue;
                }

                let entries: Vec<SidecarEntry> = inner
                    .batches
                    .iter()
                    .filter(|(_, entry)| entry.segment_base == base && !entry.tiered)
                    .map(|(offset, entry)| SidecarEntry {
                        offset: *offset,
                        position: entry.position,
                        len: entry.len,
                        last_offset_delta: entry.last_offset_delta,
                        max_timestamp: entry.max_timestamp,
                        is_control: entry.is_control,
                        is_transactional: entry.is_transactional,
                        producer_id: entry.producer_id,
                        producer_epoch: entry.producer_epoch,
                        base_sequence: entry.base_sequence,
                    })
                    .collect();

                jobs.push((base, log::segment_path(&inner.dir, base), entries));
            }

            jobs
        };

        for (base, path, entries) in jobs {
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => Bytes::from(bytes),
                Err(err) => {
                    warn!(?path, ?err, "tier read");
                    continue;
                }
            };

            let size = bytes.len() as u64;

            match tier.upload_segment(topition, base, bytes, &entries).await {
                Ok(()) => {
                    let mut inner = state.inner.lock()?;
                    _ = inner.uploaded.insert(base);
                }
                Err(err) => {
                    warn!(?topition, base, ?err, "tier upload");
                    lag += size;
                }
            }
        }
    }

    TIER_UPLOAD_LAG.record(lag, &[]);

    // Eviction: keep local usage under the budget by dropping the oldest
    // uploaded sealed segments; their entries flip to remote reads.
    if disk_budget > 0 {
        let mut usage = disk_usage.load(Ordering::Relaxed);
        let high_water = disk_budget * 8 / 10;
        let low_water = disk_budget * 6 / 10;

        if usage > high_water {
            'evict: for (topition, state) in &list {
                loop {
                    let evicted = {
                        let mut inner = state.inner.lock()?;
                        let active = inner.writer.as_ref().map(|writer| writer.base_offset);

                        let Some(base) = inner
                            .uploaded
                            .iter()
                            .copied()
                            .find(|base| {
                                Some(*base) != active
                                    && log::segment_path(&inner.dir, *base).exists()
                            })
                        else {
                            break;
                        };

                        let path = log::segment_path(&inner.dir, base);
                        let size = std::fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);

                        _ = std::fs::remove_file(&path)
                            .inspect_err(|err| warn!(?path, ?err, "tier evict"));
                        _ = inner.read_files.remove(&base);

                        let mut freed_cache = 0u64;
                        let offsets: Vec<i64> = inner
                            .batches
                            .iter()
                            .filter(|(_, entry)| entry.segment_base == base)
                            .map(|(offset, _)| *offset)
                            .collect();

                        for offset in offsets {
                            if let Some(entry) = inner.batches.get_mut(&offset) {
                                entry.tiered = true;
                                if let Some(bytes) = entry.cached.take() {
                                    freed_cache += bytes.len() as u64;
                                }
                            }
                        }

                        inner.cached_bytes = inner.cached_bytes.saturating_sub(freed_cache);

                        debug!(?topition, base, size, "evicted to tier");

                        size
                    };

                    usage = disk_usage
                        .fetch_sub(evicted, Ordering::Relaxed)
                        .saturating_sub(evicted);

                    if usage <= low_water {
                        break 'evict;
                    }
                }
            }
        }
    }

    Ok(())
}
