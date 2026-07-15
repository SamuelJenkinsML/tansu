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

//! Group-commit fsync: one dedicated thread per open appendable file.
//!
//! Appenders write to the file themselves (buffered, under their own lock,
//! so ordering is theirs) and then submit an ack request here. The flusher
//! blocks on the first request, drains everything already queued, issues one
//! `fdatasync`, and acks the whole batch — natural batching with zero added
//! latency when idle and amortized fsyncs under load. `FsyncMode::Interval`
//! acks after the write and syncs on a timer instead (not EOS-safe; for
//! experiments only).

use std::{
    fs::File,
    sync::{
        Arc, LazyLock,
        mpsc::{Receiver, RecvTimeoutError, Sender, channel},
    },
    time::Instant,
};

use opentelemetry::metrics::Histogram;
use tokio::sync::oneshot;
use tracing::{debug, error};

use super::FsyncMode;
use crate::{Error, METER, Result};

static FSYNC_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("tansu_nvme_fsync_duration")
        .with_unit("s")
        .with_description("fdatasync latency on nvme segment/WAL files")
        .build()
});

static GROUP_COMMIT_BATCH: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_nvme_group_commit_batch")
        .with_description("acks amortized by one nvme group-commit fsync")
        .build()
});

enum Job {
    Sync(oneshot::Sender<Result<()>>),
    /// Final fsync at seal/shutdown; acks when everything before it is durable.
    Seal(oneshot::Sender<Result<()>>),
}

#[derive(Debug)]
pub(crate) struct Flusher {
    sender: Sender<Job>,
}

impl Flusher {
    /// Spawn the flusher thread for `file` (a handle cloned from the
    /// writer's; both refer to the same open description).
    pub(crate) fn spawn(name: String, file: Arc<File>, mode: FsyncMode) -> Self {
        let (sender, receiver) = channel();
        let thread_name = format!("nvme-fsync-{name}");

        _ = std::thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || run(&name, &file, mode, &receiver))
            .inspect_err(|err| error!(?err, thread_name, "flusher spawn"));

        Self { sender }
    }

    /// Request an ack once everything written to the file so far is durable.
    pub(crate) fn sync(&self) -> Result<oneshot::Receiver<Result<()>>> {
        let (ack, receiver) = oneshot::channel();

        self.sender
            .send(Job::Sync(ack))
            .map_err(|_| Error::Message("nvme flusher stopped".into()))?;

        Ok(receiver)
    }

    /// Final fsync; the flusher thread exits after acking (and after the
    /// engine drops this handle).
    pub(crate) fn seal(&self) -> Result<oneshot::Receiver<Result<()>>> {
        let (ack, receiver) = oneshot::channel();

        self.sender
            .send(Job::Seal(ack))
            .map_err(|_| Error::Message("nvme flusher stopped".into()))?;

        Ok(receiver)
    }
}

fn sync_error(err: &std::io::Error) -> Error {
    Error::Message(format!("nvme fdatasync: {err}"))
}

fn run(name: &str, file: &File, mode: FsyncMode, receiver: &Receiver<Job>) {
    let interval = match mode {
        FsyncMode::Always => None,
        FsyncMode::Interval(interval) => Some(interval),
    };

    let mut dirty = false;

    loop {
        // Block for the first request (or tick the interval timer).
        let first = if let Some(interval) = interval {
            match receiver.recv_timeout(interval) {
                Ok(job) => Some(job),
                Err(RecvTimeoutError::Timeout) => {
                    if dirty && let Err(err) = file.sync_data() {
                        error!(?err, name, "interval fsync");
                    }
                    dirty = false;
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => None,
            }
        } else {
            receiver.recv().ok()
        };

        let Some(first) = first else {
            // Engine dropped the handle: final best-effort sync and exit.
            if dirty {
                _ = file.sync_data().inspect_err(|err| error!(?err, name));
            }
            debug!(name, "flusher exit");
            return;
        };

        // Drain everything already queued into one fsync batch.
        let mut batch = vec![first];
        while let Ok(job) = receiver.try_recv() {
            batch.push(job);
        }

        let has_seal = batch.iter().any(|job| matches!(job, Job::Seal(_)));

        // Always mode syncs every batch; interval mode only syncs for seals
        // (writes were already acked as non-durable-by-contract).
        let outcome: Result<()> = if interval.is_none() || has_seal {
            let started_at = Instant::now();
            let outcome = file.sync_data().map_err(|err| sync_error(&err));
            FSYNC_DURATION.record(started_at.elapsed().as_secs_f64(), &[]);
            GROUP_COMMIT_BATCH.record(batch.len() as u64, &[]);
            dirty = false;
            debug!(name, batch = batch.len(), ok = outcome.is_ok());
            outcome
        } else {
            dirty = true;
            Ok(())
        };

        for job in batch {
            let (Job::Sync(ack) | Job::Seal(ack)) = job;
            _ = ack.send(outcome.clone());
        }

        // A sealed file's flusher exits once its final sync is acked.
        if has_seal {
            debug!(name, "flusher sealed");
            return;
        }
    }
}
