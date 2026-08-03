# This is a fork

`SamuelJenkinsML/tansu` is a fork of [tansu-io/tansu](https://github.com/tansu-io/tansu),
maintained for the [Thyme](https://github.com/SamuelJenkinsML/thyme) streaming feature
platform. Upstream is the canonical project — if you want Tansu, go there.

## What this fork adds

| Area | Change |
|---|---|
| **`nvme://` storage engine** | A local append-log engine: per-partition segment logs, group-commit `fdatasync`, a metadata WAL with snapshots, and object-store tiering with zero-loss cold bootstrap. `tansu-storage/src/nvme/`. |
| **EOS aborted-transaction index** | `read_committed` fetches leaked records from aborted transactions because the fetch response hardcoded an empty aborted list. Adds `Storage::aborted_transactions` and populates it, on the dynostore, Postgres and nvme backends. |
| **EOS abort-path fixes** | A terminal-overlap livelock that pinned the last stable offset forever after any abort, and a same-epoch re-begin that corrupted the aborted index. Both fixed across all three backends. |
| **Transaction timeout handling** | `transaction.timeout.ms` validation in `InitProducerId`, plus a sweep that aborts timed-out transactions. |

Most of this is intended to go upstream. Branch topic PRs off `origin/main`
(upstream) rather than off this fork's `main`.

## CI policy — read before re-enabling anything

> [!IMPORTANT]
> **Upstream's `ci.yml` and `release.yml` are disabled on this fork**, via
> `gh workflow disable`, not by editing the files. That is deliberate: it keeps
> upstream rebases conflict-free and leaves both workflows valid for PRs we send
> upstream. Because it is repository state rather than code, nothing in the tree
> tells you they are off — hence this note.

Upstream's `ci.yml` is **38 jobs, roughly 410 billed minutes per run**: a postgres
16/17/18 test matrix, `build-storage-lake` at 5 storage × 3 lake, the librdkafka
and franz-go compat suites across every backend, plus `miri`, `fuzz`, `package`
and `smoke` — with `cache: false` set on every job. It is the right suite for a
project supporting every backend and both lake formats. It is the wrong suite for
a fork that ships one storage engine.

Dependabot was removed for the same reason. It scheduled *daily* `cargo` and
`github-actions` updates against `main`, and every PR it opened triggered the full
38-job suite. On 2026-07-14 that fired 11 times in a single day — approximately
4,500 minutes, all of them failing. This fork tracks a pinned upstream revision;
upstream owns dependency currency.

To run the full upstream suite, do it locally:

```shell
cp example.env .env
just ci      # docker compose: minio + postgres + lakekeeper
just test    # nextest --workspace --all-targets --all-features
```

### What runs instead

| Workflow | Trigger | What it does |
|---|---|---|
| `fork-ci.yml` | PRs to `main`, manual | `cargo fmt --check`, `clippy --workspace --all-targets -D warnings`, and `nextest -E 'test(nvme)'` (61 tests: the nvme unit and frame property tests, plus the nvme arm of every broker conformance suite). Default features only — **not** `--all-features`, which pulls in delta/iceberg/parquet and their datafusion and arrow trees. |
| `publish-image.yml` | push to `main`, manual | Builds `linux/arm64` on a native arm64 runner and pushes to Thyme's ECR as `thyme/tansu:git-<sha>`. |

The librdkafka and franz-go compat suites are not gone — they run once, in-cluster
against real instance-store NVMe, as a release gate.

## Branch model

- `origin` → upstream `tansu-io/tansu`. Sync with `git merge origin/main`.
- `main` (this fork) carries the Thyme work, because `publish-image.yml` builds on
  push to it.
- Upstream PRs branch off `origin/main` and cherry-pick.

## Image publishing

`publish-image.yml` triggers on **push to `main` only, never `pull_request`** — this
repository is public, and that is what prevents a PR from an external fork reaching
the OIDC role. The AWS trust policy lives in `thyme-infra`
(`terraform/modules/ci_build`) and is scoped to `repo:<repo>:ref:refs/heads/main`.

Consumers pin the `git-<sha>` tag rather than `latest`: a benchmark result that
cannot name the exact image it ran on is not reproducible.
