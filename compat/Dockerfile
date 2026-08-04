# ---------------------------------------------------------------------------
# Kafka client-compatibility suites, packaged to run against a REMOTE broker.
#
# Both compat/*/run.sh scripts already read ${BOOTSTRAP_SERVERS}, so the only
# thing standing between them and a broker in Kubernetes was the build: the
# librdkafka runner is C that has to be compiled, and franz-go needs a Go
# toolchain plus its module cache. This image does both at build time so the
# Job that runs on the cluster only has to execute tests.
#
# Why this matters for TH-235: the in-tree conformance tests exercise the
# storage layer in-process, and Thyme's own gates exercise the paths Thyme
# happens to use. Neither shows whether an arbitrary Kafka client can talk to
# the broker. These two suites do, over the wire, against the real deployed
# broker on real instance-store NVMe -- protocol correctness independent of
# Thyme.
#
# Expected baseline (memory:// locally, recorded in compat/*/FINDINGS.md):
#   librdkafka  73/74  -- 0060 also fails on memory://, so it is not an NVMe
#                         regression; it is a known broker gap.
#   franz-go    61/61
# A cloud run that matches those counts means the nvme:// engine introduces no
# client-visible protocol regression. Divergence is the finding.
#
# arm64: the demo cluster is Graviton throughout.
# ---------------------------------------------------------------------------
FROM golang:1.24-bookworm

ARG LIBRDKAFKA_VERSION=v2.14.2
ARG FRANZ_GO_VERSION=v1.21.3

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      git \
      ca-certificates \
      libssl-dev \
      zlib1g-dev \
      libzstd-dev \
      python3 \
      coreutils \
  && rm -rf /var/lib/apt/lists/*

# The layout is load-bearing. run.sh derives COMPAT_DIR from its own location
# and defaults WORK_DIR to ${COMPAT_DIR}/../../target/compat, so placing the
# scripts at /work/compat/<suite>/ puts the default work dir at
# /work/target/compat -- exactly where the sources are cloned below. Both
# scripts skip their `git clone` when the source directory already exists, so
# the container never needs network access to GitHub at run time.
WORKDIR /work
ENV WORK_DIR=/work/target/compat

RUN git clone --depth 1 --branch "${LIBRDKAFKA_VERSION}" \
      https://github.com/confluentinc/librdkafka.git "${WORK_DIR}/librdkafka" \
 && cd "${WORK_DIR}/librdkafka" \
 && ./configure --disable-curl --disable-sasl \
 && make -j "$(nproc)" libs \
 && make -j "$(nproc)" -C tests build

# Build the franz-go test binary at image-build time so the Job does not
# compile Go on the cluster. `go test -c -o /dev/null` populates the module and
# build caches without producing an artefact run.sh would ignore anyway -- it
# invokes `go test` itself, which then hits a warm cache.
RUN git clone --depth 1 --branch "${FRANZ_GO_VERSION}" \
      https://github.com/twmb/franz-go.git "${WORK_DIR}/franz-go" \
 && cd "${WORK_DIR}/franz-go/pkg/kgo" \
 && go test -c -o /dev/null .

COPY compat/ /work/compat/

COPY compat/entrypoint.sh /usr/local/bin/run-compat
RUN chmod +x /usr/local/bin/run-compat

ENTRYPOINT ["/usr/local/bin/run-compat"]
