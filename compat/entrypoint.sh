#!/usr/bin/env bash
#
# Run both Kafka client-compatibility suites against a remote broker and print a
# report card.
#
#   BOOTSTRAP_SERVERS=tansu-0.tansu.thyme.svc.cluster.local:9092 run-compat
#
# Runs BOTH suites even when the first fails. A gate that stops at the first
# failure tells you one thing is broken; this tells you what else is, which is
# what you need when deciding whether an engine change caused a regression or
# merely surfaced a pre-existing gap.
#
# Exit status is non-zero if either suite failed, so it still works as a
# Kubernetes Job gate.
set -uo pipefail

: "${BOOTSTRAP_SERVERS:?set BOOTSTRAP_SERVERS to the broker's host:port}"
export BOOTSTRAP_SERVERS

RESULTS_DIR="${RESULTS_DIR:-/work/results}"
SUITES="${SUITES:-librdkafka franz-go}"

mkdir -p "${RESULTS_DIR}"

echo "=== Kafka client compatibility ==="
echo "broker: ${BOOTSTRAP_SERVERS}"
echo "suites: ${SUITES}"
echo

declare -A status

for suite in ${SUITES}; do
    echo "==================== ${suite} ===================="
    RESULTS_FILE="${RESULTS_DIR}/${suite}.csv" \
        "/work/compat/${suite}/run.sh"
    status[${suite}]=$?
    echo
done

echo "==================== report card ===================="
overall=0
for suite in ${SUITES}; do
    csv="${RESULTS_DIR}/${suite}.csv"
    if [[ -f "${csv}" ]]; then
        pass=$(grep -c ',PASS$' "${csv}" || true)
        fail=$(grep -c ',FAIL$' "${csv}" || true)
        total=$((pass + fail))
        printf '%-12s %s/%s passed (exit %s)\n' \
               "${suite}" "${pass}" "${total}" "${status[${suite}]}"
        if (( fail > 0 )); then
            echo "  failed:"
            grep ',FAIL$' "${csv}" | sed 's/,FAIL$//; s/^/    /'
        fi
    else
        printf '%-12s no results file -- suite did not run (exit %s)\n' \
               "${suite}" "${status[${suite}]}"
    fi
    (( status[${suite}] != 0 )) && overall=1
done

echo
if (( overall == 0 )); then
    echo "PASS: every allowlisted test passed against ${BOOTSTRAP_SERVERS}"
else
    echo "FAIL: at least one suite regressed against ${BOOTSTRAP_SERVERS}"
    echo "Compare against the memory:// baselines in compat/*/FINDINGS.md before"
    echo "attributing a failure to the storage engine -- some are known broker gaps."
fi
exit "${overall}"
