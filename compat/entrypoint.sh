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

# No apostrophe in this message: inside ${var:?word} the word still undergoes
# quote processing even within double quotes, so a lone ' opens a single-quoted
# string that never closes and bash fails the whole script at parse time with
# "unexpected EOF while looking for matching `''" pointing at a line 38 lines
# further down.
: "${BOOTSTRAP_SERVERS:?set BOOTSTRAP_SERVERS to the broker host:port}"
export BOOTSTRAP_SERVERS

RESULTS_DIR="${RESULTS_DIR:-/work/results}"
SUITES="${SUITES:-librdkafka franz-go}"

# Tests that also fail against memory:// and are therefore broker gaps rather
# than storage-engine regressions (see compat/librdkafka/FINDINGS.md). Without
# this the gate can never pass, so a real regression would be indistinguishable
# from the standing failure everyone has learned to ignore.
#
# Space-separated, per suite. Override KNOWN_GAPS_<suite> to re-check one.
KNOWN_GAPS_librdkafka="${KNOWN_GAPS_librdkafka:-0060}"
KNOWN_GAPS_franz_go="${KNOWN_GAPS_franz_go:-}"

known_gaps_for() {
    # franz-go -> franz_go, because a hyphen cannot appear in a variable name.
    local var="KNOWN_GAPS_${1//-/_}"
    printf '%s' "${!var:-}"
}

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
    if [[ ! -f "${csv}" ]]; then
        printf '%-12s no results file -- suite did not run (exit %s)\n' \
               "${suite}" "${status[${suite}]}"
        overall=1
        continue
    fi

    pass=$(grep -c ',PASS$' "${csv}" || true)
    fail=$(grep -c ',FAIL$' "${csv}" || true)
    total=$((pass + fail))

    gaps=" $(known_gaps_for "${suite}") "
    regressions=()
    while read -r t; do
        [[ -z "${t}" ]] && continue
        [[ "${gaps}" == *" ${t} "* ]] || regressions+=("${t}")
    done < <(grep ',FAIL$' "${csv}" | sed 's/,FAIL$//')

    printf '%-12s %s/%s passed' "${suite}" "${pass}" "${total}"
    if (( ${#regressions[@]} )); then
        printf '  -- %s REGRESSION(S)\n' "${#regressions[@]}"
        printf '     %s\n' "${regressions[@]}"
        overall=1
    elif (( fail > 0 )); then
        printf '  -- %s known gap(s), no regression\n' "${fail}"
        grep ',FAIL$' "${csv}" | sed 's/,FAIL$//; s/^/     /'
    else
        printf '  -- clean\n'
    fi
done

echo
if (( overall == 0 )); then
    echo "PASS: no client-visible protocol regression against ${BOOTSTRAP_SERVERS}"
    echo "(Known gaps listed above fail on memory:// too, so they are broker"
    echo " limitations rather than storage-engine faults.)"
else
    echo "FAIL: regression against ${BOOTSTRAP_SERVERS}"
    echo "The tests above are NOT known gaps -- they pass on memory://, so the"
    echo "storage engine is the first thing to suspect."
fi
exit "${overall}"
