#!/usr/bin/env bash
# Build a fresh helper, run all shmipc tests with business logging, and copy
# the produced business log to a stable path for inspection.
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p testlogs
STAMP="$(date +%Y%m%d-%H%M%S)"
LOG="testlogs/run-${STAMP}.log"
export SHMIPC_TEST_LOG="${PWD}/${LOG}"

echo "[run-tests] building test helper"
go build -o /tmp/shmipc-testhelper ./tests/helper

echo "[run-tests] running shmipc tests (log: ${LOG})"
# -v output is mirrored into the business logger by the tests themselves;
# keep full stdout here too for CI.
go test ./shmipc -count=1 -v -timeout 180s "$@" 2>&1 | tee "/tmp/shmipc-${STAMP}.out"

LATEST="$(ls -t testlogs/run-*.log 2>/dev/null | head -1 || true)"
if [[ -n "${LATEST}" ]]; then
  echo "[run-tests] business summary:"
  grep -E -- '---- business:|\[case\]|\[PASS\]' "${LATEST}" || true
fi
