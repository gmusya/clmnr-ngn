#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -lt 4 ]]; then
  echo "Usage: script/run_query.sh <query_num> <columnar> <output_dir> <logs_dir>" >&2
  exit 2
fi

QUERY_NUM="$1"
COLUMNAR="$2"
OUTPUT="$3"
LOGS="$4"

BUILD_TYPE="${BUILD_TYPE:-Release}"
OUTPUT_FOLDER="${OUTPUT_FOLDER:-${ROOT_DIR}/build}"
SCHEMA="${SCHEMA:-${ROOT_DIR}/hits.schema}"
BIN="${OUTPUT_FOLDER}/build/${BUILD_TYPE}/clickbench/ngn-clickbench-run"

if [[ ! -x "${BIN}" ]]; then
  echo "ERROR: ngn-clickbench-run not found at ${BIN}" >&2
  echo "Run script/build.sh first" >&2
  exit 1
fi

if [[ ! -f "${COLUMNAR}" ]]; then
  echo "ERROR: columnar file not found: ${COLUMNAR}" >&2
  exit 2
fi

if [[ ! -f "${SCHEMA}" ]]; then
  echo "ERROR: schema file not found: ${SCHEMA}" >&2
  exit 2
fi

mkdir -p "${OUTPUT}"
mkdir -p "${LOGS}"

"${BIN}" \
  --input "${COLUMNAR}" \
  --schema "${SCHEMA}" \
  --output_dir "${OUTPUT}" \
  --queries="${QUERY_NUM}" \
  2>&1 | tee "${LOGS}/q${QUERY_NUM}.log"
