#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -lt 3 ]]; then
  echo "Usage: script/convert.sh <input_csv> <input_schema> <output_columnar>" >&2
  exit 2
fi

INPUT_CSV="$1"
INPUT_SCHEMA="$2"
COLUMNAR="$3"

BUILD_TYPE="${BUILD_TYPE:-Release}"
OUTPUT_FOLDER="${OUTPUT_FOLDER:-${ROOT_DIR}/build}"
BIN="${OUTPUT_FOLDER}/build/${BUILD_TYPE}/exe/csv_to_columnar"

if [[ ! -x "${BIN}" ]]; then
  echo "ERROR: csv_to_columnar not found at ${BIN}" >&2
  echo "Run script/build.sh first" >&2
  exit 1
fi

if [[ ! -f "${INPUT_CSV}" ]]; then
  echo "ERROR: input CSV not found: ${INPUT_CSV}" >&2
  exit 2
fi

if [[ ! -f "${INPUT_SCHEMA}" ]]; then
  echo "ERROR: schema file not found: ${INPUT_SCHEMA}" >&2
  exit 2
fi

mkdir -p "$(dirname "${COLUMNAR}")"

"${BIN}" --input "${INPUT_CSV}" --schema "${INPUT_SCHEMA}" --output "${COLUMNAR}"
