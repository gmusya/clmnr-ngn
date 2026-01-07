#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

LLVM_VERSION="${LLVM_VERSION:-20}"
OUTPUT_FOLDER="${OUTPUT_FOLDER:-${ROOT_DIR}/build-coverage}"

if ! command -v "llvm-profdata-${LLVM_VERSION}" >/dev/null 2>&1; then
  echo "ERROR: llvm-profdata-${LLVM_VERSION} not found in PATH." >&2
  exit 127
fi
if ! command -v "llvm-cov-${LLVM_VERSION}" >/dev/null 2>&1; then
  echo "ERROR: llvm-cov-${LLVM_VERSION} not found in PATH." >&2
  exit 127
fi

export BUILD_TYPE=Debug
export OUTPUT_FOLDER
export COMPILER=clang
export COMPILER_VERSION="${LLVM_VERSION}"
export CPPSTD=23
export WITH_COVERAGE=1

"${ROOT_DIR}/script/build.sh"

CMAKE_BUILD_DIR="${OUTPUT_FOLDER}/build/Debug"
TEST_BIN="${CMAKE_BUILD_DIR}/src/core/ngn-csv-test"

if [[ ! -x "${TEST_BIN}" ]]; then
  echo "Test binary not found/executable: ${TEST_BIN}" >&2
  exit 2
fi

PROFRAW="${OUTPUT_FOLDER}/coverage.profraw"
PROFDATA="${OUTPUT_FOLDER}/coverage.profdata"
HTML_DIR="${OUTPUT_FOLDER}/coverage-html"

rm -f "${PROFRAW}" "${PROFDATA}"
rm -rf "${HTML_DIR}"
mkdir -p "${HTML_DIR}"

LLVM_PROFILE_FILE="${PROFRAW}" "${TEST_BIN}"

"llvm-profdata-${LLVM_VERSION}" merge -sparse "${PROFRAW}" -o "${PROFDATA}"

"llvm-cov-${LLVM_VERSION}" show \
  "${TEST_BIN}" \
  -instr-profile="${PROFDATA}" \
  -format=html -output-dir="${HTML_DIR}" \
  -ignore-filename-regex='(/_deps/|/build/|/test|/ut/)'

echo "Coverage HTML: ${HTML_DIR}/index.html"
