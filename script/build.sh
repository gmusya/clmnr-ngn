#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Basic sanity checks (keep it simple and actionable)
if ! command -v conan >/dev/null 2>&1; then
  echo "ERROR: 'conan' not found in PATH." >&2
  echo "Install Conan 2 (example): pipx install conan" >&2
  echo "Or: python3 -m pip install --user conan" >&2
  exit 127
fi
if ! command -v cmake >/dev/null 2>&1; then
  echo "ERROR: 'cmake' not found in PATH." >&2
  exit 127
fi

BUILD_TYPE="${BUILD_TYPE:-Release}"
OUTPUT_FOLDER="${OUTPUT_FOLDER:-${ROOT_DIR}/build}"
CPPSTD="${CPPSTD:-23}"
COMPILER="${COMPILER:-clang}"
COMPILER_VERSION="${COMPILER_VERSION:-20}"
WITH_COVERAGE="${WITH_COVERAGE:-0}"

if [[ "${COMPILER}" != "clang" && "${WITH_COVERAGE}" == "1" ]]; then
  echo "ERROR: Coverage mode is only supported with clang (set COMPILER=clang)." >&2
  exit 2
fi

mkdir -p "${OUTPUT_FOLDER}"

CONAN_EXTRA_ARGS=()
if [[ "${COMPILER}" == "clang" ]]; then
  CONAN_EXTRA_ARGS+=(-s compiler=clang -s "compiler.version=${COMPILER_VERSION}")
  export CC="${CC:-clang-${COMPILER_VERSION}}"
  export CXX="${CXX:-clang++-${COMPILER_VERSION}}"
elif [[ "${COMPILER}" == "gcc" ]]; then
  CONAN_EXTRA_ARGS+=(-s compiler=gcc)
fi

if [[ "${WITH_COVERAGE}" == "1" ]]; then
  CONAN_EXTRA_ARGS+=(-o with_coverage=True)
fi

conan install "${ROOT_DIR}" \
  -of "${OUTPUT_FOLDER}" \
  --build=missing \
  -s "build_type=${BUILD_TYPE}" \
  -s "compiler.cppstd=${CPPSTD}" \
  "${CONAN_EXTRA_ARGS[@]}"

CMAKE_BUILD_DIR="${OUTPUT_FOLDER}/build/${BUILD_TYPE}"
TOOLCHAIN_FILE="${CMAKE_BUILD_DIR}/generators/conan_toolchain.cmake"

if [[ ! -f "${TOOLCHAIN_FILE}" ]]; then
  echo "ERROR: Conan toolchain not found: ${TOOLCHAIN_FILE}" >&2
  echo "Did 'conan install' succeed? Output folder: ${OUTPUT_FOLDER}" >&2
  exit 3
fi

cmake -S "${ROOT_DIR}" -B "${CMAKE_BUILD_DIR}" \
  -DCMAKE_TOOLCHAIN_FILE="${TOOLCHAIN_FILE}" \
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"

if [[ -f "${CMAKE_BUILD_DIR}/compile_commands.json" ]]; then
  mkdir -p "${OUTPUT_FOLDER}"
  CC_SRC="${CMAKE_BUILD_DIR}/compile_commands.json"
  CC_DST_OUTPUT="${OUTPUT_FOLDER}/compile_commands.json"
  CC_DST_ROOT="${ROOT_DIR}/build/compile_commands.json"

  if [[ "${CC_SRC}" != "${CC_DST_OUTPUT}" ]]; then
    cp -f "${CC_SRC}" "${CC_DST_OUTPUT}"
  fi
  if [[ "${CC_SRC}" != "${CC_DST_ROOT}" ]]; then
    mkdir -p "${ROOT_DIR}/build"
    cp -f "${CC_SRC}" "${CC_DST_ROOT}"
  fi
fi

cmake --build "${CMAKE_BUILD_DIR}" -j "$(nproc)"


