#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export BUILD_TYPE="${BUILD_TYPE:-RelWithDebInfo}"
export OUTPUT_FOLDER="${OUTPUT_FOLDER:-${ROOT_DIR}/build-profile}"
export CPPSTD="${CPPSTD:-23}"
export COMPILER="${COMPILER:-clang}"
export COMPILER_VERSION="${COMPILER_VERSION:-20}"
export WITH_PROFILING="${WITH_PROFILING:-1}"

"${ROOT_DIR}/script/build.sh"
