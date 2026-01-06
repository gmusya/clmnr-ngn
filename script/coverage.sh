#!/bin/bash

# This script must be called from the root folder of the repository

mkdir -p build-coverage

cmake -S . -B build-coverage \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER=/usr/bin/clang-20 \
  -DCMAKE_CXX_COMPILER=/usr/bin/clang++-20 \
  -DCMAKE_C_FLAGS="-O0 -g -fprofile-instr-generate -fcoverage-mapping" \
  -DCMAKE_CXX_FLAGS="-O0 -g -fprofile-instr-generate -fcoverage-mapping" \
  -DCMAKE_EXE_LINKER_FLAGS="-fprofile-instr-generate"

cmake --build build-coverage -j $(nproc)

LLVM_PROFILE_FILE="build-coverage/coverage.profraw" build-coverage/src/core/ngn-csv-test

llvm-profdata-20 merge -sparse build-coverage/coverage.profraw -o build-coverage/coverage.profdata

llvm-cov-20 show \
  build-coverage/src/core/ngn-csv-test \
  -instr-profile=build-coverage/coverage.profdata \
  -format=html -output-dir=build-coverage/coverage-html \
  -ignore-filename-regex='(/_deps/|test)'
