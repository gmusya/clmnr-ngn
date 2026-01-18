#!/bin/bash
# Run full benchmark comparison: columnar-engine vs DuckDB (single-threaded)
#
# Required environment variables:
#   COLUMNAR_INPUT   Path to hits.clmnr file
#   DUCKDB_PARQUET   Path to hits.parquet file
#
# Optional environment variables:
#   COLUMNAR_SCHEMA  Path to schema file (default: hits.schema in project root)
#   BUILD_TYPE       CMake build type (default: Release)
#
# Usage:
#   COLUMNAR_INPUT=/path/to/hits.clmnr DUCKDB_PARQUET=/path/to/hits.parquet ./bench_compare.sh [queries]
#
# Arguments:
#   queries: all | 0-10 | 0,1,2,3  (default: all)
#
# Example:
#   export COLUMNAR_INPUT=/data/hits.clmnr
#   export DUCKDB_PARQUET=/data/hits.parquet
#   ./script/bench_compare.sh           # All 43 queries
#   ./script/bench_compare.sh 0-10      # Queries Q0-Q10
#   ./script/bench_compare.sh 0,1,5,10  # Specific queries

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Check required environment variables
if [ -z "$COLUMNAR_INPUT" ]; then
    echo "ERROR: COLUMNAR_INPUT environment variable is required"
    echo "Example: COLUMNAR_INPUT=/path/to/hits.clmnr DUCKDB_PARQUET=/path/to/hits.parquet $0"
    exit 1
fi

if [ -z "$DUCKDB_PARQUET" ]; then
    echo "ERROR: DUCKDB_PARQUET environment variable is required"
    echo "Example: COLUMNAR_INPUT=/path/to/hits.clmnr DUCKDB_PARQUET=/path/to/hits.parquet $0"
    exit 1
fi

# Optional configuration with sensible defaults
COLUMNAR_SCHEMA="${COLUMNAR_SCHEMA:-$PROJECT_DIR/hits.schema}"
BUILD_TYPE="${BUILD_TYPE:-Release}"

QUERY_SPEC="${1:-all}"
OUTPUT_DIR="$PROJECT_DIR/benchmark-results"
mkdir -p "$OUTPUT_DIR"

COLUMNAR_BIN="$PROJECT_DIR/build/build/$BUILD_TYPE/clickbench/ngn-clickbench-run"

# Check prerequisites
if [ ! -x "$COLUMNAR_BIN" ]; then
    echo "ERROR: columnar-engine binary not found: $COLUMNAR_BIN"
    echo "Build it with: ./script/build.sh $BUILD_TYPE"
    exit 1
fi

if [ ! -f "$COLUMNAR_INPUT" ]; then
    echo "ERROR: columnar input file not found: $COLUMNAR_INPUT"
    exit 1
fi

if [ ! -f "$COLUMNAR_SCHEMA" ]; then
    echo "ERROR: schema file not found: $COLUMNAR_SCHEMA"
    exit 1
fi

if [ ! -f "$DUCKDB_PARQUET" ]; then
    echo "ERROR: DuckDB parquet file not found: $DUCKDB_PARQUET"
    exit 1
fi

if ! command -v duckdb &> /dev/null; then
    echo "ERROR: duckdb not found in PATH"
    exit 1
fi

# Convert query spec to columnar-engine format
get_columnar_args() {
    local spec="$1"
    if [[ "$spec" == "all" ]]; then
        echo ""
    elif [[ "$spec" =~ ^[0-9]+-[0-9]+$ ]]; then
        local start="${spec%-*}"
        local end="${spec#*-}"
        echo "--from=$start --to=$end"
    else
        echo "--queries=$spec"
    fi
}

COLUMNAR_ARGS=$(get_columnar_args "$QUERY_SPEC")

echo "========================================"
echo "BENCHMARK COMPARISON"
echo "========================================"
echo "Queries: $QUERY_SPEC"
echo "columnar-engine: $COLUMNAR_BIN"
echo "columnar input:  $COLUMNAR_INPUT"
echo "DuckDB parquet:  $DUCKDB_PARQUET"
echo "========================================"
echo ""

# Run columnar-engine benchmark
echo ">>> Running columnar-engine..."
COLUMNAR_LOG="$OUTPUT_DIR/columnar.log"
"$COLUMNAR_BIN" \
    --input "$COLUMNAR_INPUT" \
    --schema "$COLUMNAR_SCHEMA" \
    --output_dir "$OUTPUT_DIR/columnar-output" \
    $COLUMNAR_ARGS 2>&1 | tee "$COLUMNAR_LOG"

echo ""
echo ">>> Running DuckDB (single-threaded)..."

# Run DuckDB benchmark
DUCKDB_LOG="$OUTPUT_DIR/duckdb.log"
"$SCRIPT_DIR/bench_duckdb.sh" "$DUCKDB_PARQUET" "$OUTPUT_DIR/duckdb-output" "$QUERY_SPEC" 2>&1 | tee "$DUCKDB_LOG"

echo ""
echo ">>> Comparing results..."
echo ""

# Run comparison
python3 "$SCRIPT_DIR/compare_bench.py" "$COLUMNAR_LOG" "$OUTPUT_DIR/duckdb-output/duckdb_results.txt"

echo ""
echo "Logs saved to: $OUTPUT_DIR/"
