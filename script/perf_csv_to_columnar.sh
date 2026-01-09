#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  script/perf_csv_to_columnar.sh [options] [-- <extra csv_to_columnar flags>]

Options:
  --input PATH         Input CSV file (default: example/simple.csv)
  --schema PATH        Schema file (default: example/schema.csv)
  --output PATH        Output columnar file (default: <perf_dir>/out.clmnr)
  --perf-dir PATH      Output directory for perf/flamegraph artifacts (default: <OUTPUT_FOLDER>/perf)
  --output-folder PATH Build output folder used by script/build.sh (default: build-profile)
  --build-type NAME    CMake build type (default: RelWithDebInfo)
  --bin PATH           Path to csv_to_columnar binary (overrides auto-detect)
  --freq N             perf sampling frequency (default: 99)
  --callgraph MODE     perf callgraph mode: fp or dwarf (default: fp)
  --flamegraph-dir PATH
                      Where FlameGraph repo lives (default: <OUTPUT_FOLDER>/FlameGraph)

Artifacts:
  - perf data:   <perf_dir>/perf-<ts>.data      (use: perf report -i ...)
  - flamegraph:  <perf_dir>/flamegraph-<ts>.svg

Examples:
  script/build_profile.sh
  script/perf_csv_to_columnar.sh
  script/perf_csv_to_columnar.sh --freq 199 --callgraph dwarf -- --row_group_size=131072
EOF
}

INPUT="${INPUT:-${ROOT_DIR}/example/simple.csv}"
SCHEMA="${SCHEMA:-${ROOT_DIR}/example/schema.csv}"

OUTPUT_FOLDER="${OUTPUT_FOLDER:-${ROOT_DIR}/build-profile}"
BUILD_TYPE="${BUILD_TYPE:-RelWithDebInfo}"

PERF_FREQ="${PERF_FREQ:-99}"
PERF_CALLGRAPH="${PERF_CALLGRAPH:-fp}"

PERF_DIR=""
OUT_CLMNR=""
BIN="${BIN:-}"
FLAMEGRAPH_DIR=""

EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --input)
      INPUT="$2"; shift 2 ;;
    --schema)
      SCHEMA="$2"; shift 2 ;;
    --output)
      OUT_CLMNR="$2"; shift 2 ;;
    --perf-dir)
      PERF_DIR="$2"; shift 2 ;;
    --output-folder)
      OUTPUT_FOLDER="$2"; shift 2 ;;
    --build-type)
      BUILD_TYPE="$2"; shift 2 ;;
    --bin)
      BIN="$2"; shift 2 ;;
    --freq)
      PERF_FREQ="$2"; shift 2 ;;
    --callgraph)
      PERF_CALLGRAPH="$2"; shift 2 ;;
    --flamegraph-dir)
      FLAMEGRAPH_DIR="$2"; shift 2 ;;
    --)
      shift
      EXTRA_ARGS+=("$@")
      break
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${PERF_DIR}" ]]; then
  PERF_DIR="${OUTPUT_FOLDER}/perf"
fi
mkdir -p "${PERF_DIR}"

if [[ -z "${OUT_CLMNR}" ]]; then
  OUT_CLMNR="${PERF_DIR}/out.clmnr"
fi

if [[ -z "${FLAMEGRAPH_DIR}" ]]; then
  FLAMEGRAPH_DIR="${OUTPUT_FOLDER}/FlameGraph"
fi

if ! command -v perf >/dev/null 2>&1; then
  echo "ERROR: 'perf' not found in PATH." >&2
  exit 127
fi
if ! command -v perl >/dev/null 2>&1; then
  echo "ERROR: 'perl' not found in PATH (required for FlameGraph scripts)." >&2
  exit 127
fi

if [[ ! -f "${INPUT}" ]]; then
  echo "ERROR: input CSV not found: ${INPUT}" >&2
  exit 2
fi
if [[ ! -f "${SCHEMA}" ]]; then
  echo "ERROR: schema not found: ${SCHEMA}" >&2
  exit 2
fi

if [[ -z "${BIN}" ]]; then
  CMAKE_BUILD_DIR="${OUTPUT_FOLDER}/build/${BUILD_TYPE}"
  BIN="${CMAKE_BUILD_DIR}/exe/csv_to_columnar"
fi
if [[ ! -x "${BIN}" ]]; then
  echo "ERROR: csv_to_columnar binary not found/executable: ${BIN}" >&2
  echo "Hint: build it via: ${ROOT_DIR}/script/build_profile.sh" >&2
  exit 2
fi

if [[ ! -d "${FLAMEGRAPH_DIR}" ]]; then
  if ! command -v git >/dev/null 2>&1; then
    echo "ERROR: FlameGraph repo not found and 'git' is not available to clone it." >&2
    echo "Set --flamegraph-dir to an existing FlameGraph checkout." >&2
    exit 127
  fi
  git clone --depth 1 https://github.com/brendangregg/FlameGraph.git "${FLAMEGRAPH_DIR}"
fi

STACKCOLLAPSE="${FLAMEGRAPH_DIR}/stackcollapse-perf.pl"
FLAMEGRAPH="${FLAMEGRAPH_DIR}/flamegraph.pl"
if [[ ! -f "${STACKCOLLAPSE}" || ! -f "${FLAMEGRAPH}" ]]; then
  echo "ERROR: FlameGraph scripts not found in: ${FLAMEGRAPH_DIR}" >&2
  exit 3
fi

TS="$(date +%Y%m%d-%H%M%S)"
PERF_DATA="${PERF_DIR}/perf-${TS}.data"
FOLDED="${PERF_DIR}/stacks-${TS}.folded"
SVG="${PERF_DIR}/flamegraph-${TS}.svg"

echo "Running perf record..."
echo "  bin:    ${BIN}"
echo "  input:  ${INPUT}"
echo "  schema: ${SCHEMA}"
echo "  output: ${OUT_CLMNR}"
echo "  perf:   ${PERF_DATA}"

perf record \
  -F "${PERF_FREQ}" \
  -g --call-graph "${PERF_CALLGRAPH}" \
  -o "${PERF_DATA}" \
  -- \
  "${BIN}" \
  --input "${INPUT}" \
  --schema "${SCHEMA}" \
  --output "${OUT_CLMNR}" \
  "${EXTRA_ARGS[@]}"

echo "Generating FlameGraph..."
perf script -i "${PERF_DATA}" | "${STACKCOLLAPSE}" > "${FOLDED}"
"${FLAMEGRAPH}" --title "csv_to_columnar (${TS})" "${FOLDED}" > "${SVG}"

echo ""
echo "Done."
echo "Perf data:   ${PERF_DATA}"
echo "FlameGraph:  ${SVG}"
echo ""
echo "Next:"
echo "  perf report -i ${PERF_DATA}"


