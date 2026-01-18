#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  script/perf_clickbench.sh [options] [-- <extra clickbench flags>]

Options:
  --input PATH         Input columnar file (.clmnr)
  --schema PATH        Schema file (.schema)
  --output-dir PATH    Output directory for query results (default: <perf_dir>/output)
  --perf-dir PATH      Output directory for perf/flamegraph artifacts (default: <OUTPUT_FOLDER>/perf)
  --output-folder PATH Build output folder used by script/build.sh (default: build-profile)
  --build-type NAME    CMake build type (default: RelWithDebInfo)
  --bin PATH           Path to ngn-clickbench-run binary (overrides auto-detect)
  --freq N             perf sampling frequency (default: 99)
  --callgraph MODE     perf callgraph mode: fp or dwarf (default: fp)
  --flamegraph-dir PATH
                      Where FlameGraph repo lives (default: <OUTPUT_FOLDER>/FlameGraph)

Artifacts:
  - perf data:   <perf_dir>/perf-clickbench-<ts>.data      (use: perf report -i ...)
  - flamegraph:  <perf_dir>/flamegraph-clickbench-<ts>.svg

Examples:
  script/build_profile.sh
  script/perf_clickbench.sh --input data/hits.clmnr --schema hits.schema
  script/perf_clickbench.sh --input data/hits.clmnr --schema hits.schema -- --queries=0,1,2
  script/perf_clickbench.sh --input data/hits.clmnr --schema hits.schema --freq 199 -- --from=5 --to=10
EOF
}

INPUT=""
SCHEMA=""

OUTPUT_FOLDER="${OUTPUT_FOLDER:-${ROOT_DIR}/build-profile}"
BUILD_TYPE="${BUILD_TYPE:-RelWithDebInfo}"

PERF_FREQ="${PERF_FREQ:-99}"
PERF_CALLGRAPH="${PERF_CALLGRAPH:-fp}"

PERF_DIR=""
OUTPUT_DIR=""
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
    --output-dir)
      OUTPUT_DIR="$2"; shift 2 ;;
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

if [[ -z "${INPUT}" ]]; then
  echo "ERROR: --input is required" >&2
  usage >&2
  exit 2
fi

if [[ -z "${SCHEMA}" ]]; then
  echo "ERROR: --schema is required" >&2
  usage >&2
  exit 2
fi

if [[ -z "${PERF_DIR}" ]]; then
  PERF_DIR="${OUTPUT_FOLDER}/perf"
fi
mkdir -p "${PERF_DIR}"

if [[ -z "${OUTPUT_DIR}" ]]; then
  OUTPUT_DIR="${PERF_DIR}/clickbench-output"
fi
mkdir -p "${OUTPUT_DIR}"

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
  echo "ERROR: input file not found: ${INPUT}" >&2
  exit 2
fi
if [[ ! -f "${SCHEMA}" ]]; then
  echo "ERROR: schema not found: ${SCHEMA}" >&2
  exit 2
fi

if [[ -z "${BIN}" ]]; then
  CMAKE_BUILD_DIR="${OUTPUT_FOLDER}/build/${BUILD_TYPE}"
  BIN="${CMAKE_BUILD_DIR}/clickbench/ngn-clickbench-run"
fi
if [[ ! -x "${BIN}" ]]; then
  echo "ERROR: ngn-clickbench-run binary not found/executable: ${BIN}" >&2
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
PERF_DATA="${PERF_DIR}/perf-clickbench-${TS}.data"
FOLDED="${PERF_DIR}/stacks-clickbench-${TS}.folded"
SVG="${PERF_DIR}/flamegraph-clickbench-${TS}.svg"

echo "Running perf record..."
echo "  bin:        ${BIN}"
echo "  input:      ${INPUT}"
echo "  schema:     ${SCHEMA}"
echo "  output_dir: ${OUTPUT_DIR}"
echo "  perf:       ${PERF_DATA}"
if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  echo "  extra args: ${EXTRA_ARGS[*]}"
fi

perf record \
  -F "${PERF_FREQ}" \
  -g --call-graph "${PERF_CALLGRAPH}" \
  -o "${PERF_DATA}" \
  -- \
  "${BIN}" \
  --input "${INPUT}" \
  --schema "${SCHEMA}" \
  --output_dir "${OUTPUT_DIR}" \
  "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}"

echo "Generating FlameGraph..."
perf script -i "${PERF_DATA}" | "${STACKCOLLAPSE}" > "${FOLDED}"
"${FLAMEGRAPH}" --title "clickbench (${TS})" "${FOLDED}" > "${SVG}"

echo ""
echo "Done."
echo "Perf data:   ${PERF_DATA}"
echo "FlameGraph:  ${SVG}"
echo ""
echo "Next:"
echo "  perf report -i ${PERF_DATA}"
echo "  xdg-open ${SVG}  # or open in browser"
