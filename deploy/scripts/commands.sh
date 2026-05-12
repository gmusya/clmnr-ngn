#!/usr/bin/env bash
set -Eeuo pipefail

log() {
  printf '==> %s\n' "$*" >&2
}

CURRENT_STEP=""

on_error() {
  local status=$?
  if [[ -n "${CURRENT_STEP}" ]]; then
    log "FAILED: ${CURRENT_STEP} (exit ${status})"
  else
    log "FAILED: unexpected error (exit ${status})"
  fi
}

trap on_error ERR

run_step() {
  local name="$1"
  shift

  CURRENT_STEP="${name}"
  log "START: ${name}"
  set +e
  "$@"
  local status=$?
  set -e

  if [[ ${status} -ne 0 ]]; then
    log "FAILED: ${name} (exit ${status})"
    exit "${status}"
  fi

  log "DONE: ${name}"
  CURRENT_STEP=""
}

source /bench/env.sh

log "Benchmark configuration: repo=${REPO_URL} branch=${BRANCH}"
log "Data paths: input=${INPUT_CSV} columnar=${COLUMNAR} results=${RESULTS}"

run_step "clone repository" git clone --branch "${BRANCH}" "${REPO_URL}" repo
cd repo

run_step "install dependencies" source ./script/setup.sh
run_step "build project" bash ./script/build.sh
run_step "convert input CSV to columnar" bash ./script/convert.sh "${INPUT_CSV}" "${COLUMNAR}"

if [[ "${REPO_URL}" =~ github\.com[:/]([^/]+)/ ]]; then
  GITHUB_USER_NAME="${BASH_REMATCH[1]}"
else
  echo "ERROR: unable to extract GitHub user from REPO_URL='${REPO_URL}'" >&2
  exit 2
fi

COMMIT_HASH="$(git rev-parse --short HEAD)"

RESULTS_ROOT="${RESULTS}"
RUN_RESULTS_DIR="${RESULTS_ROOT}/${GITHUB_USER_NAME}/${COMMIT_HASH}"
TIMES_CSV="${RUN_RESULTS_DIR}/query_times.csv"

mkdir -p "${RUN_RESULTS_DIR}"
echo "query,time_ms" > "${TIMES_CSV}"
log "Writing benchmark results to ${RUN_RESULTS_DIR}"

for QUERY_NUM in $(seq 0 42); do
  QUERY_NUM_PADDED="$(printf "%02d" "${QUERY_NUM}")"
  OUTPUT_CSV="${RUN_RESULTS_DIR}/query_${QUERY_NUM_PADDED}.csv"
  LOG_FILE="${RUN_RESULTS_DIR}/query_${QUERY_NUM_PADDED}.log"
  TIME_FILE="$(mktemp)"

  log "START: query ${QUERY_NUM_PADDED}"
  if /usr/bin/time -f "%e" -o "${TIME_FILE}" \
    bash ./script/run_query.sh "${QUERY_NUM}" "${COLUMNAR}" "${OUTPUT_CSV}" "${LOG_FILE}"; then
    QUERY_TIME_MS="$(awk 'NF { printf "%.0f", $1 * 1000 }' "${TIME_FILE}")"
    QUERY_TIME_MS="${QUERY_TIME_MS:-NA}"
    log "DONE: query ${QUERY_NUM_PADDED} (${QUERY_TIME_MS} ms)"
  else
    QUERY_TIME_MS="NA"
    log "FAILED: query ${QUERY_NUM_PADDED} (recording NA and continuing)"
  fi

  rm -f "${TIME_FILE}"
  echo "${QUERY_NUM},${QUERY_TIME_MS}" >> "${TIMES_CSV}"
done

echo "Results saved to ${RUN_RESULTS_DIR}"

