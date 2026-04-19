#!/usr/bin/env bash
set -euo pipefail

source /bench/env.sh

git clone --branch "${BRANCH}" "${REPO_URL}" repo
cd repo

source ./script/setup.sh
bash ./script/build.sh
bash ./script/convert.sh "${INPUT_CSV}" "${COLUMNAR}"

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

for QUERY_NUM in $(seq 0 42); do
  OUTPUT_CSV="${RUN_RESULTS_DIR}/query_${QUERY_NUM}.csv"
  LOG_FILE="${RUN_RESULTS_DIR}/query_${QUERY_NUM}.log"
  TIME_FILE="$(mktemp)"

  if /usr/bin/time -f "%e" -o "${TIME_FILE}" \
    bash ./script/run_query.sh "${QUERY_NUM}" "${COLUMNAR}" "${OUTPUT_CSV}" "${LOG_FILE}"; then
    QUERY_TIME_MS="$(awk 'NF { printf "%.0f", $1 * 1000 }' "${TIME_FILE}")"
    QUERY_TIME_MS="${QUERY_TIME_MS:-NA}"
  else
    QUERY_TIME_MS="NA"
  fi

  rm -f "${TIME_FILE}"
  echo "${QUERY_NUM},${QUERY_TIME_MS}" >> "${TIMES_CSV}"
done

echo "Results saved to ${RUN_RESULTS_DIR}"
