#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat >&2 <<'EOF'
Usage:
  deploy/scripts/run_benchmark.sh [--dataset small|big] <REPO_URL> <BRANCH>

Options:
  --dataset small|big  Dataset to benchmark (default: small)
EOF
}

DATASET="small"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --dataset requires a value" >&2
        usage
        exit 2
      fi
      DATASET="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "ERROR: unknown option: $1" >&2
      usage
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

if [[ $# -lt 2 ]]; then
  usage
  exit 2
fi

REPO_URL="$1"
BRANCH="$2"

if [[ "${DATASET}" != "small" && "${DATASET}" != "big" ]]; then
  echo "ERROR: --dataset must be 'small' or 'big', got '${DATASET}'" >&2
  exit 2
fi

SSH_USER="${SSH_USER:-ubuntu}"
VM_IP=$(terraform -chdir="${DEPLOY_DIR}" output -raw external_ip)
SSH_TARGET="${SSH_USER}@${VM_IP}"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

echo "==> Running benchmark on ${SSH_TARGET}"
echo "    Repo:   ${REPO_URL}"
echo "    Branch: ${BRANCH}"
echo "    Dataset: ${DATASET}"

BENCHMARK_OUTPUT="$(
  ssh ${SSH_OPTS} "${SSH_TARGET}" \
    "docker run --rm \
      -e REPO_URL='${REPO_URL}' \
      -e BRANCH='${BRANCH}' \
      -e DATASET='${DATASET}' \
      -v ~/data:/data \
      bench" 2>&1 | tee /dev/stderr
)"

echo "==> Benchmark command finished. Fetching results..."

RESULTS_DIR="${DEPLOY_DIR}/../results"
mkdir -p "${RESULTS_DIR}"

RUN_RESULTS_DIR="$(
  printf "%s\n" "${BENCHMARK_OUTPUT}" \
    | sed -n 's/^Results saved to //p' \
    | tail -n 1
)"

if [[ -z "${RUN_RESULTS_DIR}" ]]; then
  echo "ERROR: benchmark output did not contain a 'Results saved to ...' line" >&2
  exit 1
fi

case "${RUN_RESULTS_DIR}" in
  /data/results/*)
    RESULT_PATH="${RUN_RESULTS_DIR#/data/results/}"
    ;;
  *)
    echo "ERROR: unexpected benchmark results directory: ${RUN_RESULTS_DIR}" >&2
    exit 1
    ;;
esac

mkdir -p "${RESULTS_DIR}/$(dirname "${RESULT_PATH}")"
scp ${SSH_OPTS} -r "${SSH_TARGET}:~/data/results/${RESULT_PATH}" "${RESULTS_DIR}/$(dirname "${RESULT_PATH}")/"

echo "==> Results saved to ${RESULTS_DIR}"
