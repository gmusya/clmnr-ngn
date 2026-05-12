#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT_DIR="$(cd "${DEPLOY_DIR}/.." && pwd)"
DOCKERFILE_PATH="${ROOT_DIR}/script/Dockerfile"

if [[ $# -lt 1 ]]; then
  echo "Usage: deploy/scripts/init_vm.sh <path_to_hits.csv>" >&2
  exit 2
fi

HITS_CSV="$1"
if [[ ! -f "${HITS_CSV}" ]]; then
  echo "ERROR: file not found: ${HITS_CSV}" >&2
  exit 2
fi

if [[ ! -f "${DOCKERFILE_PATH}" ]]; then
  echo "ERROR: Dockerfile not found: ${DOCKERFILE_PATH}" >&2
  exit 2
fi

SSH_USER="${SSH_USER:-ubuntu}"
VM_IP=$(terraform -chdir="${DEPLOY_DIR}" output -raw external_ip)
SSH_TARGET="${SSH_USER}@${VM_IP}"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10"

echo "==> VM: ${SSH_TARGET}"

echo "==> Installing Docker on VM..."
ssh ${SSH_OPTS} "${SSH_TARGET}" bash -s <<'REMOTE'
  sudo apt-get update
  sudo apt-get install -y docker.io
  sudo usermod -aG docker "$USER"
REMOTE

echo "==> Uploading hits.csv to VM..."
ssh ${SSH_OPTS} "${SSH_TARGET}" "mkdir -p ~/data ~/bench/scripts"
scp ${SSH_OPTS} "${HITS_CSV}" "${SSH_TARGET}:~/data/hits.csv"

echo "==> Uploading Dockerfile and scripts to VM..."
scp ${SSH_OPTS} "${DOCKERFILE_PATH}" "${SSH_TARGET}:~/bench/Dockerfile"
scp ${SSH_OPTS} "${SCRIPT_DIR}/commands.sh" "${SSH_TARGET}:~/bench/scripts/commands.sh"
scp ${SSH_OPTS} "${SCRIPT_DIR}/env.sh" "${SSH_TARGET}:~/bench/scripts/env.sh"

echo "==> Building Docker image on VM..."
ssh ${SSH_OPTS} "${SSH_TARGET}" "docker build -t bench ~/bench/"

echo "==> Done. VM is ready."
echo "    Run benchmarks with: deploy/scripts/run_benchmark.sh <REPO_URL> <BRANCH>"
