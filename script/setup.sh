#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends \
    wget gnupg ca-certificates software-properties-common \
    git build-essential python3 python3-pip python3-venv pipx

wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key \
    | gpg --dearmor -o /usr/share/keyrings/llvm.gpg
echo "deb [signed-by=/usr/share/keyrings/llvm.gpg] http://apt.llvm.org/noble/ llvm-toolchain-noble-20 main" \
    > /etc/apt/sources.list.d/llvm-20.list

wget -qO- https://apt.kitware.com/keys/kitware-archive-latest.asc \
    | gpg --dearmor -o /usr/share/keyrings/kitware.gpg
echo "deb [signed-by=/usr/share/keyrings/kitware.gpg] https://apt.kitware.com/ubuntu/ noble main" \
    > /etc/apt/sources.list.d/kitware.list

apt-get update
apt-get install -y --no-install-recommends clang-20 lld-20 cmake

rm -rf /var/lib/apt/lists/*

export CC=clang-20
export CXX=clang++-20
export PATH="/root/.local/bin:${PATH}"

pipx install conan
conan profile detect --force
