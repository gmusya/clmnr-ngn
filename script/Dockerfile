FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/commands.sh scripts/env.sh /bench/
RUN chmod +x /bench/commands.sh

WORKDIR /work

ENTRYPOINT ["/bench/commands.sh"]
