#!/usr/bin/env bash
set -euo pipefail
docker build -t data-engineer-assessment .
docker run --rm -e SEED="${SEED:-42}" -e ORDERS="${ORDERS:-1000}" data-engineer-assessment
