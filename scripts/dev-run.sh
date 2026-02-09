#!/usr/bin/env bash
set -euo pipefail

export APP_ID="${APP_ID:-student-app}"
export AWS_REGION="${AWS_REGION:-eu-central-1}"
: "${SQS_QUEUE_URL:?set SQS_QUEUE_URL}"
: "${VALIDATOR_URL:?set VALIDATOR_URL}"

python -m training_app.main
