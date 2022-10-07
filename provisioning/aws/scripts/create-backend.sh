#!/usr/bin/env bash
set -Eeuo pipefail

aws cloudformation deploy --stack-name "$TERRAFORM_BACKEND_STACK_PREFIX-terraform-backend" \
  --parameter-overrides \
  BackendPrefix=$TERRAFORM_BACKEND_STACK_PREFIX \
  --template-file ./provisioning/aws/resources/terraform-backend.json \
  --no-fail-on-empty-changeset \
  --capabilities CAPABILITY_NAMED_IAM \
  --output text
