#!/usr/bin/env bash
set -Eeuo pipefail

TERRAFORM_BACKEND_PLAN_POLICY_ARN=$(aws cloudformation describe-stacks \
  --stack-name "$TERRAFORM_BACKEND_STACK_PREFIX-terraform-backend" \
  --query "Stacks[0].Outputs[?OutputKey=='TerraformRemoteStatePlanPolicy'].OutputValue" \
  --output text)

aws cloudformation deploy --stack-name kbc-github-actions-role \
  --parameter-overrides \
    OIDCProviderArn=$GITHUB_OIDC_PROVIDER_ARN \
    GitHubOrganization=$GITHUB_ORGANIZATION \
    RepositoryName=$GITHUB_REPOSITORY_NAME \
    TerraformBackendPlanPolicyArn=$TERRAFORM_BACKEND_PLAN_POLICY_ARN \
  --template-file ./provisioning/aws/resources/github-action-production-role.json \
  --no-fail-on-empty-changeset \
  --capabilities CAPABILITY_NAMED_IAM \
  --output text

ADMIN_ROLE_ARN=$(aws cloudformation describe-stacks \
  --stack-name kbc-github-actions-role \
  --query "Stacks[0].Outputs[?OutputKey=='GithubActionAdminRoleArn'].OutputValue" \
  --output text)

echo "GITHUB_ADMIN_ROLE_ARN: $ADMIN_ROLE_ARN"

READ_ONLY_ROLE_ARN=$(aws cloudformation describe-stacks \
  --stack-name kbc-github-actions-role \
  --query "Stacks[0].Outputs[?OutputKey=='GithubActionReadOnlyRoleArn'].OutputValue" \
  --output text)

echo "GITHUB_READ_ONLY_ROLE_ARN: $READ_ONLY_ROLE_ARN"
