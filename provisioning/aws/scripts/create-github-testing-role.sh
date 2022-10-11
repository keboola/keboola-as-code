#!/usr/bin/env bash
set -Eeuo pipefail

aws cloudformation deploy --stack-name kbc-github-actions-role \
  --parameter-overrides \
    OIDCProviderArn=$GITHUB_OIDC_PROVIDER_ARN \
    GitHubOrganization=$GITHUB_ORGANIZATION \
    RepositoryName=$GITHUB_REPOSITORY_NAME\
  --template-file ./provisioning/aws/resources/github-action-testing-role.json \
  --no-fail-on-empty-changeset \
  --capabilities CAPABILITY_NAMED_IAM \
  --output text

ADMIN_ROLE_ARN=$(aws cloudformation describe-stacks \
  --stack-name kbc-github-actions-role \
  --query "Stacks[0].Outputs[?OutputKey=='GithubActionAdminRoleArn'].OutputValue" \
  --output text)

echo "GITHUB_ADMIN_ROLE_ARN: $ADMIN_ROLE_ARN"
