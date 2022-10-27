#!/usr/bin/env bash
set -Eeuo pipefail

COMMIT_HASH=$(git rev-parse HEAD)
export COMMIT_HASH

export RELEASE_ID="$BUILD_BUILDID-$RELEASE_RELEASENAME"

./provisioning/"$CLOUD_PROVIDER".sh
