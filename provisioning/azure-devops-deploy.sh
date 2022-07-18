#!/usr/bin/env bash
set -Eeuo pipefail

# Checkout correct commit
if [ "$RELEASE_ARTIFACTS__KEBOOLA_KEBOOLA_AS_CODE_SOURCEBRANCH" == "master" ]; then
  COMMIT=$(echo "$BUILD_BUILDID" | cut -d'-' -f 2)
  git checkout "$COMMIT" -f
fi

COMMIT_HASH=$(git rev-parse HEAD)
export COMMIT_HASH

export RELEASE_ID="$BUILD_BUILDID-$RELEASE_RELEASENAME"

./provisioning/"$CLOUD_PROVIDER"/scripts/deploy.sh
