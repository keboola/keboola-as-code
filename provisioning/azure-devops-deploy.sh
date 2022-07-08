#!/usr/bin/env bash
set -Eeuo pipefail

COMMIT=$(echo "$BUILD_BUILDID" | cut -d'-' -f 2)
export COMMIT

# Checkout correct commit
if [ "$RELEASE_ARTIFACTS__KEBOOLA_KEBOOLA_AS_CODE_SOURCEBRANCH" == "master" ]; then
  git checkout "$COMMIT" -f
fi

export RELEASE_ID="$BUILD_BUILDID-$RELEASE_RELEASENAME"

./provisioning/"$CLOUD_PROVIDER"/scripts/deploy.sh
