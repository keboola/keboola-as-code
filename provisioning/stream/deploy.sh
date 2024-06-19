#!/usr/bin/env bash
set -Eeuo pipefail

# Required ENVs
: ${CLOUD_PROVIDER?"Missing CLOUD_PROVIDER"}
: ${BUILD_BUILDID?"Missing BUILD_BUILDID"}
: ${RELEASE_RELEASENAME?"Missing RELEASE_RELEASENAME"}


# Get commit hash
COMMIT_HASH=$(git rev-parse HEAD)
export COMMIT_HASH

# Compose RELEASE_ID
export RELEASE_ID="$BUILD_BUILDID-$RELEASE_RELEASENAME"

. "$CLOUD_PROVIDER.sh"
