#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

if [ $# -lt 2 ]; then
  echo 1>&2 "$0: not enough arguments"
  exit 2
elif [ $# -gt 2 ]; then
  echo 1>&2 "$0: too many arguments"
  exit 2
fi

ARTIFACT_PATH=$1
TARGET_PATH=$2

# Notarize
echo "Notarizing '$ARTIFACT_PATH' -> '$TARGET_PATH'"
zip "$TARGET_PATH" "$ARTIFACT_PATH"
xcrun notarytool submit "$TARGET_PATH" --apple-id "$APPLE_ACCOUNT_USERNAME" --password "$APPLE_ACCOUNT_PASSWORD" --team-id "$APPLE_TEAM_ID" --wait
