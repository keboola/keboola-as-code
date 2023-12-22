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
HASH=`echo "${ARTIFACT_PATH}" | md5`
GON_CONFIG_FILE="gon.$HASH.json"

# Create config file
cat <<EOT >> "./${GON_CONFIG_FILE}"
{
    "source": ["${ARTIFACT_PATH}"],
    "bundle_id": "${APPLE_BUNDLE_ID}",
    "apple_id": {"username": "${APPLE_ACCOUNT_USERNAME}", "password": "@env:APPLE_ACCOUNT_PASSWORD", "provider": "${APPLE_TEAM_ID}"},
    "sign": { "application_identity": "Developer ID Application: ${APPLE_TEAM_NAME}" },
    "zip": { "output_path": "${TARGET_PATH}" }
}
EOT
echo "Created GON config file '${GON_CONFIG_FILE}'"

# Notarize
echo "Notarizing '$ARTIFACT_PATH' -> '$TARGET_PATH'"
gon -log-level=info "${GON_CONFIG_FILE}"
