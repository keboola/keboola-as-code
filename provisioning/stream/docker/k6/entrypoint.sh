#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

if [[ -n "$K6_CLOUD_API_TOKEN" ]]; then
    k6 cloud login --token "$K6_CLOUD_API_TOKEN"
fi

exec "k6" "$@"
