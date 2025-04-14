#!/usr/bin/env bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

if [[ "$1" =~ ^test/stream/api/(.*) ]]; then
  test="${BASH_REMATCH[1]}"
  go tool gotestsum -f testname -- ./test/stream/api -race -count 1 -run TestStreamApiE2E/$test
fi

if [[ "$1" =~ ^test/templates/api/(.*) ]]; then
  test="${BASH_REMATCH[1]}"
  go tool gotestsum -f testname -- ./test/templates/api -race -count 1 -run TestTemplatesApiE2E/$test
fi

if [[ "$1" =~ ^test/cli/(.*) ]]; then
  test="${BASH_REMATCH[1]}"
  go tool gotestsum -f testname -- ./test/cli -race -count 1 -run TestCliE2E/$test
fi
