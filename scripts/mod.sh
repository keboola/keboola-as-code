#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Change directory to the project root
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR/.."
pwd

# Download modules
echo "Downloading modules"
go mod download
go mod vendor
echo "Ok."
echo

# Check modules
echo "Running go mod tidy/verify ..."
go mod tidy
git diff --exit-code -- go.mod go.sum
go mod verify
echo "Ok. Tidy: go.mod and go.sum are valid."
echo
