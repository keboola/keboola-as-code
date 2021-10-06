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

echo "Running go vet ..."
if ! go vet ./...; then
    echo "Please fix ^^^ errors. You can try run \"make fix\"."
    echo
    exit 1
fi


# Run linters
echo "Running golangci-lint ..."
if golangci-lint run --timeout=2m0s -c "./build/ci/golangci.yml"; then
    echo "Ok. The code looks good."
    echo
else
    echo "Please fix ^^^ errors. You can try run \"make fix\"."
    echo
    exit 1
fi
