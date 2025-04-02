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

# Version to install
GOLANGCI_LINT_VERSION="v2.0.2"

# Install or update golangci-lint
echo "Installing golangci-lint $GOLANGCI_LINT_VERSION..."
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b "$(go env GOPATH)/bin" "$GOLANGCI_LINT_VERSION"

# Verify installation
echo "Verifying golangci-lint installation..."
golangci-lint --version

echo "golangci-lint $GOLANGCI_LINT_VERSION has been installed successfully." 
