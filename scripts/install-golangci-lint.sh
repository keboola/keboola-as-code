#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Version to install
GOLANGCI_LINT_VERSION="v2.1.5"

# Determine installation directory
BIN_DIR="$(go env GOBIN)"
if [ -z "$BIN_DIR" ]; then
  # If GOBIN is not set, use GOPATH/bin or default to ~/go/bin
  GOPATH="$(go env GOPATH)"
  if [ -z "$GOPATH" ]; then
    GOPATH="$HOME/go"
  fi
  BIN_DIR="$GOPATH/bin"
fi

# Ensure the bin directory exists
mkdir -p "$BIN_DIR"

# Install or update golangci-lint
echo "Installing golangci-lint $GOLANGCI_LINT_VERSION..."
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b "$BIN_DIR" "$GOLANGCI_LINT_VERSION"

# Verify installation
echo "Verifying golangci-lint installation..."
golangci-lint --version

echo "golangci-lint $GOLANGCI_LINT_VERSION has been installed successfully." 
