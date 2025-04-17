#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Version to install
GORELEASER_VERSION="v2.8.2"

# Install or update goreleaser
echo "Installing goreleaser $GORELEASER_VERSION..."
go install github.com/goreleaser/goreleaser/v2@$GORELEASER_VERSION

# Verify installation
echo "Verifying goreleaser installation..."
goreleaser --version

echo "goreleaser $GORELEASER_VERSION has been installed successfully."
