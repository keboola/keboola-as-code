#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Import keys
echo -e "$RPM_KEY_PUBLIC"  | gpg --import --batch
echo -e "$RPM_KEY_PRIVATE" | gpg --import --batch

cd /packages/rpm

# Clear (if previous build failed)
rm -rf .repodata || true
rm -rf repodata.old.* || true

# Index
createrepo --update  .

# Sign
gpg --yes --detach-sign --armor repodata/repomd.xml

# Clear
rm -rf .repodata || true
rm -rf repodata.old.* || true
