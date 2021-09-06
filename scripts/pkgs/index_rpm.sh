#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Import keys
echo -e "$GPG_PUBLIC"  | gpg --import --batch
echo -e "$GPG_PRIVATE" | gpg --import --batch

# Index
cd /packages
createrepo --skip-stat --update  .

# Sign
gpg --yes --detach-sign --armor repodata/repomd.xml
