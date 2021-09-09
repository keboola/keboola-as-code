#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

mkdir -p ~/.gnupg
echo "cert-digest-algo SHA256" >> ~/.gnupg/gpg.conf
echo "digest-algo SHA256" >> ~/.gnupg/gpg.conf
chmod -R 0700 ~/.gnupg

# Import keys
echo -e "$DEB_KEY_PUBLIC"  | gpg --import --batch
echo -e "$DEB_KEY_PRIVATE" | gpg --import --batch

# Index
cd /packages/deb
apt-ftparchive packages pool > Packages
cat Packages | gzip > Packages.gz

# Sign
apt-ftparchive release . > Release
gpg --yes -abs -o Release.gpg Release
gpg --yes --clearsign -o InRelease Release
