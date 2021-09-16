#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

if [ $# -lt 1 ]; then
  echo 1>&2 "$0: not enough arguments"
  exit 2
elif [ $# -gt 1 ]; then
  echo 1>&2 "$0: too many arguments"
  exit 2
fi

export PACKAGES_DIR="$(realpath "$1")"
echo "Packages dir: $PACKAGES_DIR"

SCRIPTS_DIR_REL="$(dirname "$0")"
SCRIPTS_DIR="$(realpath "$SCRIPTS_DIR_REL")"
cd $SCRIPTS_DIR

echo "Pulling Docker images ..."
docker-compose pull -q

echo "Indexing DEB packages for Debian ..."
docker-compose run --rm -u "$(id -u):$(id -g)" deb
echo "OK. DEB packages indexed."
echo
echo

echo "Indexing RPM packages for Fedora ..."
docker-compose run --rm -u "$(id -u):$(id -g)" rpm
echo "OK. RPM packages indexed."
echo
echo

echo "Indexing APK packages for Alpine ..."
docker-compose run --rm -u "$(id -u):$(id -g)" apk
echo "OK. APK packages indexed."
echo
echo

echo "Write repository public keys ..."
mkdir -p "${PACKAGES_DIR}/deb"
echo "${DEB_KEY_PUBLIC}" | gpg --dearmor > "${PACKAGES_DIR}/deb/keboola.gpg"
mkdir -p "${PACKAGES_DIR}/rpm"
echo "${RPM_KEY_PUBLIC}" > "${PACKAGES_DIR}/rpm/keboola.gpg"
mkdir -p "${PACKAGES_DIR}/apk"
echo "${APK_KEY_PUBLIC}" > "${PACKAGES_DIR}/apk/keboola.rsa.pub"
echo "All OK."
