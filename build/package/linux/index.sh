#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

# Check for gpg
if ! command -v gpg >/dev/null 2>&1; then
  echo "gpg is required but not installed. Aborting."
  exit 1
fi

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# https://gist.github.com/sj26/88e1c6584397bb7c13bd11108a579746
function retry {
  local retries=$1
  shift

  local count=0
  until "$@"; do
    exit=$?
    wait=$((2 ** $count))
    count=$(($count + 1))
    if [ $count -lt $retries ]; then
      echo "Retry $count/$retries exited $exit, retrying in $wait seconds..."
      sleep $wait
    else
      echo "Retry $count/$retries exited $exit, no more retries left."
      return $exit
    fi
  done
  return 0
}

function indexDeb {
  docker compose run --rm -T -u "$(id -u):$(id -g)" deb
  echo "OK. DEB packages indexed."
  echo
  echo
}

function indexRpm {
  docker compose run --rm -T -u "$(id -u):$(id -g)" rpm
  echo "OK. RPM packages indexed."
  echo
  echo
}

function indexApk {
  docker compose run --rm -T -u "$(id -u):$(id -g)" apk
  echo "OK. APK packages indexed."
  echo
  echo
}

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
docker compose pull -q

echo "Indexing DEB packages for Debian ..."
retry 5 indexDeb

echo "Indexing RPM packages for Fedora ..."
retry 5 indexRpm

echo "Indexing APK packages for Alpine ..."
retry 5 indexApk

echo "Write repository public keys ..."
mkdir -p "${PACKAGES_DIR}/deb"
echo "${DEB_KEY_PUBLIC}" | gpg --dearmor > "${PACKAGES_DIR}/deb/keboola.gpg"
mkdir -p "${PACKAGES_DIR}/rpm"
echo "${RPM_KEY_PUBLIC}" > "${PACKAGES_DIR}/rpm/keboola.gpg"
mkdir -p "${PACKAGES_DIR}/apk"
echo "${APK_KEY_PUBLIC}" > "${PACKAGES_DIR}/apk/keboola.rsa.pub"
echo "All OK."
