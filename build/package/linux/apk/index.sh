#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Import key
mkdir -p "$HOME/.abuild"
echo -e "$APK_KEY_PUBLIC"   > "/etc/apk/keys/keboola.rsa.pub"
echo -e "$APK_KEY_PRIVATE"  > "$HOME/.abuild/keboola.rsa"

# Index and sign
cd /packages/apk
for DIR in `find ~+ -mindepth 1 -type d`; do
    if [ -d "${DIR}" ]; then
        echo "Arch '$DIR' ..."
        cd "$DIR"
        apk index -Uv -x APKINDEX.tar.gz -o APKINDEX.new.tar.gz ./*.apk
        abuild-sign -k "$HOME/.abuild/keboola.rsa" APKINDEX.new.tar.gz
        mv -vf APKINDEX.new.tar.gz APKINDEX.tar.gz
        echo
    fi
done
