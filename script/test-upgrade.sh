#!/usr/bin/env bash

set -ex

export LC_ALL=C.UTF-8
export LANG=C.UTF-8
SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
NIX_RELEASE="${REPO_FOLDER}/result/bin/:${REPO_FOLDER}/result-bin/bin/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:${NIX_RELEASE}:$PATH"

OLD_VERSION="$1"
ARCH="$2"


echo "Downloading old garage binary..."
curl https://garagehq.deuxfleurs.fr/_releases/$OLD_VERSION/$ARCH/garage > /tmp/old_garage
chmod +x /tmp/old_garage

echo "============= insert data into old version cluster ================="

export GARAGE_BIN=/tmp/old_garage
if echo $OLD_VERSION | grep 'v0\.8\.'; then
	echo "Detected Garage v0.8.x"
	export GARAGE_08=1
fi

echo "⏳ Setup cluster using old version"
$GARAGE_BIN --version
${SCRIPT_FOLDER}/dev-clean.sh
${SCRIPT_FOLDER}/dev-cluster.sh > /tmp/garage.log 2>&1 &
sleep 6
${SCRIPT_FOLDER}/dev-configure.sh
${SCRIPT_FOLDER}/dev-bucket.sh

echo "🛠️ Inserting data in old cluster"
source ${SCRIPT_FOLDER}/dev-env-rclone.sh
rclone copy "${SCRIPT_FOLDER}/../.git/" garage:eprouvette/test_dotgit --stats=1s --stats-log-level=NOTICE --stats-one-line

echo "🏁 Stopping old cluster"
killall -INT old_garage
sleep 2
killall -9 old_garage || true

echo "🏁 Removing old garage version"
rm -rv $GARAGE_BIN
export -n GARAGE_BIN
export -n GARAGE_08

echo "================ read data from new cluster ==================="

echo "⏳ Setup cluster using new version"
pwd
ls
export GARAGE_BIN=$(which garage)
$GARAGE_BIN --version
${SCRIPT_FOLDER}/dev-cluster.sh >> /tmp/garage.log 2>&1 &
sleep 3

echo "🛠️ Retrieving data from old cluster"
rclone copy garage:eprouvette/test_dotgit /tmp/test_dotgit --stats=1s --stats-log-level=NOTICE --stats-one-line --fast-list

if ! diff <(find "${SCRIPT_FOLDER}/../.git" -type f | xargs md5sum | cut -d ' ' -f 1 | sort) <(find /tmp/test_dotgit -type f | xargs md5sum | cut -d ' ' -f 1 | sort); then
	echo "TEST FAILURE: directories are different"
	exit 1
fi
rm -r /tmp/test_dotgit

echo "🏁 Teardown"
rm -rf /tmp/garage-{data,meta}-*
rm -rf /tmp/config.*.toml

echo "✅ Success"
