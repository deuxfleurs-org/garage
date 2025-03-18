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
	export GARAGE_OLDVER=v08
elif (echo $OLD_VERSION | grep 'v0\.9\.') || (echo $OLD_VERSION | grep 'v1\.'); then
	echo "Detected Garage v0.9.x / v1.x"
	export GARAGE_OLDVER=v1
fi

if echo $OLD_VERSION | grep 'v1\.'; then
	DO_SSEC_TEST=1
fi
SSEC_KEY="u8zCfnEyt5Imo/krN+sxA1DQXxLWtPJavU6T6gOVj1Y="

echo "⏳ Setup cluster using old version"
$GARAGE_BIN --version
${SCRIPT_FOLDER}/dev-clean.sh
${SCRIPT_FOLDER}/dev-cluster.sh > /tmp/garage.log 2>&1 &
sleep 6
${SCRIPT_FOLDER}/dev-configure.sh
${SCRIPT_FOLDER}/dev-bucket.sh

echo "🛠️ Inserting data in old cluster"
source ${SCRIPT_FOLDER}/dev-env-rclone.sh
rclone copy "${SCRIPT_FOLDER}/../.git/" garage:eprouvette/test_dotgit \
	--stats=1s --stats-log-level=NOTICE --stats-one-line

if [ "$DO_SSEC_TEST" = "1" ]; then
	# upload small file (should be single part)
	rclone copy "${SCRIPT_FOLDER}/test-upgrade.sh" garage:eprouvette/test-ssec \
		--s3-sse-customer-algorithm AES256 \
		--s3-sse-customer-key-base64 "$SSEC_KEY" \
		--stats=1s --stats-log-level=NOTICE --stats-one-line
	# do a multipart upload
	dd if=/dev/urandom of=/tmp/randfile-for-upgrade bs=5M count=5
	rclone copy "/tmp/randfile-for-upgrade" garage:eprouvette/test-ssec \
		--s3-chunk-size 5M \
		--s3-sse-customer-algorithm AES256 \
		--s3-sse-customer-key-base64 "$SSEC_KEY" \
		--stats=1s --stats-log-level=NOTICE --stats-one-line
fi

echo "🏁 Stopping old cluster"
killall -INT old_garage
sleep 2
killall -9 old_garage || true

echo "🏁 Removing old garage version"
rm -rv $GARAGE_BIN
export -n GARAGE_BIN
export -n GARAGE_OLDVER

echo "================ read data from new cluster ==================="

echo "⏳ Setup cluster using new version"
pwd
ls
export GARAGE_BIN=$(which garage)
$GARAGE_BIN --version
${SCRIPT_FOLDER}/dev-cluster.sh >> /tmp/garage.log 2>&1 &
sleep 3

echo "🛠️ Retrieving data from old cluster"
rclone copy garage:eprouvette/test_dotgit /tmp/test_dotgit \
	--stats=1s --stats-log-level=NOTICE --stats-one-line --fast-list

if ! diff <(find "${SCRIPT_FOLDER}/../.git" -type f | xargs md5sum | cut -d ' ' -f 1 | sort) <(find /tmp/test_dotgit -type f | xargs md5sum | cut -d ' ' -f 1 | sort); then
	echo "TEST FAILURE: directories are different"
	exit 1
fi
rm -r /tmp/test_dotgit

if [ "$DO_SSEC_TEST" = "1" ]; then
	rclone copy garage:eprouvette/test-ssec /tmp/test_ssec_out \
		--s3-sse-customer-algorithm AES256 \
		--s3-sse-customer-key-base64 "$SSEC_KEY" \
		--stats=1s --stats-log-level=NOTICE --stats-one-line
	if ! diff "/tmp/test_ssec_out/test-upgrade.sh" "${SCRIPT_FOLDER}/test-upgrade.sh"; then
		echo "SSEC-FAILURE (small file)"
		exit 1
	fi
	if ! diff "/tmp/test_ssec_out/randfile-for-upgrade" "/tmp/randfile-for-upgrade"; then
		echo "SSEC-FAILURE (big file)"
		exit 1
	fi
	rm -r /tmp/test_ssec_out
	rm /tmp/randfile-for-upgrade
fi

echo "🏁 Teardown"
rm -rf /tmp/garage-{data,meta}-*
rm -rf /tmp/config.*.toml

echo "✅ Success"
