#!/usr/bin/env bash

set -ex

SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
NIX_RELEASE="${REPO_FOLDER}/result/bin/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:${NIX_RELEASE}:$PATH"

if [ -z "$GARAGE_BIN" ]; then
	GARAGE_BIN=$(which garage || exit 1)
	echo -en "Found garage at: ${GARAGE_BIN}\n"
else
	echo -en "Using garage binary at: ${GARAGE_BIN}\n"
fi

$GARAGE_BIN -c /tmp/config.1.toml bucket create eprouvette
if [ "$GARAGE_08" = "1" ]; then
	KEY_INFO=$($GARAGE_BIN -c /tmp/config.1.toml key new --name opérateur)
else
	KEY_INFO=$($GARAGE_BIN -c /tmp/config.1.toml key create opérateur)
fi
ACCESS_KEY=`echo $KEY_INFO|grep -Po 'GK[a-f0-9]+'`
SECRET_KEY=`echo $KEY_INFO|grep -Po 'Secret key: [a-f0-9]+'|grep -Po '[a-f0-9]+$'`
$GARAGE_BIN -c /tmp/config.1.toml bucket allow eprouvette --read --write --owner --key $ACCESS_KEY
echo "$ACCESS_KEY $SECRET_KEY" > /tmp/garage.s3

echo "Bucket s3://eprouvette created. Credentials stored in /tmp/garage.s3."
