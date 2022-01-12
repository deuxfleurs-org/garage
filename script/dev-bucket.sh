#!/usr/bin/env bash

set -ex

SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
NIX_RELEASE="${REPO_FOLDER}/result/bin/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:${NIX_RELEASE}:$PATH"

garage -c /tmp/config.1.toml bucket create eprouvette
KEY_INFO=$(garage -c /tmp/config.1.toml key new --name opérateur)
ACCESS_KEY=`echo $KEY_INFO|grep -Po 'GK[a-f0-9]+'`
SECRET_KEY=`echo $KEY_INFO|grep -Po 'Secret key: [a-f0-9]+'|grep -Po '[a-f0-9]+$'`
garage -c /tmp/config.1.toml bucket allow eprouvette --read --write --key $ACCESS_KEY
echo "$ACCESS_KEY $SECRET_KEY" > /tmp/garage.s3

echo "Bucket s3://eprouvette created. Credentials stored in /tmp/garage.s3."
