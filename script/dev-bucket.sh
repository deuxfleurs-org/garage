#!/bin/bash

SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:$PATH"

garage bucket create eprouvette
KEY_INFO=`garage key new --name opérateur`
ACCESS_KEY=`echo $KEY_INFO|grep -Po 'GK[a-f0-9]+'`
SECRET_KEY=`echo $KEY_INFO|grep -Po 'secret_key: "[a-f0-9]+'|grep -Po '[a-f0-9]+$'`
garage bucket allow eprouvette --read --write --key $ACCESS_KEY
echo "$ACCESS_KEY $SECRET_KEY" > /tmp/garage.s3

echo "Bucket s3://eprouvette created. Credentials stored in /tmp/garage.s3."
