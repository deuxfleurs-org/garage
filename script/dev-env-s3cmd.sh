#!/bin/bash

SCRIPT_FOLDER="`dirname \"${BASH_SOURCE[0]}\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:$PATH"

ACCESS_KEY=`cat /tmp/garage.s3 |cut -d' ' -f1`
SECRET_KEY=`cat /tmp/garage.s3 |cut -d' ' -f2`

alias s3grg="s3cmd \
  --host 127.0.0.1:3911 \
  --host-bucket 127.0.0.1:3911 \
  --access_key=$ACCESS_KEY \
  --secret_key=$SECRET_KEY \
  --region=garage \
  --no-ssl"

