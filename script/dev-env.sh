#!/bin/bash

SCRIPT_FOLDER="`dirname \"${BASH_SOURCE[0]}\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:$PATH"

export AWS_ACCESS_KEY_ID=`cat /tmp/garage.s3 |cut -d' ' -f1`
export AWS_SECRET_ACCESS_KEY=`cat /tmp/garage.s3 |cut -d' ' -f2`
export AWS_DEFAULT_REGION='garage'

alias s3grg="aws s3 \
  --endpoint-url http://127.0.0.1:3900"

