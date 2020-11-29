#!/bin/bash

set -ex
shopt -s expand_aliases

SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"

cargo build
${SCRIPT_FOLDER}/dev-clean.sh
${SCRIPT_FOLDER}/dev-cluster.sh > /tmp/garage.log 2>&1 &
${SCRIPT_FOLDER}/dev-configure.sh
${SCRIPT_FOLDER}/dev-bucket.sh
source ${SCRIPT_FOLDER}/dev-env.sh

dd if=/dev/urandom of=/tmp/garage.rnd bs=1M count=10

s3grg cp /tmp/garage.rnd s3://eprouvette/
s3grg ls s3://eprouvette
s3grg cp s3://eprouvette/garage.rnd /tmp/garage.dl

diff /tmp/garage.rnd /tmp/garage.dl
