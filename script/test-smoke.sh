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

dd if=/dev/urandom of=/tmp/garage.1.rnd bs=512k count=1
dd if=/dev/urandom of=/tmp/garage.2.rnd bs=1M count=5
dd if=/dev/urandom of=/tmp/garage.3.rnd bs=1M count=10

for idx in $(seq 1 3); do
  s3grg cp /tmp/garage.$idx.rnd s3://eprouvette/
  s3grg ls s3://eprouvette
  s3grg cp s3://eprouvette/garage.$idx.rnd /tmp/garage.$idx.dl
  diff /tmp/garage.$idx.rnd /tmp/garage.$idx.dl
done

echo "success"
