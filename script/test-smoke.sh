#!/bin/bash

set -ex

export LC_ALL=C.UTF-8
export LANG=C.UTF-8
SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:$PATH"

echo "⏳ Setup"
cargo build
${SCRIPT_FOLDER}/dev-clean.sh
${SCRIPT_FOLDER}/dev-cluster.sh > /tmp/garage.log 2>&1 &
${SCRIPT_FOLDER}/dev-configure.sh
${SCRIPT_FOLDER}/dev-bucket.sh

which garage
garage status
garage key list
garage bucket list

dd if=/dev/urandom of=/tmp/garage.1.rnd bs=1k count=2 # No multipart, inline storage (< INLINE_THRESHOLD = 3072 bytes)
dd if=/dev/urandom of=/tmp/garage.2.rnd bs=1M count=5 # No multipart but file will be chunked
dd if=/dev/urandom of=/tmp/garage.3.rnd bs=1M count=10 # by default, AWS starts using multipart at 8MB

echo "🧪 S3 API testing..."

# AWS
if [ -z "$SKIP_AWS" ]; then
  echo "🛠️ Testing with awscli"
  source ${SCRIPT_FOLDER}/dev-env-aws.sh
  aws s3 ls
  for idx in $(seq 1 3); do
    aws s3 cp "/tmp/garage.$idx.rnd" "s3://eprouvette/&+-é\"/garage.$idx.aws"
    aws s3 ls s3://eprouvette
    aws s3 cp "s3://eprouvette/&+-é\"/garage.$idx.aws" "/tmp/garage.$idx.dl"
    diff /tmp/garage.$idx.rnd /tmp/garage.$idx.dl
    rm /tmp/garage.$idx.dl
    aws s3 rm "s3://eprouvette/&+-é\"/garage.$idx.aws"
  done
fi

# S3CMD
if [ -z "$SKIP_S3CMD" ]; then
  echo "🛠️ Testing with s3cmd"
  source ${SCRIPT_FOLDER}/dev-env-s3cmd.sh
  s3cmd ls
  for idx in $(seq 1 3); do
    s3cmd put "/tmp/garage.$idx.rnd" "s3://eprouvette/&+-é\"/garage.$idx.s3cmd"
    s3cmd ls s3://eprouvette
    s3cmd get "s3://eprouvette/&+-é\"/garage.$idx.s3cmd" "/tmp/garage.$idx.dl"
    diff /tmp/garage.$idx.rnd /tmp/garage.$idx.dl
    rm /tmp/garage.$idx.dl
    s3cmd rm "s3://eprouvette/&+-é\"/garage.$idx.s3cmd"
  done
fi

# Minio Client
if [ -z "$SKIP_MC" ]; then
  echo "🛠️ Testing with mc (minio client)"
  source ${SCRIPT_FOLDER}/dev-env-mc.sh
  mc ls garage/
  for idx in $(seq 1 3); do
    mc cp "/tmp/garage.$idx.rnd" "garage/eprouvette/&+-é\"/garage.$idx.mc"
    mc ls garage/eprouvette
    mc cp "garage/eprouvette/&+-é\"/garage.$idx.mc" "/tmp/garage.$idx.dl"
    diff /tmp/garage.$idx.rnd /tmp/garage.$idx.dl
    rm /tmp/garage.$idx.dl
    mc rm "garage/eprouvette/&+-é\"/garage.$idx.mc"
  done
fi

# RClone
if [ -z "$SKIP_RCLONE" ]; then
  echo "🛠️ Testing with rclone"
  source ${SCRIPT_FOLDER}/dev-env-rclone.sh
  rclone lsd garage:
  for idx in $(seq 1 3); do
    cp /tmp/garage.$idx.rnd /tmp/garage.$idx.dl
    rclone copy "/tmp/garage.$idx.dl" "garage:eprouvette/&+-é\"/"
    rm /tmp/garage.$idx.dl
    rclone ls garage:eprouvette
    rclone copy "garage:eprouvette/&+-é\"/garage.$idx.dl" "/tmp/"
    diff /tmp/garage.$idx.rnd /tmp/garage.$idx.dl
    rm /tmp/garage.$idx.dl
    rclone delete "garage:eprouvette/&+-é\"/garage.$idx.dl"
  done
fi

rm /tmp/garage.{1,2,3}.rnd

if [ -z "$SKIP_AWS" ]; then
  echo "🧪 Website Testing"
  echo "<h1>hello world</h1>" > /tmp/garage-index.html
  aws s3 cp /tmp/garage-index.html s3://eprouvette/index.html
  [ `curl -s -o /dev/null -w "%{http_code}" --header "Host: eprouvette.garage.tld"  http://127.0.0.1:3923/ ` == 404 ]
  garage bucket website --allow eprouvette
  [ `curl -s -o /dev/null -w "%{http_code}" --header "Host: eprouvette.garage.tld"  http://127.0.0.1:3923/ ` == 200 ]
  garage bucket website --deny eprouvette
  [ `curl -s -o /dev/null -w "%{http_code}" --header "Host: eprouvette.garage.tld"  http://127.0.0.1:3923/ ` == 404 ]
  aws s3 rm s3://eprouvette/index.html
  rm /tmp/garage-index.html
fi

echo "🏁 Teardown"
AWS_ACCESS_KEY_ID=`cat /tmp/garage.s3 |cut -d' ' -f1`
AWS_SECRET_ACCESS_KEY=`cat /tmp/garage.s3 |cut -d' ' -f2`
garage bucket deny --read --write eprouvette --key $AWS_ACCESS_KEY_ID
garage bucket delete --yes eprouvette
garage key delete --yes $AWS_ACCESS_KEY_ID

echo "✅ Success"
