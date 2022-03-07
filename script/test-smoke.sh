#!/usr/bin/env bash

set -ex

export LC_ALL=C.UTF-8
export LANG=C.UTF-8
SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
NIX_RELEASE="${REPO_FOLDER}/result/bin/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:${NIX_RELEASE}:$PATH"
CMDOUT=/tmp/garage.cmd.tmp

# @FIXME Duck is not ready for testing, we have a bug
SKIP_DUCK=1

echo "⏳ Setup"
${SCRIPT_FOLDER}/dev-clean.sh
${SCRIPT_FOLDER}/dev-cluster.sh > /tmp/garage.log 2>&1 &
sleep 6
${SCRIPT_FOLDER}/dev-configure.sh
${SCRIPT_FOLDER}/dev-bucket.sh

which garage
garage -c /tmp/config.1.toml status
garage -c /tmp/config.1.toml key list
garage -c /tmp/config.1.toml bucket list

dd if=/dev/urandom of=/tmp/garage.1.rnd bs=1k count=2 # No multipart, inline storage (< INLINE_THRESHOLD = 3072 bytes)
dd if=/dev/urandom of=/tmp/garage.2.rnd bs=1M count=5 # No multipart but file will be chunked
dd if=/dev/urandom of=/tmp/garage.3.rnd bs=1M count=10 # by default, AWS starts using multipart at 8MB

# data of lower entropy, to test compression
dd if=/dev/urandom bs=1k count=2  | base64 -w0 > /tmp/garage.1.b64
dd if=/dev/urandom bs=1M count=5  | base64 -w0 > /tmp/garage.2.b64
dd if=/dev/urandom bs=1M count=10 | base64 -w0 > /tmp/garage.3.b64

echo "🧪 S3 API testing..."

# AWS
if [ -z "$SKIP_AWS" ]; then
  echo "🛠️ Testing with awscli"
  source ${SCRIPT_FOLDER}/dev-env-aws.sh
  aws s3 ls
  for idx in {1..3}.{rnd,b64}; do
    aws s3 cp "/tmp/garage.$idx" "s3://eprouvette/&+-é\"/garage.$idx.aws"
    aws s3 ls s3://eprouvette
    aws s3 cp "s3://eprouvette/&+-é\"/garage.$idx.aws" "/tmp/garage.$idx.dl"
    diff /tmp/garage.$idx /tmp/garage.$idx.dl
    rm /tmp/garage.$idx.dl
    aws s3 rm "s3://eprouvette/&+-é\"/garage.$idx.aws"
  done
fi

# S3CMD
if [ -z "$SKIP_S3CMD" ]; then
  echo "🛠️ Testing with s3cmd"
  source ${SCRIPT_FOLDER}/dev-env-s3cmd.sh
  s3cmd ls
  for idx in {1..3}.{rnd,b64}; do
    s3cmd put "/tmp/garage.$idx" "s3://eprouvette/&+-é\"/garage.$idx.s3cmd"
    s3cmd ls s3://eprouvette
    s3cmd get "s3://eprouvette/&+-é\"/garage.$idx.s3cmd" "/tmp/garage.$idx.dl"
    diff /tmp/garage.$idx /tmp/garage.$idx.dl
    rm /tmp/garage.$idx.dl
    s3cmd rm "s3://eprouvette/&+-é\"/garage.$idx.s3cmd"
  done
fi

# Minio Client
if [ -z "$SKIP_MC" ]; then
  echo "🛠️ Testing with mc (minio client)"
  source ${SCRIPT_FOLDER}/dev-env-mc.sh
  mc ls garage/
  for idx in {1..3}.{rnd,b64}; do
    mc cp "/tmp/garage.$idx" "garage/eprouvette/&+-é\"/garage.$idx.mc"
    mc ls garage/eprouvette
    mc cp "garage/eprouvette/&+-é\"/garage.$idx.mc" "/tmp/garage.$idx.dl"
    diff /tmp/garage.$idx /tmp/garage.$idx.dl
    rm /tmp/garage.$idx.dl
    mc rm "garage/eprouvette/&+-é\"/garage.$idx.mc"
  done
fi

# RClone
if [ -z "$SKIP_RCLONE" ]; then
  echo "🛠️ Testing with rclone"
  source ${SCRIPT_FOLDER}/dev-env-rclone.sh
  rclone lsd garage:
  for idx in {1..3}.{rnd,b64}; do
    cp /tmp/garage.$idx /tmp/garage.$idx.dl
    rclone copy "/tmp/garage.$idx.dl" "garage:eprouvette/&+-é\"/"
    rm /tmp/garage.$idx.dl
    rclone ls garage:eprouvette
    rclone copy "garage:eprouvette/&+-é\"/garage.$idx.dl" "/tmp/"
    diff /tmp/garage.$idx /tmp/garage.$idx.dl
    rm /tmp/garage.$idx.dl
    rclone delete "garage:eprouvette/&+-é\"/garage.$idx.dl"
  done
fi

# Duck (aka Cyberduck CLI)
if [ -z "$SKIP_DUCK" ]; then
  echo "🛠️ Testing with duck (aka cyberduck cli)"
  source ${SCRIPT_FOLDER}/dev-env-duck.sh
  duck --list garage:/
  duck --mkdir "garage:/eprouvette/duck"
  for idx in {1..3}.{rnd,b64}; do
    duck --verbose --upload "garage:/eprouvette/duck/" "/tmp/garage.$idx"
    duck --list garage:/eprouvette/duck/
    duck --download "garage:/eprouvette/duck/garage.$idx" "/tmp/garage.$idx.dl"
    diff /tmp/garage.$idx /tmp/garage.$idx.dl
    rm /tmp/garage.$idx.dl
    duck --delete "garage:/eprouvette/duck/garage.$idx.dk"
  done
fi

if [ -z "$SKIP_WINSCP" ]; then
  echo "🛠️ Testing with winscp"
  source ${SCRIPT_FOLDER}/dev-env-winscp.sh
  winscp <<EOF
open $WINSCP_URL
ls
mkdir eprouvette/winscp
EOF
  for idx in {1..3}.{rnd,b64}; do
    winscp <<EOF
open $WINSCP_URL
put Z:\\tmp\\garage.$idx eprouvette/winscp/garage.$idx.winscp
ls eprouvette/winscp/
get eprouvette/winscp/garage.$idx.winscp Z:\\tmp\\garage.$idx.dl
rm eprouvette/winscp/garage.$idx.winscp
EOF
    diff /tmp/garage.$idx /tmp/garage.$idx.dl
    rm /tmp/garage.$idx.dl
  done
  winscp <<EOF
open $WINSCP_URL
rm eprouvette/winscp
EOF
fi

rm /tmp/garage.{1..3}.{rnd,b64}

echo "🏁 Teardown"
AWS_ACCESS_KEY_ID=`cat /tmp/garage.s3 |cut -d' ' -f1`
AWS_SECRET_ACCESS_KEY=`cat /tmp/garage.s3 |cut -d' ' -f2`
garage -c /tmp/config.1.toml bucket deny --read --write eprouvette --key $AWS_ACCESS_KEY_ID
garage -c /tmp/config.1.toml bucket delete --yes eprouvette
garage -c /tmp/config.1.toml key delete --yes $AWS_ACCESS_KEY_ID
exec 3>&-

echo "✅ Success"
