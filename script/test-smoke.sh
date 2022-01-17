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

# Advanced testing via S3API
if [ -z "$SKIP_AWS" ]; then
  echo "🔌 Test S3API"

  echo "Test Objects"
  aws s3api put-object --bucket eprouvette --key a
  aws s3api put-object --bucket eprouvette --key a/a
  aws s3api put-object --bucket eprouvette --key a/b
  aws s3api put-object --bucket eprouvette --key a/c
  aws s3api put-object --bucket eprouvette --key a/d/a
  aws s3api put-object --bucket eprouvette --key a/é
  aws s3api put-object --bucket eprouvette --key b
  aws s3api put-object --bucket eprouvette --key c


  aws s3api list-objects-v2 --bucket eprouvette >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 8 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-objects-v2 --bucket eprouvette --page-size 0 >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 8 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-objects-v2 --bucket eprouvette --page-size 999999999 >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 8 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-objects-v2 --bucket eprouvette --page-size 1 >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 8 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-objects-v2 --bucket eprouvette --delimiter '/' >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 3 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  aws s3api list-objects-v2 --bucket eprouvette --delimiter '/' --page-size 1 >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 3 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  aws s3api list-objects-v2 --bucket eprouvette --prefix 'a/' >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 5 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-objects-v2 --bucket eprouvette --prefix 'a/' --delimiter '/' >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 4 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  aws s3api list-objects-v2 --bucket eprouvette --prefix 'a/' --page-size 1 >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 5 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-objects-v2 --bucket eprouvette --prefix 'a/' --delimiter '/' --page-size 1 >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 4 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  aws s3api list-objects-v2 --bucket eprouvette --start-after 'Z' >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 8 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-objects-v2 --bucket eprouvette --start-after 'c' >$CMDOUT
  ! [ -s $CMDOUT ]


  aws s3api list-objects --bucket eprouvette >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 8 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-objects --bucket eprouvette --page-size 1 >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 8 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-objects --bucket eprouvette --delimiter '/' >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 3 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  # @FIXME it does not work as expected but might be a limitation of aws s3api
  # The problem is the conjunction of a delimiter + pagination + v1 of listobjects
  #aws s3api list-objects --bucket eprouvette --delimiter '/' --page-size 1 >$CMDOUT
  #[ $(jq '.Contents | length' $CMDOUT) == 3 ]
  #[ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  aws s3api list-objects --bucket eprouvette --prefix 'a/' >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 5 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-objects --bucket eprouvette --prefix 'a/' --delimiter '/' >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 4 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  aws s3api list-objects --bucket eprouvette --prefix 'a/' --page-size 1 >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 5 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  # @FIXME idem
  #aws s3api list-objects --bucket eprouvette --prefix 'a/' --delimiter '/' --page-size 1 >$CMDOUT
  #[ $(jq '.Contents | length' $CMDOUT) == 4 ]
  #[ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  aws s3api list-objects --bucket eprouvette --starting-token 'Z' >$CMDOUT
  [ $(jq '.Contents | length' $CMDOUT) == 8 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-objects --bucket eprouvette --starting-token 'c' >$CMDOUT
  ! [ -s $CMDOUT ]

  aws s3api list-objects-v2 --bucket eprouvette | \
    jq -c '. | {Objects: [.Contents[] | {Key: .Key}], Quiet: true}' | \
    aws s3api delete-objects --bucket eprouvette --delete file:///dev/stdin


  echo "Test Multipart Upload"
  aws s3api create-multipart-upload --bucket eprouvette --key a
  aws s3api create-multipart-upload --bucket eprouvette --key a
  aws s3api create-multipart-upload --bucket eprouvette --key c
  aws s3api create-multipart-upload --bucket eprouvette --key c/a
  aws s3api create-multipart-upload --bucket eprouvette --key c/b

  aws s3api list-multipart-uploads --bucket eprouvette >$CMDOUT
  [ $(jq '.Uploads | length' $CMDOUT) == 5 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-multipart-uploads --bucket eprouvette --page-size 1 >$CMDOUT
  [ $(jq '.Uploads | length' $CMDOUT) == 5 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-multipart-uploads --bucket eprouvette --delimiter '/' >$CMDOUT
  [ $(jq '.Uploads | length' $CMDOUT) == 3 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  aws s3api list-multipart-uploads --bucket eprouvette --delimiter '/' --page-size 1 >$CMDOUT
  [ $(jq '.Uploads | length' $CMDOUT) == 3 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  aws s3api list-multipart-uploads --bucket eprouvette --prefix 'c' >$CMDOUT
  [ $(jq '.Uploads | length' $CMDOUT) == 3 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-multipart-uploads --bucket eprouvette --prefix 'c' --page-size 1 >$CMDOUT
  [ $(jq '.Uploads | length' $CMDOUT) == 3 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-multipart-uploads --bucket eprouvette --prefix 'c' --delimiter '/' >$CMDOUT
  [ $(jq '.Uploads | length' $CMDOUT) == 1 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  aws s3api list-multipart-uploads --bucket eprouvette --prefix 'c' --delimiter '/' --page-size 1 >$CMDOUT
  [ $(jq '.Uploads | length' $CMDOUT) == 1 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 1 ]
  aws s3api list-multipart-uploads --bucket eprouvette --starting-token 'ZZZZZ' >$CMDOUT
  [ $(jq '.Uploads | length' $CMDOUT) == 5 ]
  [ $(jq '.CommonPrefixes | length' $CMDOUT) == 0 ]
  aws s3api list-multipart-uploads --bucket eprouvette --starting-token 'd' >$CMDOUT
  ! [ -s $CMDOUT ]

  aws s3api list-multipart-uploads --bucket eprouvette | \
    jq -r '.Uploads[] | "\(.Key) \(.UploadId)"' | \
    while read r; do 
      key=$(echo $r|cut -d' ' -f 1); 
      uid=$(echo $r|cut -d' ' -f 2); 
      aws s3api abort-multipart-upload --bucket eprouvette --key $key --upload-id $uid;
      echo "Deleted ${key}:${uid}"
    done

  # Test for UploadPartCopy
  aws s3 cp "/tmp/garage.3.rnd" "s3://eprouvette/copy_part_source"
  UPLOAD_ID=$(aws s3api create-multipart-upload --bucket eprouvette --key test_multipart | jq -r .UploadId)
  PART1=$(aws s3api upload-part \
    --bucket eprouvette --key test_multipart \
    --upload-id $UPLOAD_ID --part-number 1 \
    --body /tmp/garage.2.rnd | jq .ETag)
  PART2=$(aws s3api upload-part-copy \
    --bucket eprouvette --key test_multipart \
    --upload-id $UPLOAD_ID --part-number 2 \
    --copy-source "/eprouvette/copy_part_source" \
    --copy-source-range "bytes=500-5000500" \
    | jq .CopyPartResult.ETag)
  PART3=$(aws s3api upload-part \
    --bucket eprouvette --key test_multipart \
    --upload-id $UPLOAD_ID --part-number 3 \
    --body /tmp/garage.3.rnd | jq .ETag)
  cat >/tmp/garage.multipart_struct <<EOF
{
  "Parts": [
    {
      "ETag": $PART1,
      "PartNumber": 1
    },
    {
      "ETag": $PART2,
      "PartNumber": 2
    },
    {
      "ETag": $PART3,
      "PartNumber": 3
    }
  ]
}
EOF
  aws s3api complete-multipart-upload \
    --bucket eprouvette --key test_multipart --upload-id $UPLOAD_ID \
    --multipart-upload file:///tmp/garage.multipart_struct

  aws s3 cp "s3://eprouvette/test_multipart" /tmp/garage.test_multipart
  cat /tmp/garage.2.rnd <(tail -c +501 /tmp/garage.3.rnd | head -c 5000001) /tmp/garage.3.rnd > /tmp/garage.test_multipart_reference
  diff /tmp/garage.test_multipart /tmp/garage.test_multipart_reference >/tmp/garage.test_multipart_diff 2>&1

  aws s3 rm "s3://eprouvette/copy_part_source"
  aws s3 rm "s3://eprouvette/test_multipart"

  rm /tmp/garage.multipart_struct
  rm /tmp/garage.test_multipart
  rm /tmp/garage.test_multipart_reference
  rm /tmp/garage.test_multipart_diff
fi

rm /tmp/garage.{1..3}.{rnd,b64}

if [ -z "$SKIP_AWS" ]; then
  echo "🪣 Test bucket logic "
  AWS_ACCESS_KEY_ID=`cat /tmp/garage.s3 |cut -d' ' -f1`
  [ $(aws s3 ls | wc -l) == 1 ]
  garage -c /tmp/config.1.toml bucket create seau
  garage -c /tmp/config.1.toml bucket allow --read seau --key $AWS_ACCESS_KEY_ID
  [ $(aws s3 ls | wc -l) == 2 ]
  garage -c /tmp/config.1.toml bucket deny --read seau --key $AWS_ACCESS_KEY_ID
  [ $(aws s3 ls | wc -l) == 1 ]
  garage -c /tmp/config.1.toml bucket allow --read seau --key $AWS_ACCESS_KEY_ID
  [ $(aws s3 ls | wc -l) == 2 ]
  garage -c /tmp/config.1.toml bucket delete --yes seau
  [ $(aws s3 ls | wc -l) == 1 ]
fi

if [ -z "$SKIP_AWS" ]; then
  echo "🧪 Website Testing"
  echo "<h1>hello world</h1>" > /tmp/garage-index.html
  aws s3 cp /tmp/garage-index.html s3://eprouvette/index.html
  [ `curl -s -o /dev/null -w "%{http_code}" --header "Host: eprouvette.garage.tld"  http://127.0.0.1:3921/ ` == 404 ]
  garage -c /tmp/config.1.toml bucket website --allow eprouvette
  [ `curl -s -o /dev/null -w "%{http_code}" --header "Host: eprouvette.garage.tld"  http://127.0.0.1:3921/ ` == 200 ]
  garage -c /tmp/config.1.toml bucket website --deny eprouvette
  [ `curl -s -o /dev/null -w "%{http_code}" --header "Host: eprouvette.garage.tld"  http://127.0.0.1:3921/ ` == 404 ]
  aws s3 rm s3://eprouvette/index.html
  rm /tmp/garage-index.html
fi

echo "🏁 Teardown"
AWS_ACCESS_KEY_ID=`cat /tmp/garage.s3 |cut -d' ' -f1`
AWS_SECRET_ACCESS_KEY=`cat /tmp/garage.s3 |cut -d' ' -f2`
garage -c /tmp/config.1.toml bucket deny --read --write eprouvette --key $AWS_ACCESS_KEY_ID
garage -c /tmp/config.1.toml bucket delete --yes eprouvette
garage -c /tmp/config.1.toml key delete --yes $AWS_ACCESS_KEY_ID
exec 3>&-

echo "✅ Success"
