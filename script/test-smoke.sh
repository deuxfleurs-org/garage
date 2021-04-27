#!/bin/bash

set -ex
shopt -s expand_aliases

export LC_ALL=C.UTF-8
export LANG=C.UTF-8
SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"

echo "setup"
cargo build
${SCRIPT_FOLDER}/dev-clean.sh
${SCRIPT_FOLDER}/dev-cluster.sh > /tmp/garage.log 2>&1 &
${SCRIPT_FOLDER}/dev-configure.sh
${SCRIPT_FOLDER}/dev-bucket.sh
source ${SCRIPT_FOLDER}/dev-env-aws.sh
source ${SCRIPT_FOLDER}/dev-env-s3cmd.sh

garage status
garage key list
garage bucket list

dd if=/dev/urandom of=/tmp/garage.1.rnd bs=1k count=2 # No multipart, inline storage (< INLINE_THRESHOLD = 3072 bytes)
dd if=/dev/urandom of=/tmp/garage.2.rnd bs=1M count=5 # No multipart but file will be chunked
dd if=/dev/urandom of=/tmp/garage.3.rnd bs=1M count=10 # by default, AWS starts using multipart at 8MB

echo "s3 api testing..."
awsgrg --version
s3cmd --version
python3 --version

for idx in $(seq 1 3); do
  # AWS sends
  awsgrg cp "/tmp/garage.$idx.rnd" "s3://eprouvette/&+-é\"/garage.$idx.aws"

  awsgrg ls s3://eprouvette

  awsgrg cp "s3://eprouvette/&+-é\"/garage.$idx.aws" "/tmp/garage.$idx.dl"
  diff /tmp/garage.$idx.rnd /tmp/garage.$idx.dl
  rm /tmp/garage.$idx.dl

  s3grg get "s3://eprouvette/&+-é\"/garage.$idx.aws" "/tmp/garage.$idx.dl"
  diff /tmp/garage.$idx.rnd /tmp/garage.$idx.dl
  rm /tmp/garage.$idx.dl

  awsgrg rm "s3://eprouvette/&+-é\"/garage.$idx.aws"

  # S3CMD sends
  s3grg put "/tmp/garage.$idx.rnd" "s3://eprouvette/&+-é\"/garage.$idx.s3cmd"

  s3grg ls s3://eprouvette
	
  s3grg get "s3://eprouvette/&+-é\"/garage.$idx.s3cmd" "/tmp/garage.$idx.dl"
  diff /tmp/garage.$idx.rnd /tmp/garage.$idx.dl
  rm /tmp/garage.$idx.dl

  awsgrg cp "s3://eprouvette/&+-é\"/garage.$idx.s3cmd" "/tmp/garage.$idx.dl"
  diff /tmp/garage.$idx.rnd /tmp/garage.$idx.dl
  rm /tmp/garage.$idx.dl
	
  s3grg rm "s3://eprouvette/&+-é\"/garage.$idx.s3cmd"
done
rm /tmp/garage.{1,2,3}.rnd

echo "website testing"
echo "<h1>hello world</h1>" > /tmp/garage-index.html
awsgrg cp /tmp/garage-index.html s3://eprouvette/index.html
[ `curl -s -o /dev/null -w "%{http_code}" --header "Host: eprouvette.garage.tld"  http://127.0.0.1:3923/ ` == 404 ]
garage bucket website --allow eprouvette
[ `curl -s -o /dev/null -w "%{http_code}" --header "Host: eprouvette.garage.tld"  http://127.0.0.1:3923/ ` == 200 ]
garage bucket website --deny eprouvette
[ `curl -s -o /dev/null -w "%{http_code}" --header "Host: eprouvette.garage.tld"  http://127.0.0.1:3923/ ` == 404 ]
awsgrg rm s3://eprouvette/index.html
rm /tmp/garage-index.html

echo "teardown"
garage bucket deny --read --write eprouvette --key $AWS_ACCESS_KEY_ID
garage bucket delete --yes eprouvette
garage key delete --yes $AWS_ACCESS_KEY_ID

echo "success"
