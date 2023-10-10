#!/usr/bin/env bash

set -ex

SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
NIX_RELEASE="${REPO_FOLDER}/result/bin/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:${NIX_RELEASE}:$PATH"

if [ -z "$GARAGE_BIN" ]; then
	GARAGE_BIN=$(which garage || exit 1)
	echo -en "Found garage at: ${GARAGE_BIN}\n"
else
	echo -en "Using garage binary at: ${GARAGE_BIN}\n"
fi
$GARAGE_BIN --version

sleep 5
RETRY=120
until $GARAGE_BIN -c /tmp/config.1.toml status 2>&1|grep -q HEALTHY ; do
  (( RETRY-- ))
  if (( RETRY <= 0 )); then
    echo "garage did not start in time, failing."
    exit 1
  fi
	echo "cluster starting..."
	sleep 1
done

if [ "$GARAGE_08" = "1" ]; then
	$GARAGE_BIN -c /tmp/config.1.toml status \
		| grep 'NO ROLE' \
		| grep -Po '^[0-9a-f]+' \
		| while read id; do
		  $GARAGE_BIN -c /tmp/config.1.toml layout assign $id -z dc1 -c 1
		done
else
	$GARAGE_BIN -c /tmp/config.1.toml status \
		| grep 'NO ROLE' \
		| grep -Po '^[0-9a-f]+' \
		| while read id; do
		  $GARAGE_BIN -c /tmp/config.1.toml layout assign $id -z dc1 -c 1G
		done
fi

$GARAGE_BIN -c /tmp/config.1.toml layout apply --version 1
