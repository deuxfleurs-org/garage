#!/bin/bash

set -ex

SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:$PATH"

sleep 5
until garage status 2>&1|grep -q Healthy ; do 
	echo "cluster starting..."
	sleep 1
done

garage status \
	| grep UNCONFIGURED \
	| grep -Po '^[0-9a-f]+' \
	| while read id; do 
	  garage node configure -d dc1 -n 100 $id
	done

