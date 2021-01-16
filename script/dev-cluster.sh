#!/bin/bash

set -e

SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:$PATH"
FANCYCOLORS=("41m" "42m" "44m" "45m" "100m" "104m")

export RUST_BACKTRACE=1 
export RUST_LOG=garage=info,garage_api=debug
MAIN_LABEL="\e[${FANCYCOLORS[0]}[main]\e[49m"

WHICH_GARAGE=$(which garage || exit 1)
echo -en "${MAIN_LABEL} Found garage at: ${WHICH_GARAGE}\n"

for count in $(seq 1 3); do
CONF_PATH="/tmp/config.$count.toml"
LABEL="\e[${FANCYCOLORS[$count]}[$count]\e[49m"

cat > $CONF_PATH <<EOF
block_size = 1048576			# objects are split in blocks of maximum this number of bytes
metadata_dir = "/tmp/garage-meta-$count"
data_dir = "/tmp/garage-data-$count"
rpc_bind_addr = "[::]:$((3900+$count))"		# the port other Garage nodes will use to talk to this node
bootstrap_peers = [
  "[::1]:3901",
  "[::1]:3902",
  "[::1]:3903"
]
max_concurrent_rpc_requests = 12
data_replication_factor = 3
meta_replication_factor = 3
meta_epidemic_fanout = 3

[s3_api]
api_bind_addr = "[::]:$((3910+$count))"	# the S3 API port, HTTP without TLS. Add a reverse proxy for the TLS part.
s3_region = "garage"				# set this to anything. S3 API calls will fail if they are not made against the region set here.

[s3_web]
bind_addr = "[::]:$((3920+$count))"
root_domain = ".garage.tld"
index = "index.html"
EOF

echo -en "$LABEL configuration written to $CONF_PATH\n"

(garage server -c /tmp/config.$count.toml 2>&1|while read r; do echo -en "$LABEL $r\n"; done) &
done

until garage status 2>&1|grep -q Healthy ; do 
	echo -en "${MAIN_LABEL} cluster starting...\n"
	sleep 1
done
echo -en "${MAIN_LABEL} cluster started\n"

wait
