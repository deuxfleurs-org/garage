#!/usr/bin/env bash

set -e

SCRIPT_FOLDER="`dirname \"$0\"`"
REPO_FOLDER="${SCRIPT_FOLDER}/../"
GARAGE_DEBUG="${REPO_FOLDER}/target/debug/"
GARAGE_RELEASE="${REPO_FOLDER}/target/release/"
NIX_RELEASE="${REPO_FOLDER}/result/bin/"
PATH="${GARAGE_DEBUG}:${GARAGE_RELEASE}:${NIX_RELEASE}:$PATH"
FANCYCOLORS=("41m" "42m" "44m" "45m" "100m" "104m")

export RUST_BACKTRACE=1 
export RUST_LOG=garage=info,garage_api_common=debug,garage_api_s3=debug
MAIN_LABEL="\e[${FANCYCOLORS[0]}[main]\e[49m"

if [ -z "$GARAGE_BIN" ]; then
	GARAGE_BIN=$(which garage || exit 1)
	echo -en "${MAIN_LABEL} Found garage at: ${GARAGE_BIN}\n"
else
	echo -en "${MAIN_LABEL} Using garage binary at: ${GARAGE_BIN}\n"
fi
$GARAGE_BIN --version

NETWORK_SECRET="$(openssl rand -hex 32)"


# <<<<<<<<< BEGIN FOR LOOP ON NODES
for count in $(seq 1 3); do
CONF_PATH="/tmp/config.$count.toml"
LABEL="\e[${FANCYCOLORS[$count]}[$count]\e[49m"

if [ "$GARAGE_OLDVER" == "v08" ]; then
	REPLICATION_MODE="replication_mode = \"3\""
else
	REPLICATION_MODE="replication_factor = 3"
fi

cat > $CONF_PATH <<EOF
block_size = 1048576			# objects are split in blocks of maximum this number of bytes
metadata_dir = "/tmp/garage-meta-$count"
db_engine = "lmdb"
data_dir = "/tmp/garage-data-$count"
rpc_bind_addr = "0.0.0.0:$((3900+$count))"		# the port other Garage nodes will use to talk to this node
rpc_public_addr = "127.0.0.1:$((3900+$count))"
bootstrap_peers = []
$REPLICATION_MODE
rpc_secret = "$NETWORK_SECRET"

[s3_api]
api_bind_addr = "0.0.0.0:$((3910+$count))"	# the S3 API port, HTTP without TLS. Add a reverse proxy for the TLS part.
s3_region = "garage"				# set this to anything. S3 API calls will fail if they are not made against the region set here.
root_domain = ".s3.garage.localhost"

[s3_web]
bind_addr = "0.0.0.0:$((3920+$count))"
root_domain = ".web.garage.localhost"
index = "index.html"

[admin]
api_bind_addr = "0.0.0.0:$((9900+$count))"
EOF

echo -en "$LABEL configuration written to $CONF_PATH\n"

($GARAGE_BIN -c /tmp/config.$count.toml server 2>&1|while read r; do echo -en "$LABEL $r\n"; done) &
done
# >>>>>>>>>>>>>>>> END FOR LOOP ON NODES

if [ -z "$SKIP_HTTPS" ]; then
  echo -en "$LABEL Starting dummy HTTPS reverse proxy\n"
  mkdir -p /tmp/garagessl
  openssl req \
    -new \
    -x509 \
    -keyout /tmp/garagessl/test.key \
    -out /tmp/garagessl/test.crt \
    -nodes  \
    -subj "/C=XX/ST=XX/L=XX/O=XX/OU=XX/CN=localhost/emailAddress=X@X.XX" \
    -addext "subjectAltName = DNS:localhost, IP:127.0.0.1"
  cat /tmp/garagessl/test.key /tmp/garagessl/test.crt > /tmp/garagessl/test.pem
  socat openssl-listen:4443,reuseaddr,fork,cert=/tmp/garagessl/test.pem,verify=0 tcp4-connect:localhost:3911 &
fi

sleep 3
# Establish connections between nodes
for count in $(seq 1 3); do
	NODE=$($GARAGE_BIN -c /tmp/config.$count.toml node id -q)
	for count2 in $(seq 1 3); do
		$GARAGE_BIN -c /tmp/config.$count2.toml node connect $NODE
	done
done

RETRY=120
until $GARAGE_BIN -c /tmp/config.1.toml status 2>&1|grep -q HEALTHY ; do 
  (( RETRY-- ))
  if (( RETRY <= 0 )); then 
    echo -en "${MAIN_LABEL} Garage did not start"
    exit 1
  fi
	echo -en "${MAIN_LABEL} cluster starting...\n"
	sleep 1
done

echo -en "${MAIN_LABEL} cluster started\n"

wait
