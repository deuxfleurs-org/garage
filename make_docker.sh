#!/bin/sh

BIN=target/release/garage
DOCKER=lxpz/garage_amd64

TAG=$1
if [ -z "$1" ]; then
	echo "Usage: $0 <tag>"
	exit 1
fi

RUSTFLAGS="-C link-arg=-fuse-ld=lld -C target-cpu=x86-64 -C target-feature=+sse2" cargo build --release --no-default-features
cp $BIN $BIN.stripped
strip $BIN.stripped

docker pull archlinux:latest
docker build -t $DOCKER:$TAG .
docker push $DOCKER:$TAG
docker tag $DOCKER:$TAG $DOCKER:latest
docker push $DOCKER:latest
