#!/usr/bin/env bash

set -e

if [ "$#" -ne 1 ]; then
    echo "[fail] usage: $0 binary"
    exit 2
fi

if [ ! -x "$1" ]; then
	echo "[fail] $1 does not exist or is not an executable"
	exit 1
fi

if file "$1" | grep 'dynamically linked' 2>&1; then
    echo "[fail] $1 is dynamic"
    exit 1
fi
echo "[ok] $1 is probably static"
