#!/usr/bin/env bash

set -ex

killall -9 garage || echo "garage is not running"
killall -9 socat || echo "socat is not running"
rm -rf /tmp/garage*
rm -rf /tmp/config.*.toml
