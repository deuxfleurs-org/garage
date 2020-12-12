#!/bin/bash

set -ex

killall -9 garage || echo "garage is not running"
rm -rf /tmp/garage*
rm -rf /tmp/config.*.toml
