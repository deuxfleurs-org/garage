#!/bin/bash
echo "-- check --"
nix-shell --attr rust --run "cargo fmt -- --check"

echo "-- build --"
nix-build --no-build-output --attr clippy.amd64 --argstr git_version $COMMIT

echo "-- unit tests --"
nix-build --no-build-output --attr test.amd64
./result/bin/garage_db-*
./result/bin/garage_api-*
./result/bin/garage_model-*
./result/bin/garage_rpc-*
./result/bin/garage_table-*
./result/bin/garage_util-*
./result/bin/garage_web-*
./result/bin/garage-*

echo "-- integration tests --"
./result/bin/integration-*

echo "-- smoke tests --"
rm result
nix-build --no-build-output --attr clippy.amd64 --argstr git_version $COMMIT
nix-shell --attr integration --run ./script/test-smoke.sh || (cat /tmp/garage.log; false)
