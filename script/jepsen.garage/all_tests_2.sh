#!/usr/bin/env bash

set -x

#for ppatch in task3c tsfix2; do
for ppatch in tsfix2; do
	for psc in cdp r pr cpr dpr; do
		for ptsk in set1; do
			for irun in $(seq 10); do
				lein run test --nodes-file nodes2.vagrant \
					--time-limit 60 --rate 100  --concurrency 100 --ops-per-key 100 \
					--workload $ptsk --patch $ppatch --scenario $psc
			done
		done
	done
done
