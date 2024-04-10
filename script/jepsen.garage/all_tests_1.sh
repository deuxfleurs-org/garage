#!/usr/bin/env bash

set -x

#for ppatch in task3c task3a tsfix2; do
for ppatch in v093 v1rc1; do
	#for psc in c cp cdp r pr cpr dpr; do
	for ptsk in reg2 set2; do
		for psc in c cp cdp r pr cpr dpr; do
			for irun in $(seq 10); do
				lein run test --nodes-file nodes.vagrant \
					--time-limit 60 --rate 100  --concurrency 100 --ops-per-key 100 \
					--workload $ptsk --patch $ppatch --scenario $psc
			done
		done
	done
done
