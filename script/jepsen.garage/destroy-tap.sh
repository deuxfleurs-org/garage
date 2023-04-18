#!/usr/bin/env bash

USER=$(whoami)

for NODE in 1 2 3 4 5; do
	sudo ip link delete microvm-n$NODE
done


