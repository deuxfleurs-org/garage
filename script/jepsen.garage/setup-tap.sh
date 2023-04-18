#!/usr/bin/env bash

USER=$(whoami)

for NODE in 1 2 3 4 5; do
	sudo ip tuntap add microvm-n$NODE mode tap user $USER
	sudo ip addr add dev microvm-n$NODE 10.$NODE.0.1
done


