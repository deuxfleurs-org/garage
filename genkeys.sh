#!/bin/bash

set -xe

cd $(dirname $0)

mkdir -p pki
cd pki

if [ ! -f garage-ca.key ]; then
    echo "Generating Garage CA keys..."
	openssl genrsa -out garage-ca.key 4096
	openssl req -x509 -new -key garage-ca.key -subj "/C=FR/O=Garage" -days 3650 -out garage-ca.crt
fi

if [ ! -f garage.key ]; then
    echo "Generating Garage agent keys..."
	openssl genrsa -out garage.key 4096
	openssl req -new -sha256 -key garage.key -subj "/C=FR/O=Garage/CN=*" -out garage.csr
	openssl req -in garage.csr -noout -text
	openssl x509 -req -in garage.csr \
		-CA garage-ca.crt -CAkey garage-ca.key -CAcreateserial \
		-out garage.crt -days 365 -sha256
	rm garage.csr
fi

if [ ! -f garage-client.key ]; then
	echo "Generating Garage client key..."
	openssl genrsa -out garage-client.key 4096
	openssl req -new -sha256 -key garage-client.key -subj "/C=FR/O=Garage" -out garage-client.csr
	openssl req -in garage-client.csr -noout -text
	openssl x509 -req -in garage-client.csr \
		-CA garage-ca.crt -CAkey garage-ca.key -CAcreateserial \
		-out garage-client.crt -days 365 -sha256
	rm garage-client.csr
fi
