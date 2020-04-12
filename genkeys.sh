#!/bin/bash

set -xe

cd $(dirname $0)

mkdir -p pki
cd pki

if [ ! -f garage-ca.key ]; then
    echo "Generating Garage CA keys..."
	openssl genrsa -out garage-ca.key 4096
	openssl req -x509 -new -nodes -key garage-ca.key -sha256 -days 3650 -out garage-ca.crt -subj "/C=FR/O=Garage"
fi


if [ ! -f garage.crt ]; then
	echo "Generating Garage agent keys..."
	if [ ! -f garage.key ]; then
		openssl genrsa -out garage.key 4096
	fi
	openssl req -new -sha256 -key garage.key -subj "/C=FR/O=Garage/CN=garage" \
		-out garage.csr
	openssl req -in garage.csr -noout -text
	openssl x509 -req -in garage.csr \
		-extensions v3_req \
		-extfile <(cat <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = FR
O = Garage
CN = garage

[v3_req]
keyUsage = keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = garage
EOF
) \
		-CA garage-ca.crt -CAkey garage-ca.key -CAcreateserial \
		-out garage.crt -days 365
fi
