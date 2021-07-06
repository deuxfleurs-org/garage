#!/bin/bash

set -xe

cd $(dirname $0)

mkdir -p pki
cd pki

# Create a certificate authority that both the client side and the server side of
# the RPC protocol will use to authenticate the other side.
if [ ! -f garage-ca.key ]; then
    echo "Generating Garage CA keys..."
	openssl genpkey -algorithm ED25519 -out garage-ca.key
	openssl req -x509 -new -nodes -key garage-ca.key -sha256 -days 3650 -out garage-ca.crt -subj "/C=FR/O=Garage"
fi


# Generate a certificate that can be used either as a server certificate
# or a client certificate. This is what the RPC client and server will use
# to prove that they are authenticated by the CA.
if [ ! -f garage.crt ]; then
	echo "Generating Garage agent keys..."
	if [ ! -f garage.key ]; then
		openssl genpkey -algorithm ED25519 -out garage.key
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

# Client-only certificate used for the CLI
if [ ! -f garage-client.crt ]; then
	echo "Generating Garage client keys..."
	if [ ! -f garage-client.key ]; then
		openssl genpkey -algorithm ED25519 -out garage-client.key
	fi
	openssl req -new -sha256 -key garage-client.key -subj "/C=FR/O=Garage" \
		-out garage-client.csr
	openssl req -in garage-client.csr -noout -text
	openssl x509 -req -in garage-client.csr \
		-extensions v3_req \
		-extfile <(cat <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = FR
O = Garage

[v3_req]
keyUsage = keyEncipherment, dataEncipherment
extendedKeyUsage = clientAuth
EOF
) \
		-CA garage-ca.crt -CAkey garage-ca.key -CAcreateserial \
		-out garage-client.crt -days 365
fi
