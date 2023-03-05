#!/usr/bin/env bash

# Create certs dir if it does not exists
mkdir -p ../certs

# Generate publi - private key 
openssl req -newkey rsa:2048 -nodes -keyout ../certs/certificate.key -x509 -out ../certs/certificate.pem -subj '/CN=Test Certificate' -addext "subjectAltName = DNS:localhost"
