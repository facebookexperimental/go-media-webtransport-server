# Get base 64 of scert cmd
certbase64=$(eval "openssl x509 -pubkey -noout -in ../certs/certificate.pem | openssl rsa -pubin -outform der | openssl dgst -sha256 -binary | base64")

/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --user-data-dir="/tmp/chrome_dev_session" --enable-features=SharedArrayBuffer --origin-to-force-quic-on=localhost:4433  --ignore-certificate-errors-spki-list=$certbase64