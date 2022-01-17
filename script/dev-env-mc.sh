ACCESS_KEY=`cat /tmp/garage.s3 |cut -d' ' -f1`
SECRET_KEY=`cat /tmp/garage.s3 |cut -d' ' -f2`

mkdir -p /tmp/garage.mc/certs/CAs

cat > /tmp/garage.mc/config.json <<EOF
{
  "version": "10",
  "aliases": {
    "garage": {
      "url": "http://127.0.0.1:3911",
      "accessKey": "$ACCESS_KEY",
      "secretKey": "$SECRET_KEY",
      "api": "S3v4",
      "path": "auto"
    }
  }
}
EOF

function mc { command mc --insecure --config-dir /tmp/garage.mc $@ ; }
mc --version
