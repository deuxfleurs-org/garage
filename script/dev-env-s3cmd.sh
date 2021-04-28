ACCESS_KEY=`cat /tmp/garage.s3 |cut -d' ' -f1`
SECRET_KEY=`cat /tmp/garage.s3 |cut -d' ' -f2`

cat > /tmp/garage.s3cmd.cfg <<EOF
[default]
access_key = $ACCESS_KEY
secret_key = $SECRET_KEY
host_base = 127.0.0.1:3911
host_bucket = 127.0.0.1:3911
use_https = False
EOF

function s3cmd { command s3cmd --config=/tmp/garage.s3cmd.cfg $@ ; }
s3cmd --version
python3 --version
