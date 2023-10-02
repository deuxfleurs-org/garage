export AWS_ACCESS_KEY_ID=`cat /tmp/garage.s3 |cut -d' ' -f1`
export AWS_SECRET_ACCESS_KEY=`cat /tmp/garage.s3 |cut -d' ' -f2`
export AWS_DEFAULT_REGION='garage'
# FUTUREWORK: set AWS_ENDPOINT_URL instead, once nixpkgs bumps awscli to >=2.13.0.
function aws { command aws --endpoint-url http://127.0.0.1:3911 $@ ; }

aws --version
