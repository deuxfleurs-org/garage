export AWS_ACCESS_KEY_ID=`cat /tmp/garage.s3 |cut -d' ' -f1`
export AWS_SECRET_ACCESS_KEY=`cat /tmp/garage.s3 |cut -d' ' -f2`
export AWS_DEFAULT_REGION='garage'
export WINSCP_URL="s3://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@127.0.0.1:4443 -certificate=* -rawsettings S3DefaultRegion=garage S3UrlStyle=1"
