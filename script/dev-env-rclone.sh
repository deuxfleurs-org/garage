ACCESS_KEY=`cat /tmp/garage.s3 |cut -d' ' -f1`
SECRET_KEY=`cat /tmp/garage.s3 |cut -d' ' -f2`

cat > /tmp/garage.rclone.conf <<EOF
[garage]
type = s3
provider = Other
env_auth = false
access_key_id = $ACCESS_KEY
secret_access_key = $SECRET_KEY
endpoint = http://127.0.0.1:3911
bucket_acl = private
force_path_style = true
region = garage
no_check_bucket = true
EOF
# It seems that region is mandatory as rclone does not support redirection


function rclone { command rclone --config /tmp/garage.rclone.conf $@ ; }
rclone --version
