mkdir -p /tmp/garage.cyberduck.home/.duck/profiles

DUCK_ACCESS_KEY=`cat /tmp/garage.s3 |cut -d' ' -f1`
DUCK_SECRET_KEY=`cat /tmp/garage.s3 |cut -d' ' -f2`

cat > /tmp/garage.cyberduck.home/.duck/credentials <<EOF
https\://$DUCK_ACCESS_KEY@127.0.0.1\:4443=$DUCK_SECRET_KEY
EOF

cat > /tmp/garage.cyberduck.home/.duck/profiles/garage.cyberduckprofile <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
    <dict>
        <key>Protocol</key>
        <string>s3</string>
        <key>Vendor</key>
        <string>garage</string>
        <key>Scheme</key>
        <string>https</string>
        <key>Description</key>
        <string>GarageS3</string>
        <key>Default Hostname</key>
        <string>127.0.0.1</string>
        <key>Default Port</key>
        <string>4443</string>
        <key>Hostname Configurable</key>
        <false/>
        <key>Port Configurable</key>
        <false/>
        <key>Username Configurable</key>
        <true/>
        <key>Username Placeholder</key>
        <string>Access Key ID (GK...)</string>
        <key>Password Placeholder</key>
        <string>Secret Key</string>
        <key>Properties</key>
        <array>
            <string>s3service.disable-dns-buckets=true</string>
        </array>
        <key>Region</key>
        <string>garage</string>
        <key>Regions</key>
        <array>
            <string>garage</string>
        </array>
    </dict>
</plist>
EOF

function duck { HOME=/tmp/garage.cyberduck.home/ command duck --username $DUCK_ACCESS_KEY $@ ; }

