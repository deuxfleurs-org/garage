
#!/bin/bash

for FILE in $(find target/debug/deps); do 
	SHA2=$(curl localhost:3900/$FILE -H 'Host: garage' 2>/dev/null | sha256sum | cut -d ' ' -f 1)
	SHA2REF=$(sha256sum $FILE | cut -d ' ' -f 1)
	if [ "$SHA2" = "$SHA2REF" ]; then
		echo "OK $FILE"
	else
		echo "!!!! ERROR $FILE !!!!"
	fi
done

