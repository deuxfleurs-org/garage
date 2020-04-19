#!/bin/bash

for FILE in $(find target/debug/deps); do 
	echo
	echo $FILE
	curl -v localhost:3900/$FILE -X PUT -H 'Host: garage' -H 'Content-Type: application/blob' --data-binary "@$FILE"
done

