#!/bin/bash

for FILE in $(find target/debug/deps); do 
	curl -v localhost:3900/$FILE -X DELETE -H 'Host: garage'
done

