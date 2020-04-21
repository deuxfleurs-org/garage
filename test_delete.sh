#!/bin/bash

for FILE in $(find target); do
	curl localhost:3900/$FILE -X DELETE -H 'Host: garage'
done

