#!/usr/bin/env bash

: '
	This script tests whether uploaded parts can be skipped in a
	CompleteMultipartUpoad

	On Minio: yes, parts can be skipped

	On S3: not tested

	Sample output (on Minio):

		f23911bcd1230f5ebe8887cbf5bc396e  part1
		a2657143167eaf647c40473e78a091dc  part4
		72f72c02c5163bc81024b28ac818c5e0  part5
		e29cf500d20498218904b8df8806caa2  part6
		Upload ID: e8fe7b83-9800-46fb-ae90-9d7ccd42fe76
		PART 1 ETag: "f23911bcd1230f5ebe8887cbf5bc396e"
		PART 4 ETag: "a2657143167eaf647c40473e78a091dc"
		PART 5 ETag: "72f72c02c5163bc81024b28ac818c5e0"
		PART 6 ETag: "e29cf500d20498218904b8df8806caa2"
		======================================== COMPLETE ====
		{
			"Location": "http://localhost:9000/test/upload",
			"Bucket": "test",
			"Key": "upload",
			"ETag": "\"48246e44d4b38bdc2f3c10ee25b1af17-3\""
		}
		======================================== GET FULL ====
		{
			"AcceptRanges": "bytes",
			"LastModified": "2023-04-25T10:54:35+00:00",
			"ContentLength": 31457280,
			"ETag": "\"48246e44d4b38bdc2f3c10ee25b1af17-3\"",
			"ContentType": "binary/octet-stream",
			"Metadata": {}
		}
		97fb904da7ad310699a6afab0eb6e061  get-full
		97fb904da7ad310699a6afab0eb6e061  -
		======================================== GET PART 3 ====
		{
			"AcceptRanges": "bytes",
			"LastModified": "2023-04-25T10:54:35+00:00",
			"ContentLength": 10485760,
			"ETag": "\"48246e44d4b38bdc2f3c10ee25b1af17-3\"",
			"ContentRange": "bytes 20971520-31457279/31457280",
			"ContentType": "binary/octet-stream",
			"Metadata": {},
			"PartsCount": 3
		}
		e29cf500d20498218904b8df8806caa2  get-part3

	Conclusions:

	- Skipping a part in a CompleteMultipartUpoad call is OK
	- The part is simply not included in the stored object
	- Sequential part renumbering counts only non-skipped parts
'

export AWS_ACCESS_KEY_ID=1D8Pk2k4oQSoh1BU
export AWS_SECRET_ACCESS_KEY=4B46SR8U7FUgY0raB8Zuxg1NLyLTvbNV

function aws { command aws --endpoint-url http://localhost:9000 $@ ; }

aws --version

aws s3 mb s3://test

for NUM in 1 4 5 6; do
	dd if=/dev/urandom of=part$NUM bs=1M count=10
done
md5sum part*

UPLOAD=$(aws s3api create-multipart-upload --bucket test --key 'upload' | jq -r ".UploadId")
echo "Upload ID: $UPLOAD"

PARTS=""

for NUM in 1 4 5 6; do
	ETAG=$(aws s3api upload-part --bucket test --key 'upload' --part-number $NUM \
		--body "part$NUM" --upload-id "$UPLOAD" | jq -r ".ETag")
	echo "PART $NUM ETag: $ETAG"
	if [ "$NUM" != "5" ]; then
		if [ -n "$PARTS" ]; then
			PARTS="$PARTS,"
		fi
		PARTS="$PARTS {\"ETag\":$ETAG,\"PartNumber\":$NUM}"
	fi
done

echo "======================================== COMPLETE ===="
echo "{\"Parts\":[$PARTS]}" > mpu
aws s3api complete-multipart-upload --multipart-upload file://mpu \
		--bucket test --key 'upload' --upload-id "$UPLOAD"

echo "======================================== GET FULL ===="
aws s3api get-object --bucket test --key upload get-full
md5sum get-full
cat part1 part4 part6 | md5sum

echo "======================================== GET PART 3 ===="
aws s3api get-object --bucket test --key upload --part-number 3 get-part3
md5sum get-part3
