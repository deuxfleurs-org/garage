#!/usr/bin/env bash

: '
	This script tests part renumbering on an S3 remote (here configured for Minio).

	On Minio:

	The results confirm that if I upload parts with number 1, 4, 5 and 6,
	they are renumbered to 1, 2, 3 and 4 after CompleteMultipartUpload.
	Thus, specifying partNumber=4 on a GetObject/HeadObject should return
	information on the part I originally uploaded with part number

	On S3: not tested

	Sample output (on Minio):

		f07e1404cc527d494242824ded3a616b  part1
		78974cd4d0f622eb3426ea7cd22f5a1c  part4
		f9cc379f8baa61645558d9ba7e6351fa  part5
		1bd2383eebbac1f8e7143575ba5b1f4a  part6
		Upload ID: 6838b813-d0ca-400b-9d28-ec8b2b5cd004
		PART 1 ETag: "f07e1404cc527d494242824ded3a616b"
		PART 4 ETag: "78974cd4d0f622eb3426ea7cd22f5a1c"
		PART 5 ETag: "f9cc379f8baa61645558d9ba7e6351fa"
		PART 6 ETag: "1bd2383eebbac1f8e7143575ba5b1f4a"
		======================================== LIST ====
		{
		  "Parts": [
			{
			  "PartNumber": 1,
			  "LastModified": "2023-04-25T10:21:54.350000+00:00",
			  "ETag": "\"f07e1404cc527d494242824ded3a616b\"",
			  "Size": 20971520
			},
			{
			  "PartNumber": 4,
			  "LastModified": "2023-04-25T10:21:54.350000+00:00",
			  "ETag": "\"78974cd4d0f622eb3426ea7cd22f5a1c\"",
			  "Size": 20971520
			},
			{
			  "PartNumber": 5,
			  "LastModified": "2023-04-25T10:21:54.350000+00:00",
			  "ETag": "\"f9cc379f8baa61645558d9ba7e6351fa\"",
			  "Size": 20971520
			},
			{
			  "PartNumber": 6,
			  "LastModified": "2023-04-25T10:21:54.350000+00:00",
			  "ETag": "\"1bd2383eebbac1f8e7143575ba5b1f4a\"",
			  "Size": 20971520
			}
		  ],
		  "ChecksumAlgorithm": "",
		  "Initiator": {
			"ID": "02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4",
			"DisplayName": "02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4"
		  },
		  "Owner": {
			"DisplayName": "02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4",
			"ID": "02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4"
		  },
		  "StorageClass": "STANDARD"
		}
		======================================== COMPLETE ====
		{
			"Location": "http://localhost:9000/test/upload",
			"Bucket": "test",
			"Key": "upload",
			"ETag": "\"8e817c8ccd442f9a79c77b58fe808c43-4\""
		}
		======================================== LIST ====

		An error occurred (NoSuchUpload) when calling the ListParts operation: The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.
		======================================== GET PART 4 ====
		{
			"AcceptRanges": "bytes",
			"LastModified": "2023-04-25T10:21:59+00:00",
			"ContentLength": 20971520,
			"ETag": "\"8e817c8ccd442f9a79c77b58fe808c43-4\"",
			"ContentRange": "bytes 62914560-83886079/83886080",
			"ContentType": "binary/octet-stream",
			"Metadata": {},
			"PartsCount": 4
		}
		1bd2383eebbac1f8e7143575ba5b1f4a  get-part4

	
	Conclusions:

	- Parts are indeed renumbered with consecutive numbers
	- ListParts only applies to multipart uploads in progress,
	  it cannot be used once the multipart upload has been completed
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
	if [ -n "$PARTS" ]; then
		PARTS="$PARTS,"
	fi
	PARTS="$PARTS {\"ETag\":$ETAG,\"PartNumber\":$NUM}"
done

echo "======================================== LIST ===="
aws s3api list-parts --bucket test --key upload --upload-id "$UPLOAD" | jq

echo "======================================== COMPLETE ===="
echo "{\"Parts\":[$PARTS]}" > mpu
aws s3api complete-multipart-upload --multipart-upload file://mpu \
		--bucket test --key 'upload' --upload-id "$UPLOAD"

echo "======================================== LIST ===="
aws s3api list-parts --bucket test --key upload --upload-id "$UPLOAD" | jq

echo "======================================== GET PART 4 ===="
aws s3api get-object --bucket test --key upload --part-number 4 get-part4
md5sum get-part4
