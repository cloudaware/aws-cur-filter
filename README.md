Usage:
```
java -jar cur-filter/cur-filter-1.0-SNAPSHOT.jar "${REPORT_NAME}" "${REPORT_PREFIX}" "${INPUT_BUCKET}" "${OUTPUT_BUCKET}"  "${LINKED_ACCOUNT_IDS}"
```

Sample IAM Role Policy for EC2 Instance
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::inputBucket",
                "arn:aws:s3:::outputBucket"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::inputBucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::outputBucket/*"
            ]
        }
    ]
}
```



Sample user-data for EC2 instance, for this script folder `lib` and JAR file `cur-filter-1.0-SNAPSHOT.jar` should be placed at `s3://outputFolder/cur-filter/`
```
#! /bin/bash
INPUT_BUCKET="inputBucket"
OUTPUT_BUCKET="outputBucket"
REPORT_NAME="reportName"
REPORT_PREFIX="reportPrefix/reportName/"
LINKED_ACCOUNT_IDS="0000000000,11111111111"

sudo yum install -y java-1.8.0-openjdk

aws s3 sync s3://${OUTPUT_BUCKET}/cur-filter cur-filter

java -jar cur-filter/cur-filter-1.0-SNAPSHOT.jar "${REPORT_NAME}" "${REPORT_PREFIX}" "${INPUT_BUCKET}" "${OUTPUT_BUCKET}"  "${LINKED_ACCOUNT_IDS}"

#if needed add EC2 instance self terminating, for example use Shutdown behavior - Terminaton
shutdown -h now
```