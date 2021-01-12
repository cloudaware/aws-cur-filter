# aws-cur-filter

NOTE: Dependencies contains `parquet-hadoop:1.10.1-ca-SNAPSHOT` this version from `parquet-mr` fork https://github.com/cloudaware/parquet-mr/tree/apache-parquet-1.10.1-ca

NOTE: at 1.0.2 version build process was change to fat jar, so before an update clean `s3://outputFolder/aws-cur-filter/` 

Usage:
```
java -jar aws-cur-filter/aws-cur-filter-1.0.3-jar-with-dependencies.jar "${REPORT_NAME}" "${REPORT_PREFIX}" "${INPUT_BUCKET}" "${OUTPUT_BUCKET}"  "${LINKED_ACCOUNT_IDS}" ["periodPrefix"]
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



Sample user-data for EC2 instance, for this script folder JAR file `aws-cur-filter-1.0.3-jar-with-dependencies.jar` should be placed at `s3://outputFolder/aws-cur-filter/`
```
#! /bin/bash
INPUT_BUCKET="inputBucket"
OUTPUT_BUCKET="outputBucket"
REPORT_NAME="reportName"
REPORT_PREFIX="reportPrefix/reportName/"
LINKED_ACCOUNT_IDS="0000000000,11111111111"

sudo yum install -y java-1.8.0-openjdk

aws s3 sync s3://${OUTPUT_BUCKET}/aws-cur-filter aws-cur-filter

java -jar aws-cur-filter/aws-cur-filter-1.0.3-jar-with-dependencies.jar "${REPORT_NAME}" "${REPORT_PREFIX}" "${INPUT_BUCKET}" "${OUTPUT_BUCKET}"  "${LINKED_ACCOUNT_IDS}"

#if needed add EC2 instance self terminating, for example use Shutdown behavior - Terminaton
shutdown -h now
```

# Changelog

* 1.0.3 - Added generating new AssemblyId (inputAssemblyId + linkedAccountIds) at output Manifest file. With change of Linked Account IDs  all output periods will be processed again.