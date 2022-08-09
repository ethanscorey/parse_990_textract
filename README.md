# Parsing 990 Textract Outputs on AWS Lambda
This repository contains all the code necessary to build a Docker image to deploy as an AWS Lambda function that parses the Textract outputs for 990 forms from 2008 to present.

### Usage

##### Deploying the code
1. To deploy this code in a Lambda function, first clone the repository and create a Docker image locally.
```
git clone https://github.com/ethanscorey/parse_990_textract.git
cd parse_990_textract
docker build -t parse_990_textract:latest .
```

2. Then, authenticate the Docker CLI to your Amazon ECR registry.
```
aws ecr get-login-password --region [INSERT_AWS_REGION_HERE] | docker login --username \
AWS --password-stdin [INSERT_AWS_ACCT_NO_HERE].dkr.ecr.us-east-1.amazonaws.com
```
3. Create a repository in Amazon ECR using the `create-repository` command.
```
aws ecr create-repository --repository-name parse_990_textract \
--image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
```
4. Tag your image to match the name of your repository, then push it to Amazon ECR using the `docker push` command:
```
docker tag parse_990_textract:latest [INSERT_AWS_ACCT_NO_HERE].dkr.ecr.[INSERT_AWS_REGION_HERE].amazonaws.com/parse_990_textract:latest
docker push [INSERT_AWS_ACCT_NO_HERE].dkr.ecr.[INSERT_AWS_REGION_HERE].amazonaws.com/parse_990_textract:latest
```
5. Create an AWS IAM role to ensure that your Lambda function has the appropriate permissions:
```
TKTK
```

6. Finally, create your Lambda function using the container. For best performance, provision the function with at least 512 MB of memory and a timeout of at least 45 seconds.
```
aws lambda create-function \
  --function-name parse_990_textract \
  --role [INSERT_ROLE_URI_HERE] \
  --code ImageUri=[INSERT_IMAGE_URI_HERE] \
  --timeout 45 \
  --memory-size 512 \
  --package-type Image
```

(You can also do all of these steps using the AWS console if you prefer.)

##### Running the code

The code is designed to work most effectively as part of an AWS Step Function state machine, in which it receives input values from other Lambda functions responsible for other steps in the scraping and parsing process. However, you can also invoke the Lambda function from using the AWS CLI as follows:
1. Create a JSON file with the relevant attributes set:
```
{
    "textract_job_id": "string",
    "pdf_key": "string",
    "bucket_name": "string",
    "ein": "number",
    "year": "number",
    "doc_type": "string",
    "table_name": "string"
}
```
Where each attribute represents the following:
| Attribute Name    | Description                                                                                                     |
|  ---------------- | --------------------------------------------------------------------------------------------------------------- |
| `textract_job_id` | The job ID for the Textract files you want to parse                                                             |
| `pdf_key`         | The object key for the original PDF file for the 990                                                            |
| `bucket_name`     | The name of the S3 bucket in which the Textract and PDF files are stored                                        |
| `ein`             | The EIN for the organization that filed the 990.                                                                |
| `year`            | The year in which the 990 was filed.                                                                            |
| `doc_type`        | The type of 990 document (e.g., 990, 990-T, 990-EZ, 990-F)                                                      |
| `table_name`      | The name of the database table to which you plan to save the results. (Optional if used outside Step Function.) |

2. Invoke the function from the AWS CLI.
```
aws lambda invoke \
  --function-name parse_990_textract
  --payload file://[PATH_TO_JSON_FILE]
  EIN_[EIN]_YEAR_[YEAR]_FORMTYPE_[FORMTYPE].json
```
(You can replace the name of the output file with whatever name you prefer.)