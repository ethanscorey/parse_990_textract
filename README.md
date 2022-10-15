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
AWS --password-stdin [INSERT_AWS_ACCT_NO_HERE].dkr.ecr.[INSERT_AWS_REGION_HERE].amazonaws.com
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

5. Create an IAM service role with appropriate permissions for your Lambda function.
```
aws iam create-role \
  --role-name parse-990-textract-lambda-execution \
  --assume-role-policy-document '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}'
aws iam attach-role-policy \
  --role-name parse_990_textract_lambda_execution
  --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
aws iam attach-role-policy \
  --role-name parse_990_textract_lambda_execution
  --policy-arn arn:aws:iam::aws:policy/AmazonS3ObjectLambdaExecutionRolePolicy
```

6. Create your Lambda function using the container. For best performance, provision the function with at least 1024 MB of memory and a timeout of at least 360  seconds. If you want to parse the largest files, you may need to increase these thresholds further, but these parameters should work for about 99 percent of 990s.

```
aws lambda create-function \
  --function-name parse_990_textract \
  --role [INSERT_ROLE_URI_HERE] \
  --code ImageUri=[INSERT_AWS_ACCT_NO_HERE].dkr.ecr.[INSERT_AWS_REGION_HERE].amazonaws.com/parse_990_textract:latest
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
  EIN_[EIN]_YEAR_[YEAR]_FORMTYPE_[FORMTYPE]_OUTPUT.json
```
(You can replace the name of the output file with whatever name you prefer.)

### Building and Testing Locally
If you have the [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html) installed, you can also build and test the function locally. To do so, perform the following steps.

1. Run `sam build` to create the required build artifact.
2. Create an `events.json` file with the event data specified above.
3. Run `sam local invoke "Parse990TextractFunction" -e event.json`

And that's it! Please note that you may have some difficulties with Docker and/or SAM depending on
how you have your AWS credentials stored. If you're able to run Docker without root access, then SAM
should run fine as long as you have [setup your AWS
credentials](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-quickstart.html). If you
require root access to run Docker, then ensure that your AWS credentials are available in the root
environment.

### Running Outside of Lambda
You don't _need_ to run this code through Lambda. For large numbers of PDFs, it helps, because you
can run jobs in parallel without worrying about overheating your laptop. But for a smaller number of
PDFs, it's probably easier to just run the code outside of Lambda. To do that, you just need to wrap
the `handler` function as follows:
```python
from parse_990_textract import handler


bucket_name = "NAME_OF_YOUR_S3_BUCKET"
job_id = "TEXTRACT_JOB_ID_FOR_PDF"
pdf_key = "S3_KEY_FOR_ORIGINAL_PDF"


def parse_990(bucket_name: str, job_id: str, pdf_key: str) -> pd.DataFrame:
    event = {
        "bucket_name": bucket_name,
        "job_id": job_id,
        "pdf_key": pdf_key,
    }
    return handler(event, None)


filing_data = parse_990(bucket_name, job_id, pdf_key)["filing_data"]
```
 
### Extending the Parser
I only wrote the parser to handle the data I needed to obtain from the 990s I had. Specifically, I
parsed the Part I Summary and the Part IX Statement of Functional Expenses, as well as the optional
Schedule F for nonprofits with international operations. If you want to extract other data, you'll
have to modify/add files in the `parse_data` directory.

If the data you wish to extract is contained in a single field:
1. Identify the page on which the data appears. Choose a distinctive phrase that _only_ appears on
   that page. Then, in the file `parse_990_textract/parse.py`, add a function as follows:
   ```python
   def id_page_X(ocr_data):
       match_page = ocr_data.loc[
           ocr_data["Text"].str.contains(
               "REGEX TO MATCH THE PHRASE AND/OR SLIGHT VARIATIONS"
           ),
           "Page",
       ]
       return matching_page.iloc[0]
   ```
   Then, add `"Page X": id_page_X(ocr_data)` to the dictionary in the `find_pages` function. I
   should probably refactor this code to make adding new pages easier--sorry about that!
2. Identify landmarks on the page to create a bounding box around the data you wish to extract. For
   instance, the word "Domicile" in Item M has roughly the same left-bound as the number of voting members in
   Part I; the phrase "Number of voting" has roughly the same top-bound as the number of voting
   members; the right-most edge of the page is the farthest possible x-coordinate for the
   right-bound of the number of voting members; the top of the text "Number of individual..." marks
   the bottom boundary for the number of voting members. You should then inspect the Textract output
   to find the approximate coordinates for each of these landmarks (this will help in case an OCR
   error makes it difficult to find the landmark in another 990). Once you have this information,
   enter the following into the file `parse_data/990_roadmap.csv` for each landmark:
   - `landmark`: A human-readable name for the landmark -- e.g. "Item C" or "Part IX, Item 12"
   - `regex`: A regular expression to match the text in the landmark. You'll have to balance
     flexibility with specificity, because OCR errors mean that the text may not be identical on
     every 990. It's often easiest to pick the first few and last few characters in a phrase, e.g.
     `Name.+zation` for "Name of organization". This will take some trial and error!
   - `left_default`: The approximate coordinate for the left side of this landmark. Used as a
     fallback if the text isn't found.
   - `top_default`: The approximate coordinate for the top of this landmark. Used as a fallback if
     the text isn't found.
   - `x_tolerance`: The maximum distance away from the `left_default` coordinate that you expect the
     text to appear. Something between 0.1 and 0.2 is usually a good choice here, but you should
     experiment with what works best.
   - `y_tolerance`: The maximum distance away from the `top_default` coordinate that you expect the
     text to appear. Usually this will be a bit smaller than the `x_tolerance`, but your mileage may
     vary.
3. Add an entry to `parse_data/990_extractors.csv` with the following info:
   - `field_name`: The name of the field you wish to extract
   - `strategy`: Either `lines` or `words`. `words` is usually better, but you should experiment to
    see what works best. See [Textract documentation](https://docs.aws.amazon.com/textract/latest/dg/how-it-works-lines-words.html) for more.
   - `left`: The label for the landmark whose left coordinate is closest to the field you wish to
    extract.
   - `left_delta`: The distance to the right/left of the landmark for the left bound of your
     bounding box.
   - `top`: The label for the top bound of the field you wish to extract.
   - `top_delta`: Same as `left_delta`, but for distance up/down from the `top` landmark.
   - `right`: The label for the right bound
   - `right_delta`: You get the point...
   - `bottom`: Believe it or not, the label for the landmark for the bottom of the bounding box
   - `bottom_delta`: etc.
   - `page`: The name of the page that you included in the `find_pages` dictionary.
   - `regex`: A regex to extract the data from within the bounding box. The part of the regex that
     identifies the match should be in a capture group named `match`. For instance, if you just want
     to capture _all_ of the text within the box, try `(?P<match>.*)`. Your regex can be as complex
     as needed, e.g. `(?P<match>(?:www[. ] ?|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.  ?[^\s]{2,}|N[/a-zA-Z]+|[wW]+[. ]\w+[. ]\w{3}|\w+[. ]?(?:com|COM|org|ORG|edu|EDU))` for the organization's website in Part I.

And that's it! Now, all you have to do is submit a pull request to this repo so that others can
benefit from your hard work.

If the data you want is contained within a table and spread across multiple rows, then you'll need
to use the `TableExtractor` instead. To get that working:
1. Identify text that _only_ appears on pages that contain this table. Usually, you can pick
   something from the table header. Save that in your code as a variable (e.g.,
   `EXAMPLE_TABLE_HEADER = "fee fie foe fum"`).
2. Identify landmarks for the top, bottom, and each column in the table and add those landmarks to
   `parse_data/schedule_f_table_roadmap.csv`. I should probably rename that file to something more
   generic, whoops! Use the same process as you did above for adding landmarks for single fields,
   except you should also pick a unique name for the table and enter that in the `table` column.
   You'll use that name to refer to the landmarks later.
3. Fill in the `parse_data/schedule_f_table_extractors.csv` file along the same lines as the
   `parse_data/990_extractors.csv` file above. Ignore `index_col_left` and all of the columns that
   follow it; they will be removed in a future commit. Make sure to include the table name in the
   `table` column.
4. Fill in the `parse_data/schedule_f_row_extractors.csv` file in much the same way. Make sure to
   include the table name in the `table` column.

Once you've done all of that, you can extract data from the tables as follows:
```python
from parse_990_textract.bucket import open_df
from parse_990_textract.setup import load_extractor_df
from parse_990_textract.table import extract_table_data


HEADER = "INSERT PHRASE FROM STEP ONE HERE"
NAME = "INSERT NAME OF TABLE HERE"
textract_df = open_df("TEXTRACT_JOB_ID")
lines = textract_df.loc[textract_df["BlockType"] == "LINE"]
words = textract_df.loc[textract_df["BlockType"] == "WORD"]
tablemap_df = load_extractor_df("parse_data/schedule_f_table_roadmap.csv")
table_extractor_df = pd.read_csv("parse_data/schedule_f_table_extractors.csv")
row_extractor_df = pd.read_csv("parse_data/schedule_f_row_extractors.csv")
pages = lines.groupby("Page")
table_data = extract_table_data(
    pages,
    lines,
    words,
    HEADER,
    NAME,
    tablemap_df,
    table_extractor_df,
    row_extractor_df,
)
print(table_data)
```
