# s3-key-validations

## How to execute
1. Create a file containing key's to be verfied in S3 under the workspace folder with name key-data
2. Make sure the user has AWS account and access to S3 along with credentials setup in the system where the script is executed.
3. Navigate to the src/ and run the python script(Make sure requirements file is executed)
4. Results will be in resultscsv file and contains only key's that are not present in S3
