import boto3
from botocore.exceptions import NoCredentialsError

local_file_path = './customer/landing/customer-1691348231425.json'
bucket_name = 'diecadu-stedi-project'
s3_file_key = 'customer/landing/file.txt'

s3 = boto3.client('s3')
s3.upload_file(local_file, bucket_name, s3_file)
