"""Python file to upload local documents to the cloud"""

import boto3
from botocore.exceptions import NoCredentialsError
import os

local_file_path = './project/starter/customer/landing/customer-1691348231425.json'
local_folder_customer = './project/starter/customer/landing'
local_folder_accelerometer = './project/starter/accelerometer/landing'
local_folder_step = './project/starter/step_trainer/landing'

bucket_name = 'diecadu-stedi-project'
s3_file_key = 'customer/landing/customer-1691348231425.json'
s3_file_acc = 'accelerometer/landing'
s3_file_cust = 'customer/landing'
s3_file_setp = 'step_trainer/landing'

#Correct keys for files
KEY = 'AKIAXYKJTRKDIGMY6YRG'
SECRET = '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'
REGION = 'us-east-1'

s3 = boto3.resource('s3',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )

def upload_files_in_folder_to_s3(s3, local_folder, bucket_name, s3_folder):
    try:
        for root, dirs, files in os.walk(local_folder):
            for file_name in files:
                local_file_path = os.path.join(root, file_name)
                s3_file_path = f"{s3_folder}/{file_name}"

                # Upload the file
                s3.upload_file(local_file_path, bucket_name, s3_file_path)

        print("Upload successful")
        return True

    except NoCredentialsError:
        print("Credentials not available")
        return False

try:
    s3 = boto3.client('s3', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name=REGION)
    # s3.upload_file(local_file_path, bucket_name, s3_file_key)
    upload_files_in_folder_to_s3(s3, local_folder_customer, bucket_name, s3_file_cust)
    upload_files_in_folder_to_s3(s3, local_folder_accelerometer, bucket_name, s3_file_acc)
    upload_files_in_folder_to_s3(s3, local_folder_step, bucket_name, s3_file_setp)
    print("Files uploaded")
except Exception as e:
    print(e)
