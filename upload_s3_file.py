

import os
MINIOLOCAL_S3_ACCESS_KEY_ID = os.environ['MINIOLOCAL_S3_ACCESS_KEY_ID']
MINIOLOCAL_S3_SECRET_ACCESS_KEY = os.environ['MINIOLOCAL_S3_SECRET_ACCESS_KEY']


import boto3
import boto3.session
from botocore.config import Config
import pandas
from zipfile import ZipFile
from lib import month_string_to_integer
from datetime import datetime


def main():
    
    minio_session = boto3.Session(
        aws_access_key_id=MINIOLOCAL_S3_ACCESS_KEY_ID,
        aws_secret_access_key=MINIOLOCAL_S3_SECRET_ACCESS_KEY,
    )
    
    minio_s3 = minio_session.client(
        's3',
        endpoint_url='http://192.168.0.120:9000',
        config=Config(signature_version='s3v4'),
        use_ssl=False,
    )
    
    minio_bucket_name = 'land-registry-monthly-data-files'
    
    df = pandas.read_csv('PPMS_update.csv', header=None)
    
    columns = [
        'transaction_unique_id',
        'price',
        'transaction_date',
        'postcode',
        'property_type',
        'new_tag',
        'lease',
        'primary_address_object_name',
        'secondary_address_object_name',
        'street',
        'locality',
        'town_city',
        'district',
        'county',
        'ppd_cat',
        'record_status',
        'file_date',
    ]
    df.columns = columns
    
    df.to_csv('PPMS_update_with_header.csv', index=False)
    
    minio_s3.upload_file('PPMS_update_with_header.csv', minio_bucket_name, 'PPMS_update_with_header.csv')
    minio_s3.upload_file('PPMS_update.csv', minio_bucket_name, 'PPMS_update.csv')
        

if __name__ == '__main__':
    main()
    
    