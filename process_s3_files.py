

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


# def process_dataframe(
#     df: pandas.DataFrame,
#     df_columns: list,
#     filename: str,
# ):
#     for column in df.columns:
#         if not column in df_columns:
#             print(f'adding missing column: {column}')
#             df_columns.append(column)
#     for column in df_columns:
#         if not column in df.columns:
#             print(f'DataFrame is missing column with name {column}')
#             print(f'filename: {filename}')

def process_dataframe(
    df: pandas.DataFrame,
    filename: str,
    date: datetime,
):
    df['file_date'] = date
    return df
    
    
    

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
    
    minio_paginator = minio_s3.get_paginator('list_objects_v2')

    minio_bucket_name = 'land-registry-monthly-data-files'
    
    key_list = []
    
    for page in minio_paginator.paginate(Bucket=minio_bucket_name, Prefix='original-data'):
        for object in page['Contents']:
            key: str = object['Key']
            print(f'key={key}')
            key_list.append(key)
            
            
    def key_ends_with_select(key: str) -> bool:
        return key.endswith('.txt') or key.endswith('.zip') or key.endswith('.csv')
               
    key_list = (
        list(
            filter(
                lambda key: key_ends_with_select(key),
                key_list,
            )
        )
    )
    
    def key_to_date(key: str) -> datetime:
        key = key.rsplit('/', maxsplit=1)[1]
        day = key.split('_')[2]
        month = key.split('_')[3]
        year = key.split('_')[4]
        month = month_string_to_integer[month]
        return datetime(
            year=int(year),
            month=int(month),
            day=int(day),
        )
    
    key_dict = {
        key_to_date(key): key for key in key_list
    }
    
    key_dict = dict(sorted(key_dict.items()))
    
    # for key, value in key_dict.items():
    #     print(f'{key} {value}')
    # return
    
    #df_columns = []
    df_list = []
            
    for key_date, key in key_dict.items():
        #print(f'processing key: {key}, date: {key_date}')
    
        tmp_file_path = key.rsplit('/', maxsplit=1)[1]
        #print(tmp_file_path)
        minio_s3.download_file(minio_bucket_name, key, tmp_file_path)
        
        if tmp_file_path.endswith('.zip'):
            with ZipFile(tmp_file_path) as zip:
                
                day = tmp_file_path.split('_')[2]
                month_as_string = tmp_file_path.split('_')[3]
                year = tmp_file_path.split('_')[4]
                
                zip_internal_filename = f'PPMS_update_{day}_{month_as_string}_{year}_ew.txt'
                zip.extract(zip_internal_filename)

                try:
                    df = pandas.read_csv(zip_internal_filename, header=None)
                    #print(df)
                    if len(df.columns) != 16:
                        print(f'ERROR: file {tmp_file_path} has wrong number of columns: {len(df.columns)}')
                    if key_date.year <= 2015:
                        pass
                    else:
                        df_list.append(process_dataframe(df, tmp_file_path, key_date))
                except:
                    print(f'ERROR: file {tmp_file_path} is not readable')
                
                os.remove(zip_internal_filename)
        elif tmp_file_path.endswith('.txt'):
            print(f'skipping file {tmp_file_path}')
        elif tmp_file_path.endswith('.csv'):
            try:
                df = pandas.read_csv(tmp_file_path, header=None)
                #print(df)
                if len(df.columns) != 16:
                    print(f'ERROR: file {tmp_file_path} has wrong number of columns: {len(df.columns)}')
                if key_date.year <= 2015:
                    pass
                else:
                    df_list.append(process_dataframe(df, tmp_file_path, key_date))
            except:
                print(f'ERROR: file {tmp_file_path} is not readable')
        else:
            raise RuntimeError(f'error cant read {tmp_file_path}')
        
        os.remove(tmp_file_path)
        
    df = pandas.concat(df_list)
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
    df.to_csv(f'PPMS_update.csv', index=False)


if __name__ == '__main__':
    main()
    
    