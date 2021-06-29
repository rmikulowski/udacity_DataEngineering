import configparser
import boto3
import io
import pandas as pd

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

def create_folder_s3(name,
                     access_user = config['IAM_ROLE']['ACCESS_USER'], 
                     access_key = config['IAM_ROLE']['ACCESS_KEY']):
    """
    """
    client = boto3.client('s3',
                          aws_access_key_id=access_user,
                          aws_secret_access_key=access_key)

    response = client.put_object(
            Bucket=config['IAM_ROLE']['ARN_MY_S3'],
            Body='',
            Key=f'{name}/'
            )
    
def save_csv_in_s3(dataframe, s3_client, folder_path, csv_name, my_bucket = config['IAM_ROLE']['ARN_MY_S3']):
    """
    """
    with io.StringIO() as csv_buffer:
        dataframe.to_csv(csv_buffer, index=False, header = False)

        response = s3_client.put_object(
            Bucket=my_bucket, Key=f"{folder_path}/{csv_name}.csv", Body=csv_buffer.getvalue()
        )