import json
import traceback
import pandas as pd
import numpy as np
import boto3
import awswrangler as wr
from collections.abc import Mapping, Iterable
from decimal import Decimal
from io import StringIO
from pprint import pprint

client = boto3.client('glue')
Bucket_role = 'arn:aws:iam::239126490696:role/dev-data-lake-cms-conform-eu-west-1-239126490696-writer'

def write_dataframe_to_csv_on_s3(dataframe, filename):
    """ Write a dataframe to a CSV on S3 """
    print("Writing {} records to {}".format(len(dataframe), filename))
     # Create buffer
    csv_buffer = StringIO()
    # Write dataframe to buffer
    dataframe.to_csv(csv_buffer, sep=",", index=False)
    # Create S3 object
    boto3_session = get_session_for(Bucket_role)
    client = boto3_session.client('s3')
    #s3_resource = boto3_session.resource("s3")
    # Write buffer to S3 object
    #s3_resource.Object("dev-data-lake-cms-conform-eu-west-1-239126490696", filename).put(Body=csv_buffer.getvalue())
    client.put_object(Bucket='dev-data-lake-cms-conform-eu-west-1-239126490696', Key=filename,ACL="bucket-owner-full-control")
    
def to_parquet(df, bucket, prefix, database, table):
    boto3_session = get_session_for(Bucket_role)
    databases = wr.catalog.databases(boto3_session=boto3_session)
    if database not in databases.values:
        wr.catalog.create_database(database)
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/{prefix}/",
        dataset=True,
        database=database,
        table=table,
        mode="append",
        use_threads=True,
        boto3_session=boto3_session,
        s3_additional_kwargs={"ACL": "bucket-owner-full-control"},
        catalog_versioning=True,
        schema_evolution=True
    )
    
def dynamodb_to_pandas_dataframe(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    response = table.scan()
    
    print(response['Items'])
    
    df = pd.json_normalize(
        response['Items'],
        record_path = ['data'],
        meta = ['studycode','viewID#vendorshortname'])
    
    all_columns = list(df)
    df[all_columns] = df[all_columns].astype('string')
    
    lis = []
    
    for x in df.columns:
        lis.append(x)
    
    if 'SUBJECT' in lis:
        df.rename(columns = {'SUBJECT':'Subject_SAS'}, inplace = True)
    if 'Visit' in lis:
        df.rename(columns = {'Visit':'Visit_ecoA'}, inplace = True)
        
      
    print(df)
    print(df.columns)
    
    df.to_csv(r'D:\data\cms\columns_output.csv')
    
    return df
    
def get_session_for(role):
    acct_b = boto3.client('sts').assume_role(RoleArn=role, RoleSessionName="cross_acct_lambda")
    print(acct_b)
    session = boto3.Session(
        aws_access_key_id=acct_b['Credentials']['AccessKeyId'],
        aws_secret_access_key=acct_b['Credentials']['SecretAccessKey'],
        aws_session_token=acct_b['Credentials']['SessionToken'],
    )
    return session
            
if __name__ == "__main__":
    #Step1 Loading DynamoDB data into S3:
    table_name = "dev-ofr-datahub-cms-clinical-study-consolidation"
    bucket = "dev-data-lake-cms-conform-eu-west-1-239126490696"
    prefix = "dynamodb_data/studyconsolidationnorm"
    database = "dev_data_lake_cms_conform_glue_database"
    table = "study_consolidation_normalized"
    
    df = dynamodb_to_pandas_dataframe(table_name)
    #write_dataframe_to_csv_on_s3(df,prefix)
    to_parquet(df,bucket,prefix,database,table)