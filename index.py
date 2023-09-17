from distutils.command.config import config
import imp
import secrets
import psycopg2
import boto3
import pandas as pd

# pip3 install sqlalchemy


from sqlalchemy import create_engine
from utils.helper import create_bucket
from configparser import ConfigParser
 
config = ConfigParser()
config.read('env')
 
region       = config['AWS']['region']
access_key   = config['AWS']['aws_access_key']
secret_key   = config['AWS']['aws_secret_access_key']

host            = config['DB_CRED']['host']
user            = config['DB_CRED']['username'] 
password        = config['DB_CRED']['password']
database        = config['DB_CRED']['database']


# region = 'us-west-2'
# bucket_name = "payminutewest2testerr"


# Step 1: Create S3 bucket in us-west-2 suing boto3
#create_bucket()



# Step 2: Extract from Database(Postgres) into Datalake (S3)

db_conn = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:5432/{database})


s3_path = "s3://{}/{}.csv"

for table in db_tables:
    query = f'SELECT * FROM {table}'
    df = pd.read_sql_query(query, db_conn)
    
    df.to_csv(
        s3_path.format(bucket_name, table)
        , index=False
        , storage_options={
            'key': access_key
            , 'secret': secret_key
        }
    )

