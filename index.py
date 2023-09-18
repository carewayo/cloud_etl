from distutils.command.config import config
import imp
import secrets
from sqlite3 import Cursor
import psycopg2
import boto3
import pandas as pd

# pip3 install sqlalchemy




from sqlalchemy import create_engine
from utils.helper import create_bucket, connect_to_dwh
from configparser import ConfigParser
from sql_statements.create import raw_data_tables
from utils.constants import db_tables



config = ConfigParser()
config.read('.env')



# Access the 'region' key in the 'AWS' section


region = config['AWS']['region']
access_key = config['AWS']['aws_access_key']
secret_key = config['AWS']['aws_secret_access_key']
bucket = config['AWS']['bucketname']

host = config['DB_CRED']['host']
user = config['DB_CRED']['username'] 
password = config['DB_CRED']['password']
database = config['DB_CRED']['database']


dwh_host = config['DWH']['host']
dwh_user = config['DWH']['username'] 
dwh_password = config['DWH']['password']
dwh_database = config['DWH']['database']
dwh_role = config['DWH']['role']


bucket_name = "payminutewest2testerr"


# Step 1: Create S3 bucket in us-west-2 suing boto3
#create_bucket()



# Step 2: Extract from Database(Postgres) into Datalake (S3)

# db_conn = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:5432/{database})

s3_path = "s3://{}/{}.csv"

# for table in db_tables:
#     query = f'SELECT * FROM {table}'
#     df = pd.read_sql_query(query, db_conn)
    
#     df.to_csv(
#         s3_path.format(bucket_name, table)
#         , index=False
#         , storage_options={
#             'key': access_key
#             , 'secret': secret_key
#         }
#     )



# Step 3: Create the Raw schema in DWH
conn_details = {
    'host':dwh_host
    , 'user':dwh_user
    , 'password':dwh_password
    , 'database':dwh_database
}

conn = connect_to_dwh(conn_details)
print('conn successful')
schema = 'raw_data'


cursor = conn.cursor()

# Create the dev schema
# create_dev_schema_query = 'CREATE SCHEMA raw_data;'
# cursor.execute(create_dev_schema_query)

# ----- Creating the raw tables
for query in raw_data_tables:
    print(f'=================== {query[:50]}')
    cursor.execute(query)
    conn.commit()


# Copying from S3 to Redshift DWH
for table in db_tables:
    query = f'''
    copy {schema}.{table} 
    from '{s3_path.format(bucket_name, table)}'
    iam_role '{dwh_role}'
    delimiter ','
    ignoreheader 1;
    '''
    cursor.execute(query)
    conn.commit()

    
conn.commit()
cursor.close()
conn.close()