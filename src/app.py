import json
import boto3
import os
import psycopg2
import csv
from datetime import datetime



def handler():
    call_easebase()

def easebase_conn():
    ssm = boto3.client('ssm',  aws_access_key_id=os.environ['KEY'], aws_secret_access_key=os.environ['SECRET'],  region_name='us-east-2')
    param = ssm.get_parameter(Name='db_postgres_easebase_sa', WithDecryption=True )
    db_request = json.loads(param['Parameter']['Value']) 

    hostname = db_request['host']
    portno = db_request['port']
    dbname = db_request['database']
    dbusername = db_request['user']
    dbpassword = db_request['password']
    conn = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
    conn.autocommit = False
    return conn

def call_easebase():
    s3_bucket_name = '107635001951-us-east-2-prod-internal-facing-data'
    s3_object_key = 'podium/' + 'SM_VISITS_' + datetime.now().strftime('%Y-%m-%d')
    view = 'public.podium_sm_visits_dtl'
    _targetconnection = easebase_conn()
    cursor = _targetconnection.cursor()
    try:
        cursor.execute(f'SELECT * FROM {view}')
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    except Exception as e:
        print(f'Error executing SQL query: {e}')
        _targetconnection.close()
        exit(1)
    # Define the CSV file path
    csv_file_path = 'output.csv'

    # Write data to a CSV file
    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(colnames)  # Write column headers
        csv_writer.writerows(rows)

    print(f'Data exported to {csv_file_path}')

    # Upload the CSV file to S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ['KEY'],
        aws_secret_access_key=os.environ['SECRET']
    )

    try:
        s3.upload_file(csv_file_path, s3_bucket_name, s3_object_key)
        print(f'CSV file uploaded to S3 bucket: s3://{s3_bucket_name}/{s3_object_key}')
    except Exception as e:
        print(f'Error uploading CSV file to S3: {e}')

    # Close the database connection
    cursor.close()
    _targetconnection.close()