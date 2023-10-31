import json
import boto3
import os
import psycopg2
import csv
from datetime import datetime



def handler(event, context):
    print("Starting Job")
    update_view()

# def easebase_conn():
#     ssm = boto3.client('ssm',  aws_access_key_id=os.environ['KEY'], aws_secret_access_key=os.environ['SECRET'],  region_name='us-east-2')
#     param = ssm.get_parameter(Name='db_postgres_easebase_sa', WithDecryption=True )
#     db_request = json.loads(param['Parameter']['Value']) 

#     hostname = db_request['host']
#     portno = db_request['port']
#     dbname = db_request['database']
#     dbusername = db_request['user']
#     dbpassword = db_request['password']
#     conn = psycopg2.connect(host=hostname,user=dbusername,port=portno,password=dbpassword,dbname=dbname)
#     conn.autocommit = False
#     return conn

def proxy_conn():
    user_name = os.environ['username']
    password = os.environ['password']
    rds_proxy_host = os.environ['host']
    db_name = os.environ['engine']
    try:
        conn = psycopg2.connect(host=rds_proxy_host,user=user_name,password=password,dbname=db_name)
        return conn
    except Exception as e:
        print(f'db connection failed: {e}')


def upload_csv(run_id, filename):
    s3_bucket_name = '107635001951-us-east-2-prod-internal-facing-data'
    s3_object_key = 'podium/' + 'SM_VISITS_' + datetime.now().strftime('%Y-%m-%d')
    view = 'rpt.podium_sm_visits_dtl'
    _targetconnection = proxy_conn()
    cursor = _targetconnection.cursor()
    try:
        cursor.execute(f'SELECT * FROM {view}')
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        print(len(rows))
    except Exception as e:
        print(f'Error executing SQL query: {e}')
        _targetconnection.close()
        exit(1)
    # Define the CSV file path
    csv_file_path = '/tmp/output.csv'

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
        with open(csv_file_path, 'rb') as file:
            s3.put_object(Bucket=s3_bucket_name, Key=s3_object_key,Body=file, ContentType='text/csv')
            print(f'CSV file uploaded to S3 bucket: s3://{s3_bucket_name}/{s3_object_key}')
        update_log_table(run_id, filename)
        os.remove(csv_file_path)
    except Exception as e:
        print(f'Error uploading CSV file to S3: {e}')

    # Close the database connection
    cursor.close()
    _targetconnection.close()

def update_view():
    print('hello')
    try:
        _targetconnection = proxy_conn()
        cursor = _targetconnection.cursor()
    except Exception as e:
        print(f'db connection failed: {e}')
    
    try:
        run_id = 0
        file_name = ''
        cursor.execute(f"call rpt.podium_sm_gen_file({run_id}, '')")
        output = cursor.fetchall()
        run_id, file_name = output[0]
        _targetconnection.commit()
        print('before upload')
        upload_csv(run_id, file_name)
    except Exception as e:
        print(f'Error executing SQL query: {e}')
        _targetconnection.close()
        exit(1)

def update_log_table(run_id, filename):
    _targetconnection = proxy_conn()
    cursor = _targetconnection.cursor()
    print(run_id, filename)
    cursor.execute(f"call rpt.podium_file_status({run_id}, 'Complete')")
    _targetconnection.commit()
    _targetconnection.close()

# update_view()