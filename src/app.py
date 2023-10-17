import json
import boto3
import os
import psycopg2

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
    _targetconnection = easebase_conn()
    cursor = _targetconnection.cursor()
    key = os.environ['KEY']
    secret = os.environ['SECRET']
    proc_call = f"call public.podium_sm_gen_file({key}, {secret})"
    cursor.execute(proc_call,)
    _targetconnection.commit()