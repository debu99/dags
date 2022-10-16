from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import boto3, os

print("init s3...")
with open(os.getenv("AWS_WEB_IDENTITY_TOKEN_FILE")) as f:
    lines = f.readlines()
response = boto3.client('sts').assume_role_with_web_identity(
    DurationSeconds=3600,
    RoleArn=os.getenv("AWS_ROLE_ARN"),
    RoleSessionName='dag',
    WebIdentityToken=lines[0]
)
credentials = response['Credentials']

s3_client = boto3.client('s3', region_name='ap-southeast-1', 
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
    )
for key in s3_client.list_objects(Bucket='fargate-airflow-logs')['Contents']:
    print(key['Key'])
print("s3 initilized...")

def print_hello():
    return 'Hello world from first Airflow DAG!'

dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='30 12 * * *',
          start_date=datetime(2022, 10, 15), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator
