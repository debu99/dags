from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import boto3, os

print("init s3...")
sts_client = boto3.client('sts')
with open(os.getenv("AWS_WEB_IDENTITY_TOKEN_FILE")) as f:
    web_token = f.readlines()
response = sts_client.assume_role_with_web_identity(
    DurationSeconds=3600,
    RoleArn=os.getenv("AWS_ROLE_ARN"),
    RoleSessionName='dag',
    WebIdentityToken=web_token
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





    s3_resource = boto3.resource(
        's3',
        

    for bucket in s3_resource.buckets.all():
        print(bucket.name)





def print_hello():
    return 'Hello world from first Airflow DAG!'

dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 10, 15), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator
