from distutils.sysconfig import PREFIX
import boto3
from dotenv import load_dotenv
from loguru import logger
import os

from botocore.exceptions import ClientError

load_dotenv()

ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
S3_BUCKET = os.environ.get('S3_BUCKET')
S3_URL = os.environ.get('S3_URL')
REGION = os.environ.get('REGION')


# import boto3
# # from boto3 import Session
# session = boto3.Session
# s3 = session.client('s3')

# sess = session(
#     endpoint_url= S3_URL,
#     aws_access_key_id=ACCESS_KEY,
#     aws_secret_access_key=SECRET_KEY,
#     use_ssl=False,
# )


# s3 = sess.client("s3")
# print(s3.list_objects(Bucket=S3_BUCKET))
# print(ACCESS_KEY)
# print(SECRET_KEY)

def get_bucket():
    try:
        s3 = get_s3()
        return s3.Bucket(S3_BUCKET)
    except Exception as e:
        logger.exception(e)

LINODE_OBJ_CONFIG = {
                        "aws_access_key_id": ACCESS_KEY,
                        "aws_secret_access_key": SECRET_KEY,
                        "region_name": REGION,
                        "endpoint_url": S3_URL
}

def get_s3():
    s3 = boto3.resource('s3',
            **LINODE_OBJ_CONFIG)
    return s3

def get_client():
    client = boto3.client("s3", **LINODE_OBJ_CONFIG)
    return client

def get_list_from_s3():
    client = get_client()
    print(client)
    try:
        response = client.list_objects(Bucket=S3_BUCKET, PREFIX="sam/")
        # response = client.list_objects(Bucket=S3_BUCKET, Prefix='original/') # doesn't work
        original = list()
        processed = list()
        print(response['Contents'])
        for object in response["Contents"]:
            original.append(object["Key"].replace("sam/","mikeyy/"))
        response = client.list_objects(Bucket=S3_BUCKET, Prefix='processed/')
        for object in response["Contents"]:
            processed.append(object["Key"].replace("processed/",""))

    except:
        return None
    return [original,processed]

# this works************
# client = get_client()
# print(client.list_objects(Bucket=S3_BUCKET))

print(get_list_from_s3())


def get_bucket_items(s3, bucket):
    try:
        for buck_obj in s3.Bucket(bucket.name).objects.all():
            print(buck_obj.key)
        
    except ClientError as e:
        print(f"Error: {e}")

s3 = get_s3()
# bucket = get_bucket()


# Copy from old to new file
# s3.meta.client.copy(copy_source, S3_BUCKET, new_file)
old_file = 'sam/pdf/temp.pdf'
new_file = 'mikeyy3/pdf/temp.pdf'
copy_source = {
    'Bucket': S3_BUCKET,
    'Key': old_file
}
def copy_key(source, new_key):
    bucket = get_bucket()
    bucket.copy(source, new_key)

copy_key(copy_source, new_file)

# Delete old file
del_source = {
    'Bucket': S3_BUCKET,
    'Key': 'mikeyy2/pdf/temp.pdf'
}

def del_key(key):
    s3.Object(S3_BUCKET, key).delete()
    
del_key(del_source['Key'])


rename_source = {
    'Bucket': S3_BUCKET,
    'Key': 'mikeyy3/pdf/temp.pdf'
}

rename_to = 'ichigo/pdf/temp.pdf'
def rename_key(rename_source, rename_to):
    
    copy_key(rename_source, rename_to)
    del_key(rename_source['Key'])


rename_key(rename_source, rename_to)


for bucket in s3.buckets.all():
        print(bucket.name)
        # print(bucket.key)
        get_bucket_items(s3, bucket)
        print()

# s3.Object(S3_BUCKET, "/mikey/pdf/temp.pdf").copy_from(CopySource=copy_source)
# s3.Object(S3_BUCKET, "/mikey/pdf/temp.pdf").copy_from(CopySource=f'{old_file}')

# s3 = get_s3()
# # bucket = get_bucket()
# for bucket in s3.buckets.all():
#         print(bucket.name)
#         # print(bucket.key)
#         get_bucket_items(s3, bucket)
#         print()