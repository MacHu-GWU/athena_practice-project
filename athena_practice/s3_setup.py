# -*- coding: utf-8 -*-

"""
s3://sanhe-aws-athena-practice/data/db_learn_athena/
"""

import boto3
from .config import aws_profile, bucket_name, dbname

ses = boto3.Session(profile_name=aws_profile)
s3 = ses.resource("s3")
bucket = s3.Bucket(bucket_name)
if bucket.creation_date is None:
    s3.create_bucket(Bucket=bucket_name)
    bucket = s3.Bucket(bucket_name)

root_uri = f"s3://{bucket_name}/data/{dbname}"
result_uri = f"s3://{bucket_name}/result"

athena = ses.client("athena")

def get_data_key(dataset_name):
    return f"data/{dbname}/tb_{dataset_name}"


def get_data_uri(dataset_name):
    return f"s3://{bucket_name}/data/{dbname}/tb_{dataset_name}/"


def run_query(sql):
    from pyathena import connect

    cursor = connect(
        profile_name=aws_profile,
        s3_staging_dir=result_uri,
        schema_name="learn_athena",
    ).cursor()

    cursor.execute(sql)
    for row in cursor.fetchall():
        print(row)
