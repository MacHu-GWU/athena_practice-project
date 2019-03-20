# -*- coding: utf-8 -*-

import json
import random
import pandas as pd
import boto3
import rolex
from faker import Faker
from pprint import pprint
from s3iotools.io.dataframe import S3Dataframe

# s3://sanhe-aws-athena-practice/data/db_learn_athena/tb_orders

aws_profile = "sanhe"
bucket_name = "sanhe-aws-athena-practice"
dbname = "db_learn_athena"


class S3URI:
    db_requests_key = f"data/{dbname}/tb_requests"
    db_requests_uri = f"s3://{bucket_name}/{db_requests_key}/"

    db_users_key = f"data/{dbname}/tb_users"
    db_users_uri = f"s3://{bucket_name}/{db_users_key}/"

    db_orders_key = f"data/{dbname}/tb_orders"
    db_orders_uri = f"s3://{bucket_name}/{db_orders_key}/"


root_uri = f"s3://{bucket_name}/data/{dbname}"
result_uri = f"s3://{bucket_name}/result"

ses = boto3.Session(profile_name=aws_profile)
s3 = ses.resource("s3")
bucket = s3.Bucket(bucket_name)
if bucket.creation_date is None:
    s3.create_bucket(Bucket=bucket_name)
    bucket = s3.Bucket(bucket_name)


# def pprint(data):
#     print(json.dumps(data, indent=4, sort_keys=True))


class TestData(object):
    @staticmethod
    def create_users():
        fake = Faker()
        s3df = S3Dataframe(s3_resource=s3, bucket_name=bucket_name)

        n_users = 1000
        users_data = [
            dict(
                id=id,
                name=fake.name(),
                create_time=rolex.rnd_datetime("2018-01-01", "2019-01-01"),
            )
            for id in range(1, n_users+1)
        ]
        df = pd.DataFrame(
            users_data,
            columns="id,name,create_time".split(","),
        )
        s3df.df = df
        s3df.to_parquet(key="{}/2018.parquet.gz".format(S3URI.db_users_key), gzip_compressed=True)

    @staticmethod
    def create_requests():
        fake = Faker()
        s3df = S3Dataframe(s3_resource=s3, bucket_name=bucket_name)

        # --- requests
        method_list = "GET,POST,PATCH,DELETE".split(",")
        n_requests = 1000
        requests_data = [
            dict(
                id=id,
                time=rolex.rnd_datetime("2018-01-01", "2019-01-01"),
                method=random.choice(method_list),
                endpoint=fake.url(),
            )
            for id in range(1, n_requests + 1)
        ]
        # --- Save to s3
        df = pd.DataFrame(
            requests_data,
            columns="id,time,method,endpoint".split(","),
        )
        s3df.df = df
        s3df.to_csv(key="{}/2018.csv.gz".format(S3URI.db_requests_key), gzip_compressed=True)

    @staticmethod
    def create_orders():
        fake = Faker()
        s3df = S3Dataframe(s3_resource=s3, bucket_name=bucket_name)

        # --- orders
        n_shop = 5
        shop_data = [
            dict(
                shop_id=shop_id,
                address=fake.address().replace("\n", ", "),
                zipcode=str(random.randint(10000, 99999)),
            )
            for shop_id in range(1, n_shop + 1)
        ]
        # print(shop_data[0])

        n_customer = 100
        gender_list = ["male", "female"]
        customer_data = [
            dict(
                customer_id=customer_id,
                name=fake.name(),
                gender=random.choice(gender_list),
                dob=str(rolex.rnd_datetime("1950-01-01", "2010-01-01").date()),
            )
            for customer_id in range(1, n_customer + 1)
        ]
        # print(customer_data[0])

        n_item = 800
        item_data = [
            dict(
                item_id=item_id,
                name=fake.word(),
                price=random.randint(100, 10000) / 100.0,
            )
            for item_id in range(1, n_item + 1)
        ]
        # pprint(item_data[0])

        n_order = 1000
        order_data = [
            dict(
                order_id=order_id,
                time=rolex.rnd_datetime("2018-01-01", "2018-12-31 23:59:59"),
                # shop=json.dumps(random.choice(shop_data), ensure_ascii=False),
                # customer=json.dumps(random.choice(customer_data), ensure_ascii=False),
                # items=json.dumps(list(random.sample(item_data, random.randint(1, 20))), ensure_ascii=False),
            )
            for order_id in range(1, n_order + 1)
        ]
        # pprint(order_data[0])

        df = pd.DataFrame(
            order_data,
            columns="order_id,time,shop,customer,items".split(","),
        )
        s3df.df = df
        # s3df.to_csv(key="{}/2018.csv.gz".format(S3URI.db_orders_key), sep="\t", gzip_compressed=True)
        s3df.to_parquet(key="{}/2018.parquet.gz".format(S3URI.db_orders_key), compression="gzip")


def make_query():
    athena = ses.client("athena")
    sql = """
    SELECT * FROM users LIMIT 5;
    """
    res = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext=dict(Database="learn_athena"),
        ResultConfiguration=dict(
            OutputLocation=result_uri,
        )
    )
    print(res)


# make_query()


from pyathena import connect

cursor = connect(
    profile_name=aws_profile,
    s3_staging_dir=result_uri,
    schema_name="learn_athena",
).cursor()


def query_requests():
    sql = """
    SELECT *
    FROM requests
    WHERE
        requests.time BETWEEN TIMESTAMP '2018-07-01' AND TIMESTAMP '2018-08-01'
    ORDER BY requests.time ASC
    LIMIT 5;
    """

    cursor.execute(sql)
    for row in cursor.fetchall():
        print(row)


# query_requests()

def query_orders():
    sql = """
    SELECT 
        orders.order_id,
        orders.time
    FROM orders
    LIMIT 5;
    """
    # sql = """
    # WITH dataset AS (
    #     SELECT ARRAY[
    #         CAST(ROW(1, 'Alice') AS ROW(id INTEGER, name VARCHAR)),
    #         CAST(ROW(2, 'Bob') AS ROW(id INTEGER, name VARCHAR))
    #     ] AS users
    # )
    # SELECT *
    # FROM dataset
    # """
    cursor.execute(sql)
    for row in cursor.fetchall():
        print(row)
        # items_data = row[4]
        # print(json.loads(items_data))

# TestData.create_requests()

# TestData.create_orders()
# query_orders() # s3://sanhe-aws-athena-practice/data/db_learn_athena/tb_orders/


def query_users():
    sql = """
    SELECT *
    FROM users
    LIMIT 5;
    """

    cursor.execute(sql)
    for row in cursor.fetchall():
        print(row)

TestData.create_users()
# query_users()


"""
- Redshift: 0.16TB, $180 / Month
- Athena: 36TB, $180 / Month
1GB = 64M
"""

"""
CREATE EXTERNAL TABLE IF NOT EXISTS learn_athena.orders (
  `order_id` int,
  `time` timestamp,
  `shop` string,
  `customer` string,
  `items` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://sanhe-aws-athena-practice/data/db_learn_athena/tb_orders/'
TBLPROPERTIES ('has_encrypted_data'='false');
"""