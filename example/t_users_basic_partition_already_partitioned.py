# -*- coding: utf-8 -*-

"""
If your data is already partitioned:

1. S3 key naming convention: s3://<bucket_name>/<prefix>/<partition_key_column_name1>=<partition_value1>/<partition_key_column_name2>=<partition_value2>/xxx.csv
2. Run this command first ``MSCK REPAIR TABLE <table_name>`` to load those partition.

Reference: https://docs.aws.amazon.com/athena/latest/ug/partitions.html
"""

import rolex
import pandas as pd
from faker import Faker
from datetime import datetime
from s3iotools.io.dataframe import S3Dataframe
from athena_practice.s3_setup import s3, bucket_name, get_data_key, get_data_uri
from athena_practice.s3_setup import run_query

dataset_name = "users_already_partitioned"
data_key = get_data_key(dataset_name)
data_uri = get_data_uri(dataset_name)
print(f"dataset uri: {data_uri}")

create_table_sql = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS learn_athena.{dataset_name} (
  `id` int,
  `name` string,
  `create_time` timestamp 
) PARTITIONED BY (
  create_year smallint,
  create_month tinyint 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://sanhe-aws-athena-practice/data/db_learn_athena/tb_users_already_partitioned/'
TBLPROPERTIES ('has_encrypted_data'='false');
"""

load_partition_sql = f"MSCK REPAIR TABLE {dataset_name};"


def create_testdata():
    fake = Faker()
    s3df = S3Dataframe(s3_resource=s3, bucket_name=bucket_name)

    n_users_per_month = 1000
    n_years = 3
    start_year = 2000
    user_id = 0
    for i in range(n_years):
        year = start_year + i
        for month in range(1, 12 + 1):
            folder_name = f"create_year={str(year).zfill(4)}/create_month={str(month).zfill(2)}"
            key = f"{data_key}/{folder_name}/data.parquet.tz"

            start = datetime(year, month, 1)
            end = rolex.add_seconds(rolex.add_months(start, 1), -1)

            users_data = list()
            for _ in range(n_users_per_month):
                user_id += 1
                user = dict(
                    id=user_id,
                    name=fake.name(),
                    create_time=rolex.rnd_datetime(start, end),
                )
                users_data.append(user)

            df = pd.DataFrame(
                users_data,
                columns="id,name,create_time".split(","),
            )
            s3df.df = df

            print(f"writing to {folder_name}")
            s3df.to_parquet(
                key=key,
                compression="gzip",
                use_deprecated_int96_timestamps=True,
            )


# create_testdata()


run_query(create_table_sql)
run_query(load_partition_sql)

sql = """
SELECT
    T.id,
    T.name,
    T.create_time
FROM users_already_partitioned AS T
WHERE
    T.create_year = 2001
    AND T.create_month BETWEEN 7 AND 9
ORDER BY T.create_time ASC
LIMIT 5;
"""
run_query(sql)
