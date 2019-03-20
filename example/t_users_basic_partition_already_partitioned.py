# -*- coding: utf-8 -*-

"""
If your data is already partitioned:

1. S3 key naming convention: s3://<bucket_name>/<prefix>/<partition_key_column_name>=<partition_value>/xxx.csv
2. Run this command first ``MSCK REPAIR TABLE <table-name>`` to load those partition.

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


def create_testdata():
    fake = Faker()
    s3df = S3Dataframe(s3_resource=s3, bucket_name=bucket_name)

    n_users_per_month = 1000
    year = 2018
    user_id = 0
    for month in range(1, 12 + 1):
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
        partition_key = f"{year}-{str(month).zfill(2)}"
        s3df.to_parquet(
            key=f"{data_key}/create_month={partition_key}/data.parquet.gz",
            compression="gzip",
            use_deprecated_int96_timestamps=True,
        )


# create_testdata()

print(data_uri)
sql = """
SELECT
    T.id,
    T.name,
    T.create_time
FROM users_already_partitioned AS T
WHERE
    T.create_time BETWEEN timestamp '2018-07-15' AND timestamp '2018-07-16'
    AND T.create_month = '2018-07'
ORDER BY T.create_time ASC
LIMIT 5;
"""
# run_query(sql)
