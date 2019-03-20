# -*- coding: utf-8 -*-

"""
If your data is already partitioned:

1. S3 key naming convention: s3://<bucket_name>/<prefix>/<partition_value>/xxx.csv
2. **Specify Partition Key** when you define the table.
3. Run the command ALTER first (see example ``sql_add_partition``).

Reference: https://docs.aws.amazon.com/athena/latest/ug/partitions.html
"""

import rolex
import pandas as pd
from faker import Faker
from datetime import datetime
from s3iotools.io.dataframe import S3Dataframe
from athena_practice.s3_setup import s3, bucket_name, get_data_key, get_data_uri
from athena_practice.s3_setup import run_query, athena

dataset_name = "users_not_partitioned"
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
            key=f"{data_key}/{partition_key}/data.parquet.gz",
            compression="gzip",
            use_deprecated_int96_timestamps=True,
        )


# create_testdata()

# Run this command in console or boto3 athena API
sql_add_partition = """
ALTER TABLE users_not_partitioned ADD PARTITION (create_month='2018-07') location 's3://sanhe-aws-athena-practice/data/db_learn_athena/tb_users_not_partitioned/2018-07/'
"""

response = athena.start_query_execution(
    QueryString=sql_add_partition,
    QueryExecutionContext={"Database": "learn_athena"},
    ResultConfiguration={"OutputLocation": "s3://sanhe-aws-athena-practice/result"}
)

print(data_uri)
sql = """
SELECT
    T.id,
    T.name,
    T.create_time
FROM users_not_partitioned AS T
WHERE
    T.create_time BETWEEN timestamp '2018-07-15' AND timestamp '2018-07-16'
    AND T.create_month = '2018-07'
ORDER BY T.create_time ASC
LIMIT 5;
"""
st = datetime.now()
run_query(sql)
print((datetime.now() - st).total_seconds())
