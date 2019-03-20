# -*- coding: utf-8 -*-

"""
Parquet works fine with comma and line change.
"""

import pandas as pd
from s3iotools.io.dataframe import S3Dataframe
from athena_practice.s3_setup import s3, bucket_name, get_data_key, get_data_uri
from athena_practice.s3_setup import run_query

dataset_name = "addresses_string_with_comma"
data_key = get_data_key(dataset_name)
data_uri = get_data_uri(dataset_name)


def create_testdata():
    s3df = S3Dataframe(s3_resource=s3, bucket_name=bucket_name)

    address_data = [
        dict(id=1, address="123 St\n Los Angeles, CA 90000"),
    ]
    df = pd.DataFrame(
        address_data,
        columns="id,address".split(","),
    )
    s3df.df = df
    s3df.to_parquet(
        key=f"{data_key}/data.parquet.gz",
        compression="gzip",
    )


# create_testdata()

print(data_uri)
sql = f"""
SELECT
    T.address
FROM {dataset_name} AS T
LIMIT 5;
"""
run_query(sql)
