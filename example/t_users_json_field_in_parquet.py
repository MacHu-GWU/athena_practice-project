# -*- coding: utf-8 -*-

"""
Don't use Json string in Parquet, if you want to filter based on its value.
"""

import json
import pandas as pd
from s3iotools.io.dataframe import S3Dataframe
from athena_practice.s3_setup import s3, bucket_name, get_data_key, get_data_uri
from athena_practice.s3_setup import run_query

dataset_name = "users_json_field_in_parquet"
data_key = get_data_key(dataset_name)
data_uri = get_data_uri(dataset_name)


def create_testdata():
    s3df = S3Dataframe(s3_resource=s3, bucket_name=bucket_name)

    n_users = 1000
    users_data = [
        dict(
            id=id,
            profile=json.dumps(dict(user_id=id)),
        )
        for id in range(1, 1+n_users)
    ]
    df = pd.DataFrame(
        users_data,
        columns="id,name,create_time".split(","),
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
    T.id,
    CAST(T.profile.name AS varchar∂) as name
FROM {dataset_name} AS T
LIMIT 5;
"""
run_query(sql)
