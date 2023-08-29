import os
import json
import time
import boto3
import duckdb as db
import pandas as pd
import awswrangler as wr
import streamlit as st
from dotenv import load_dotenv
from helpers.constants import config_vars

load_dotenv()

st.write("Using DuckDB to query csv files from S3")

session = boto3.Session(aws_access_key_id=config_vars.get("AWS_ACCESS_KEY_ID"),\
                        aws_secret_access_key=config_vars.get("AWS_SECRET_ACCESS_KEY"))

lambda_client = session.client('lambda', region_name='us-east-1')

if st.button("Get Data Analysis"):
    response = lambda_client.invoke(
        FunctionName=config_vars.get("LAMBDA_FUNCTION_NAME"),
        Payload='{}'
    )
    payload = response['Payload'].read()

    st.write(f"Payload: {payload}")

str_payload = payload.decode("utf-8")

s3_path = json.loads( str_payload ).get("body").get("output_path")

st.write(f"Path: {s3_path}")

df = wr.s3.read_parquet(path=s3_path, dataset=True, boto3_session=session)

# df["index"] = df["anio"]
# df.set_index("index", inplace=True)

# #df.rename(columns={'anio':'index'}).set_index('index')

# st.bar_chart(df,x='city',y='count_visits', color='anio')

st.line_chart(df, x='anio', y='count_visits', color='city')
