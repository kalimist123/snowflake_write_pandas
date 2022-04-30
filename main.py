from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect
import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

# Define the table name, schema, and database you want to write to
# Note: the table, schema, and database need to already exist in Snowflake

table_name = 'CITIES'
schema = 'CUSTOMERS'
database = 'CW'

conn = connect(
    user=os.getenv("SNOW_USER"),
    password=os.getenv("SNOW_PASSWORD"),
    account=os.getenv("SNOW_ACCOUNT"),
    role=os.getenv("SNOW_ROLE"),
    warehouse=os.getenv("SNOW_WAREHOUSE"),
    schema=schema

)

first_stamp = int(round(time.time() * 1000))

dfcv = pd.read_csv('ratings.csv')

second_stamp = int(round(time.time() * 1000))

# Calculate the time taken in milliseconds
time_taken = second_stamp - first_stamp

# To get time in seconds:
time_taken_seconds = round(time_taken / 1000)
print(f'Read from CSV took{time_taken_seconds} seconds or {time_taken} milliseconds')

conn.cursor().execute(f"USE DATABASE CW")
dfcv.columns = dfcv.columns.str.upper()

first_stamp = int(round(time.time() * 1000))
dfcv.to_parquet('df.parquet.gzip',
                compression='gzip')

second_stamp = int(round(time.time() * 1000))

# Calculate the time taken in milliseconds
time_taken = second_stamp - first_stamp

# To get time in seconds:
time_taken_seconds = round(time_taken / 1000)
print(f'write to parquet took {time_taken_seconds} seconds or {time_taken} milliseconds')

first_stamp = int(round(time.time() * 1000))
dfcvpq = pd.read_parquet('df.parquet.gzip')

second_stamp = int(round(time.time() * 1000))

# Calculate the time taken in milliseconds
time_taken = second_stamp - first_stamp

# To get time in seconds:
time_taken_seconds = round(time_taken / 1000)
print(f'Read from parquet took{time_taken_seconds} seconds or {time_taken} milliseconds')

first_stamp = int(round(time.time() * 1000))
write_pandas(
    conn=conn,
    df=dfcvpq,
    table_name='RATINGS',
    database=database,
    schema=schema

)

second_stamp = int(round(time.time() * 1000))

# Calculate the time taken in milliseconds
time_taken = second_stamp - first_stamp

# To get time in seconds:
time_taken_seconds = round(time_taken / 1000)
print(f'time taken to write to snowflake{time_taken_seconds} seconds or {time_taken} milliseconds')
