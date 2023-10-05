# https://docs.snowflake.com/en/developer-guide/python-connector/python-connector
# pip install snowflake-connector-python
import snowflake.connector

USER_NAME = '<YOUR_USERNAME>' ## your username
PASSWORD = '<YOUR_PASSWORD>' ## your password
SNOWFLAKE_ACCOUNT = 'icb36344.us-east-1'
SNOWFLAKE_WAREHOUSE = "INOVEST_PARTNERS"
SNOWFLAKE_SCHEMA = "NIQ_ERECEIPT.INOVEST"

## snowflake connection function
def start_snowflake_connection():
    ctx = snowflake.connector.connect(user=USER_NAME, password=PASSWORD, account=SNOWFLAKE_ACCOUNT,)
    cs = ctx.cursor()
    run_sql(cs, 'use warehouse ' + SNOWFLAKE_WAREHOUSE)
    run_sql(cs, 'use ' + SNOWFLAKE_SCHEMA)
    return cs

## run_sql function
def run_sql(cur, sql):
    try:
        cur.execute(sql)
        records = cur.fetchall()
    except Exception as e:
        records = []
        print(sql)
        print(e)
        pass
    return records

## start connection
cur = start_snowflake_connection()

## run a sql
sql = """SELECT * FROM NIQ_ERECEIPT.INOVEST.NIQ_QUANT_FEED_DAILY LIMIT 100;"""
results = run_sql(cur, sql)
print(results)

## run a sql
sql = """SELECT * FROM NIQ_ERECEIPT.INOVEST.NIQ_QUANT_FEED_MERCHANT_DAILY LIMIT 100;"""
results = run_sql(cur, sql)
print(results)