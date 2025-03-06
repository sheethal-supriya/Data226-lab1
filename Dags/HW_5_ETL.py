from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime
import requests
import pandas as pd


# Establish Snowflake Connection
def return_snowflake_conn():
    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_conn"
    )  # Ensure connection name is correct
    conn = hook.get_conn()
    return conn.cursor()


# Task 1: Extract Data from Alpha Vantage API
@task
def extract(symbol):
    api_key = Variable.get(
        "alpha_vantage_api_key"
    )  # Fetch API key from Airflow Variables
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"

    response = requests.get(url)
    data = response.json()

    # Extract time series data
    time_series = data.get("Time Series (Daily)", {})
    df = pd.DataFrame.from_dict(time_series, orient="index")
    df.columns = ["open", "high", "low", "close", "volume"]
    df.index = pd.to_datetime(df.index)

    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    df["symbol"] = symbol

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    df = df.sort_values(by="date", ascending=False).head(90)

    print("Extracted Data (First 5 rows):")
    print(df.head())

    return df.to_dict(orient="records")


# Task 2: Transform Data
@task
def transform(data):
    transformed_data = [
        (
            row["symbol"],
            row["date"],
            float(row.get("open", 0)),
            float(row.get("high", 0)),
            float(row.get("low", 0)),
            float(row.get("close", 0)),
            int(row.get("volume", 0)),
        )
        for row in data
    ]

    print("Transformed Data (First 5 rows):")
    print(transformed_data[:5])

    return transformed_data


# Task 3: Load Data into Snowflake
@task
def load(records, target_table):
    con = return_snowflake_conn()

    try:
        con.execute("BEGIN;")
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {target_table} (
            symbol STRING,
            date TIMESTAMP,
            open NUMBER(38, 4),
            high NUMBER(38, 4),
            low NUMBER(38, 4),
            close NUMBER(38, 4),
            volume NUMBER(38, 0),
            PRIMARY KEY (symbol, date)
        );"""
        )

        if not records:
            raise ValueError("No data available for insertion.")

        symbol_to_insert = records[0][0]  # Extract symbol from first record
        con.execute(
            f"DELETE FROM {target_table} WHERE symbol = %s", (symbol_to_insert,)
        )

        sql = f"""
            INSERT INTO {target_table} (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        con.executemany(sql, records)

        con.execute("COMMIT;")
        print(f"✅ Successfully inserted {len(records)} rows into {target_table}")

    except Exception as e:
        con.execute("ROLLBACK;")
        print(f"❌ Error inserting data into Snowflake: {e}")
        raise e


# DAG Configuration
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# Define the DAG
with DAG(
    dag_id="hw5_ETL_DAG",
    default_args=default_args,
    start_date=datetime(2024, 9, 25),
    catchup=False,
    tags=["ETL"],
    schedule_interval="@daily",
) as dag:
    symbol = "TSLA"
    target_table = "dev.raw.STOCK_DATA"

    raw_data = extract(symbol)
    transformed_data = transform(raw_data)
    load_task = load(transformed_data, target_table)

    raw_data >> transformed_data >> load_task
