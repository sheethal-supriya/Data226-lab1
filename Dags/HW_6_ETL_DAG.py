from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging


# Establishing Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


@task
def create_tables():

    logging.info("Creating user_session_channel and session_timestamp tables...")

    cur = return_snowflake_conn()

    create_user_session_channel = """
    CREATE TABLE IF NOT EXISTS USER_DB_CAT.raw.user_session_channel (
        userId INT NOT NULL,
        sessionId VARCHAR(32) PRIMARY KEY,
        channel VARCHAR(32) DEFAULT 'direct'
    );
    """

    create_session_timestamp = """
    CREATE TABLE IF NOT EXISTS USER_DB_CAT.raw.session_timestamp (
        sessionId VARCHAR(32) PRIMARY KEY,
        ts TIMESTAMP
    );
    """

    create_stage = """
    CREATE OR REPLACE STAGE USER_DB_CAT.raw.blob_stage
    URL = 's3://s3-geospatial/readonly/'
    FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
    """

    try:
        cur.execute(create_user_session_channel)
        cur.execute(create_session_timestamp)
        cur.execute(create_stage)
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        raise e


@task
def load_user_session_channel():
    """
    Loads data into the user_session_channel table from S3 stage.
    """
    logging.info("Loading data into user_session_channel...")

    cur = return_snowflake_conn()

    load_user_session_channel_data = """
    COPY INTO USER_DB_CAT.raw.user_session_channel
    FROM @USER_DB_CAT.raw.blob_stage/user_session_channel.csv;
    """

    try:
        cur.execute(load_user_session_channel_data)
    except Exception as e:
        logging.error(f"Error loading user_session_channel: {e}")
        raise e


@task
def load_session_timestamp():
    """
    Loads data into the session_timestamp table from S3 stage.
    """
    logging.info("Loading data into session_timestamp...")

    cur = return_snowflake_conn()

    load_session_timestamp_data = """
    COPY INTO USER_DB_CAT.raw.session_timestamp
    FROM @USER_DB_CAT.raw.blob_stage/session_timestamp.csv;
    """

    try:
        cur.execute(load_session_timestamp_data)
    except Exception as e:
        logging.error(f"Error loading session_timestamp: {e}")
        raise e


# Define DAG
with DAG(
    dag_id="HW6_ETL_DAG",
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=["ETL"],
    schedule="45 3 * * *",  # Runs daily at 2:45 AM
) as dag:

    # Creating tables
    create_task = create_tables()

    # Loading data into tables
    load_user_task = load_user_session_channel()
    load_session_task = load_session_timestamp()

    # Define dependencies
    create_task >> [load_user_task, load_session_task]
