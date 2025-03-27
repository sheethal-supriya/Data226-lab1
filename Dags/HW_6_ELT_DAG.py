from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_ctas(database, schema, table, select_sql, primary_key=None):
    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        sql = (
            f"CREATE OR REPLACE TABLE {database}.{schema}.temp_{table} AS {select_sql}"
        )

        logging.info(sql)
        cur.execute(sql)

        # do primary key uniquess check
        if primary_key is not None:
            sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {database}.{schema}.temp_{table}
              GROUP BY 1
              ORDER BY 2 DESC
              LIMIT 1"""
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")

        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} AS
            SELECT * FROM {database}.{schema}.temp_{table} WHERE 1=0;"""
        cur.execute(main_table_creation_if_not_exists_sql)

        swap_sql = f"""ALTER TABLE {database}.{schema}.{table} SWAP WITH {database}.{schema}.temp_{table};"""
        cur.execute(swap_sql)
    except Exception as e:
        raise


@task
def check_duplicates(database, schema, table):

    cur = return_snowflake_conn()

    try:
        # Get total row count
        total_rows_count = f"SELECT COUNT(1) FROM {database}.{schema}.{table};"
        logging.info(f"Executing total rows count: {total_rows_count}")
        cur.execute(total_rows_count)
        total_count = cur.fetchone()[0]

        # Get count of distinct rows
        distinct_rows_count = f"""
            SELECT COUNT(1) FROM (
                SELECT DISTINCT * FROM {database}.{schema}.{table}
            );
        """
        logging.info(f"Executing distinct rows count: {distinct_rows_count}")
        cur.execute(distinct_rows_count)
        distinct_count = cur.fetchone()[0]

        logging.info(
            f"Total rows count: {total_count}, Distinct rows count: {distinct_count}"
        )

        # Log and raise exception if duplicates exist
        if total_count > distinct_count:
            logging.warning(f"Duplicate records found in {database}.{schema}.{table}!")
            raise Exception(
                f"Duplicate records detected: {total_count - distinct_count} duplicates found."
            )

    except Exception as e:
        logging.error(f"Error while checking duplicates: {str(e)}")
        raise


# Define DAG
with DAG(
    dag_id="HW6_ELT_DAG",
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=["ELT"],
    schedule="45 2 * * *",  # Runs daily at 2:45 AM
) as dag:

    database = "USER_DB_CAT"
    schema = "analytics"
    table = "session_summary"

    select_sql = """
        SELECT u.*, s.ts
        FROM USER_DB_CAT.raw.user_session_channel u
        JOIN USER_DB_CAT.raw.session_timestamp s 
        ON u.sessionId = s.sessionId;
    """

    # Create the session_summary table
    create_table_task = run_ctas(
        database, schema, table, select_sql, primary_key="sessionId"
    )

    # Check for duplicates
    duplicate_check_task = check_duplicates(database, schema, table)

    # Define task dependencies
    create_table_task >> duplicate_check_task
