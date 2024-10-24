from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# SQL queries for creating the tables and populating data
CREATE_TABLE_USER_SESSION_CHANNEL = """
CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'
);
"""

CREATE_TABLE_SESSION_TIMESTAMP = """
CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp
);
"""

POPULATE_USER_SESSION_CHANNEL = """
COPY INTO dev.raw_data.user_session_channel
FROM @dev.raw_data.blob_stage/user_session_channel.csv;
"""

POPULATE_SESSION_TIMESTAMP = """
COPY INTO dev.raw_data.session_timestamp
FROM @dev.raw_data.blob_stage/session_timestamp.csv;
"""

CREATE_STAGE = """
CREATE OR REPLACE STAGE dev.raw_data.blob_stage
url = 's3://s3-geospatial/readonly/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
"""

# Define the DAG
with DAG(
    'snowflake_etl_user_session',
    default_args=default_args,
    description='A simple ETL process for creating and populating Snowflake tables',
    schedule_interval=None,  # Set to 'None' for manual triggering, adjust if you want a schedule
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Create user_session_channel table
    create_user_session_channel_table = SnowflakeOperator(
        task_id='create_user_session_channel_table',
        sql=CREATE_TABLE_USER_SESSION_CHANNEL,
        snowflake_conn_id='snowflake_conn',
    )

    # Task 2: Create session_timestamp table
    create_session_timestamp_table = SnowflakeOperator(
        task_id='create_session_timestamp_table',
        sql=CREATE_TABLE_SESSION_TIMESTAMP,
        snowflake_conn_id='snowflake_conn',
    )

    # Task 3: Create stage for S3 data
    create_stage = SnowflakeOperator(
        task_id='create_stage',
        sql=CREATE_STAGE,
        snowflake_conn_id='snowflake_conn',
    )

    # Task 4: Populate user_session_channel table
    populate_user_session_channel = SnowflakeOperator(
        task_id='populate_user_session_channel',
        sql=POPULATE_USER_SESSION_CHANNEL,
        snowflake_conn_id='snowflake_conn',
    )

    # Task 5: Populate session_timestamp table
    populate_session_timestamp = SnowflakeOperator(
        task_id='populate_session_timestamp',
        sql=POPULATE_SESSION_TIMESTAMP,
        snowflake_conn_id='snowflake_conn',
    )

    # Define task dependencies
    create_user_session_channel_table >> create_stage >> populate_user_session_channel
    create_session_timestamp_table >> create_stage >> populate_session_timestamp
