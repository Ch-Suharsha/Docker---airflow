from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
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

# SQL to create the joined table (session_summary) and check for duplicates
CREATE_SESSION_SUMMARY = """
CREATE TABLE IF NOT EXISTS dev.analytics.session_summary AS
SELECT
    u.userId,
    u.sessionId,
    u.channel,
    s.ts
FROM dev.raw_data.user_session_channel u
JOIN dev.raw_data.session_timestamp s
ON u.sessionId = s.sessionId;
"""

CHECK_DUPLICATES = """
DELETE FROM dev.analytics.session_summary
USING (
    SELECT sessionId, COUNT(*) AS cnt
    FROM dev.analytics.session_summary
    GROUP BY sessionId
    HAVING COUNT(*) > 1
) dups
WHERE dev.analytics.session_summary.sessionId = dups.sessionId;
"""

# Define the DAG
with DAG(
    'snowflake_elt_session_summary',
    default_args=default_args,
    description='ELT process to create and populate the session_summary table with duplicate record checks',
    schedule_interval=None,  # Set to 'None' for manual triggering, adjust if you want a schedule
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Create user_session_channel table
    create_user_session_channel_table = SnowflakeOperator(
        task_id='create_user_session_channel_table',
        sql="""
        CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
            userId int not NULL,
            sessionId varchar(32) primary key,
            channel varchar(32) default 'direct'
        );
        """,
       snowflake_conn_id='snowflake_conn',
    )

    # Task 2: Create session_timestamp table
    create_session_timestamp_table = SnowflakeOperator(
        task_id='create_session_timestamp_table',
        sql="""
        CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
            sessionId varchar(32) primary key,
            ts timestamp
        );
        """,
       snowflake_conn_id='snowflake_conn',
    )

    # Task 3: Create stage for S3 data
    create_stage = SnowflakeOperator(
        task_id='create_stage',
        sql="""
        CREATE OR REPLACE STAGE dev.raw_data.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """,
       snowflake_conn_id='snowflake_conn',
    )

    # Task 4: Populate user_session_channel table
    populate_user_session_channel = SnowflakeOperator(
        task_id='populate_user_session_channel',
        sql="""
        COPY INTO dev.raw_data.user_session_channel
        FROM @dev.raw_data.blob_stage/user_session_channel.csv;
        """,
       snowflake_conn_id='snowflake_conn',
    )

    # Task 5: Populate session_timestamp table
    populate_session_timestamp = SnowflakeOperator(
        task_id='populate_session_timestamp',
        sql="""
        COPY INTO dev.raw_data.session_timestamp
        FROM @dev.raw_data.blob_stage/session_timestamp.csv;
        """,
       snowflake_conn_id='snowflake_conn',
    )

    # Task 6: Create the session_summary table by joining the two tables
    create_session_summary = SnowflakeOperator(
        task_id='create_session_summary',
        sql=CREATE_SESSION_SUMMARY,
       snowflake_conn_id='snowflake_conn',
    )

    # Task 7: Check for duplicates in session_summary and remove them
    check_and_remove_duplicates = SnowflakeOperator(
        task_id='check_and_remove_duplicates',
        sql=CHECK_DUPLICATES,
        snowflake_conn_id='snowflake_conn',
    )

    # Task dependencies
    create_user_session_channel_table >> create_stage >> populate_user_session_channel
    create_session_timestamp_table >> create_stage >> populate_session_timestamp
    populate_user_session_channel >> populate_session_timestamp >> create_session_summary >> check_and_remove_duplicates
