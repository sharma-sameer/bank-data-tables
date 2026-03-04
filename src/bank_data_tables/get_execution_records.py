from pathlib import Path
import os
import botocore
import snowflake.connector as snf
import botocore.session
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
from snowflake.connector.connection import SnowflakeConnection
import json
import logging
import polars as pl
from .get_sql_list import get_sql_list
from concurrent.futures import ThreadPoolExecutor

# Configure the root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Use in your module
logger = logging.getLogger(__name__)

os.environ["AWS_DEFAULT_REGION"] = (
    "us-east-1"  # Set default region for botocore
)
client = botocore.session.get_session().create_client("secretsmanager")
cache_config = SecretCacheConfig()
cache = SecretCache(config=cache_config, client=client)
secret = cache.get_secret_string("snowflake/user-login")
secret_json = json.loads(secret)
batch_num = 0


def get_connector() -> SnowflakeConnection:
    """
    Function to establish connection with snowflake and return the connection object.

    Args:
        None
    Returns:
        ctx (SnowflakeConnection): A connection object to the snowflake database.
    """
    # . Establish connection with the snowflake database.
    logger.info("Establishing connection with the snowflake database.")
    crdntls = {
        "user": secret_json["username"],
        "password": secret_json["password"],
        "account": "onemainfinancial-prod",
    }
    conn = snf.connect(**crdntls)

    # Return the connection object.
    logger.info(
        "Established connection to snowflake. Returning the connection object."
    )
    return conn


def to_polars(batch: snf.result_batch.ArrowResultBatch) -> pl.DataFrame:
    """
    Function to consume a batch of data from snowflake and convert it into polars DataFrame.

    Args:
        batch (snowflake.connector.result_batch.ArrowResultBatch): A batch of the result from the current query execution.
    Returns:
        pl.DataFrame: A polars DataFrame with data from the current batch.
    """
    global batch_num
    logger.info(f"Executing batch number {batch_num + 1}")
    batch_num += 1
    return pl.from_arrow(batch.to_arrow())


def get_execution_records(sql_filename: str) -> pl.DataFrame:
    """
    1. Establish connection to snowflake.
    2. Execute the query and store the data as a polars DataFrame.
    3. Return the DataFrame.
    Args:
        sql_filename (str): sql file location to execute.
    Returns:
        records_df (pl.DataFrame): A polars DataFrame with the data from query execution.
    """
    # Establish connection to snowflake.
    ctx = get_connector()
    # Get the cursor
    logger.info("Create a new cursor.")
    cursor = ctx.cursor()
    # Open the sql file to the executed.
    logger.info("Trying to execute the query.")
    try:
        table_name = Path(sql_filename).stem
        query = """SELECT DATA_AS_OF_DATE 
            FROM EDS.SB_DATA_SCIENCE.BANK_FEATURES_METADATA 
            WHERE TABLE_NAME = %s"""
        latest_time = cursor.execute(query, table_name.upper()).fetchone()
        if latest_time is None:
            latest_time = "1900-01-01 00:00:00"
        with open(sql_filename, "r") as query:
            cursor.execute(query.read(), latest_time)
        logger.info("Ran the Query. Consolidating batches.")
        batches = cursor.get_result_batches()
        with ThreadPoolExecutor(max_workers=8) as executor:
            dfs = list(executor.map(to_polars, batches))

        records_df = pl.concat(dfs)
    except Exception as e:
        logger.error(
            f"Failed to execute the query. Got the following error:",
            exc_info=True,
        )
        return None

    # Return the resulting DataFrame
    return records_df


if __name__ == "__main__":
    df = get_execution_records()
    df.write_parquet(
        "s3://omwbp-s3-prod-data-science-modeling-shared-data-ue1-all/Sameer_S/bank_data_tables/BATv2/data.parquet"
    )
