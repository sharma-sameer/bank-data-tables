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
BATCH_NUM = 0


def get_connector() -> SnowflakeConnection:
    """
    Function to establish connection with snowflake and return the connection object.

    Args:
        None
    Returns:
        ctx (Connection): A connection object to the snowflake datanase.
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


def to_polars(batch):
    # .to_arrow() is highly efficient for Polars
    logger.info(f"Executing batch number {BATCH_NUM + 1}")
    BATCH_NUM += 1
    return pl.from_arrow(batch.to_arrow())


def get_execution_records() -> pl.DataFrame:
    """
    1. Establish connection to snowflake.
    2. Execute the query and store the data as a pandas DataFrame.
    3. Return the DataFrame.
    Args:
        None
    Returns:
        reports_df (pd.DataFrame): A pandas DataFrame with the report data and the acap_key
    """
    # Establish connection to snowflake.
    ctx = get_connector()
    # Get the cursor
    logger.info("Create a new cursor.")
    cursor = ctx.cursor()
    # Open the sql file to the executed.
    logger.info("Getting the list of sql files to execute.")
    sql_files = get_sql_list(Path.cwd() / "sql")
    logger.info(f"Executing {len(sql_files)} files")
    logger.info("Trying to execute the query.")
    try:
        for file in sql_files:
            with open(file, "r") as query:
                # Run the query and save results as a pandas DataFrame.
                # reports_df = cursor.execute(query.read()).fetch_pandas_all()
                # Using Polars
                # reports_df = pl.read_database(query.read(), ctx).lazy()
                cursor.execute(query.read())
            logger.info("Ran the Query. Consolidating batches.")
            batches = cursor.get_result_batches()
            with ThreadPoolExecutor(max_workers=8) as executor:
                dfs = list(executor.map(to_polars, batches))

            reports_df = pl.concat(dfs)
    except Exception as e:
        logger.error(
            f"Failed to execute the query. Got the following error:",
            exc_info=True,
        )
        return None

    # Return the resulting DataFrame
    return reports_df


if __name__ == "__main__":
    df = get_execution_records()
    df.write_parquet('s3://omwbp-s3-prod-data-science-modeling-shared-data-ue1-all/Sameer_S/bank_data_tables/BATv2/data.parquet')
