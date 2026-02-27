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
    logger.info("Trying to execute the query.")
    try:
        with open(Path.cwd() / "sql" / "pull_data.sql", "r") as query:
            # Run the query and save results as a pandas DataFrame.
            # reports_df = cursor.execute(query.read()).fetch_pandas_all()
            # Using Polars
            reports_df = pl.read_database(query.read(), ctx)
    except Exception as e:
        logger.error(
            f"Failed to execute the query. Got the following error:",
            exc_info=True,
        )
        return None

    # Return the resulting DataFrame
    return reports_df

if __name__ == '__main__':
    df = get_reports()
    df.glimpse()