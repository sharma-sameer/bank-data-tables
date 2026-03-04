from .get_execution_records import *
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime as dt
import datetime

logger.info("Getting snowflake connector to save data.")


def save_to_snowflake(table_df: pl.DataFrame, table_name: str) -> str:
    """
    Function to save the data to snowflake.

    Args:
        table_df (pl.DataFrame): Data to be inserted.
        table_name (str): Name of the table to which the table is to be inserted.
    Returns:
        str: Status of the data insertion.
    """
    conn = get_connector()
    logger.info("Checking if the table already exists.")
    if (
        conn.cursor().execute(f"SHOW TABLES LIKE '{table_name}'").fetchone()
        is not None
    ):
        logger.info("The table already exists. Need to insert in the table.")
        insert_time = dt.now(datetime.timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        create_update_table(table_df, table_name, conn)
        logger.info(
            "Data inserted successfully. Now need to update the Metadata table to reflect the refresh time."
        )
        query = """UPDATE TABLE BANK_FEATURES_METADATA 
            SET LAST_REFRESH_DATE = TO_TIMESTAMP(%s),
            DATA_AS_OF_DATE = TO_TIMESTAMP(%s) 
            WHERE TABLE_NAME = %s{};"""
        conn.cursor().execute(
            query,
            (
                insert_time,
                table_df["MODEL_EXECUTION_TIMESTAMP"]
                .max()
                .strftime("%Y-%m-%d %H:%M:%S"),
                table_name.upper(),
            ),
        )
    else:
        logger.info("The table needs to be created.")
        insert_time = dt.now(datetime.timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        create_update_table(table_df, table_name, conn)
        logger.info(
            "Data inserted successfully. Now need to update the Metadata table to reflect the refresh time."
        )
        query = """INSERT INTO BANK_FEATURES_METADATA (TABLE_NAME, LAST_REFRESH_DATE, DATA_AS_OF_DATE)
            VALUES(%s, 
                   TO_TIMESTAMP(%s), 
                   TO_TIMESTAMP(%s));"""
        conn.cursor().execute(
            query,
            (
                table_name.upper(),
                insert_time,
                table_df["MODEL_EXECUTION_TIMESTAMP"]
                .max()
                .strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    return "Saved to snowflake table successfully."


def create_update_table(
    table_df: pl.DataFrame, table_name: str, conn: SnowflakeConnection
) -> None:
    """
    Function to run the insert or create table command in snowflake.

    Args:
        table_df (pl.DataFrame): Data to be inserted.
        table_name (str): Name of the table to which the table is to be inserted.
        conn (SnowflakeConnection): A connection object to the snowflake database.
    Returns:
        None
    """
    logger.info(
        "Snowflake only supports pandas so need to convert the dataframe to pandas."
    )
    df = table_df.to_pandas()
    logger.info("Using Database EDS.")
    conn.cursor().execute("USE DATABASE EDS;")
    logger.info("Using Database SB_DATA_SCIENCE.")
    conn.cursor().execute("USE SCHEMA SB_DATA_SCIENCE;")
    logger.info("Inserting Data.")
    write_pandas(
        conn=conn,
        df=df,
        table_name=table_name.upper(),
        auto_create_table=True,
        overwrite=False,
    )
    return
