from .get_execution_records import *
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime

logger.info("Getting snowflake connector to save data.")


def save_to_snowflake(table_df, table_name):
    """ """
    conn = get_connector()
    logger.info("Checking if the table already exists.")
    if (
        conn.cursor().execute(f"SHOW TABLES LIKE '{table_name}'").fetchone()
        is not None
    ):
        logger.info("The table already exists. Need to insert in the table.")
        create_update_table(table_df, table_name, conn)
        conn.cursor().execute(f"""UPDATE TABLE BANK_FEATURES_METADATA 
            SET LAST_REFRESH_DATE = {datetime.now()},
            DATA_AS_OF_DATE = {table_df['MODEL_EXECUTION_TIMESTAMP'].max()} 
            WHERE TABLE_NAME = {table_name.upper()};""")
    else:
        logger.info("The table needs to be created.")
        create_update_table(table_df, table_name, conn)
        query = """INSERT INTO BANK_FEATURES_METADATA (TABLE_NAME, LAST_REFRESH_DATE, DATA_AS_OF_DATE)
            VALUES(%s, 
                   TO_TIMESTAMP(%s), 
                   TO_TIMESTAMP(%s));"""
        conn.cursor().execute(
            query,
            (
                table_name,
                datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
                table_df["MODEL_EXECUTION_TIMESTAMP"]
                .max()
                .strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    return "Saved to snowflake table successfully."


def create_update_table(table_df, table_name, conn):
    df = table_df.to_pandas()
    conn.cursor().execute("USE DATABASE EDS;")
    conn.cursor().execute("USE SCHEMA SB_DATA_SCIENCE;")
    write_pandas(
        conn=conn,
        df=df,
        table_name=table_name.upper(),
        auto_create_table=True,
        overwrite=False,
    )
    return
