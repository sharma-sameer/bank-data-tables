from .get_execution_records import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from .flatten_data import *
from .save_to_database import *


def tabularize_data() -> None:
    """
    Function to consume a batch of data from snowflake transform it for table insertion.

    Args:
        None
    Returns:
        None
    """
    # records_df = get_execution_records()
    logger.info("Getting the list of sql files to execute.")
    sql_files = get_sql_list(Path.cwd() / "sql")

    query = """SELECT IS_DEPRECATED 
        FROM EDS.SB_DATA_SCIENCE.BANK_FEATURES_METADATA 
        WHERE TABLE_NAME = %s"""

    conn = get_connector()
    cursor = conn.cursor()
    sql_files = [
        sql_file
        for sql_file in sql_files
        if not cursor.execute(query, Path(sql_file).stem).fetchone()
    ]

    logger.info(f"Executing {len(sql_files)} files")

    with ThreadPoolExecutor(max_workers=4) as executor:
        # Map the read_file_content function to the list of files
        logger.info(f"{len(sql_files)} tables to create/refresh.")
        sql_scheduled = {
            executor.submit(get_execution_records, sql_file): sql_file
            for sql_file in sql_files
        }

        # Process results as they are completed
        for future in as_completed(sql_scheduled):
            file = sql_scheduled[future]
            try:
                records_df = (
                    future.result()
                )  # Get the result (or raise any exception)
                logger.info(f"Result generated for {file}.")
                table_name = Path(file).stem
                logger.info("Transforming data for table creation/insertion.")
                table_df = flatten_features(records_df, table_name)
                logger.info(f"Table name is {table_name}")
                result = save_to_snowflake(table_df, table_name)
                logger.info(result)

            except Exception as exc:
                logger.error(f"%r generated an exception: %s" % (file, exc))


if __name__ == "__main__":
    df = tabularize_data()
