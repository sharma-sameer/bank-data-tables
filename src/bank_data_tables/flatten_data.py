import polars as pl
from pathlib import Path
import csv
import polars.selectors as cs
from .get_execution_records import logger
from typing import List


def flatten_features(records_df: pl.DataFrame, table_name: str) -> pl.DataFrame:
    """
    Function to convert the features from row values to columns.

    Args:
        records_df (pl.DataFrame): Data to be flattened.
        table_name (str): Name of the table to which the table is to be inserted.
    Returns:
        table_df (pl.DataFrame): Data to be inserted to the snowflake table.
    """
    logger.info("Performing column transformations.")
    records_df = records_df.with_columns(
        pl.col("MODEL_EXECUTION_TIMESTAMP").dt.date().alias("EXECUTION_DATE")
    )
    records_df = records_df.with_columns(
        pl.col("EXECUTION_DATE").dt.strftime("%Y%m").alias("YYYYMM")
    )
    unique_months = records_df.select(pl.col("YYYYMM").unique())[
        "YYYYMM"
    ].to_list()
    unique_features = records_df["FEATURE"].unique()

    pivoted_chunks = []

    logger.info("Pivoting the dataframe.")

    for month in unique_months:
        chunk = records_df.filter(pl.col("YYYYMM") == month)
        pivoted_chunk = chunk.pivot(
            index=[
                "ACAP_KEY",
                "MODEL_EXECUTION_TIMESTAMP",
                "EXL_CONTAINER_VERSION",
            ],
            on="FEATURE",
            values="VALUE",
            on_columns=unique_features,
            # An aggregation function is needed if the index and columns combination isn't unique
            # For this example, 'first' works as the original data has unique combinations
            aggregate_function="first",
        )
        pivoted_chunks.append(pivoted_chunk)

    final_df = pl.concat(pivoted_chunks, how="diagonal")
    logger.info(f"Successfully pivoted the data. Got {len(final_df)} records.")
    table_cols = get_table_cols(table_name)
    # logger.info(f"The required table columns are {table_cols}")
    logger.info("Keeping only the columns needed for the table.")
    # logger.info(f"The columns in the dataframe right now are: {final_df.columns}")
    table_df = final_df.select(table_cols[0])
    table_df = table_df.with_columns(
        pl.col("ACAP_KEY").str.replace_all('"', "").alias("ACAP_KEY")
    )
    table_df = table_df.with_columns(
        pl.col("ACAP_KEY").replace("", "0").cast(pl.Int64)
    )
    table_df = table_df.with_columns(
        (
            cs.string().exclude(
                [
                    "ACAP_KEY",
                    "MODEL_EXECUTION_TIMESTAMP",
                    "EXL_CONTAINER_VERSION",
                ]
            )
        ).cast(pl.Float64)
    )
    logger.info("Data transformation completed.")
    return table_df


def get_table_cols(table_name: str) -> List:
    """
    Function to get the table columns.

    Args:
        table_name (str): Name of the table to which the table is to be inserted.
    Returns:
        table_cols (List): List of columns for this table.
    """
    table_cols = []
    logger.info("Reading the list of valid table columns.")
    file = table_name + ".csv"
    filename = Path.cwd() / "table_columns" / file
    with open(filename, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            table_cols.append(row)
    return table_cols
