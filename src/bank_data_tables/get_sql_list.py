"""
This code pulls the list of all the reports in a given directory.
"""

from pathlib import Path
from typing import List


def get_sql_list(directory: Path) -> List[str]:
    """
    Given a directory, grab the list of all the json files in the directory.

    Args:
        directory (Path): Directory location where the json files are stored.

    Returns:
        list: list of all the json files in the directory.

    """
    return list(directory.glob("*.sql"))
