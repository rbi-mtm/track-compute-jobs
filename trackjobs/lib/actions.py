#
# Copyright (c) 2025 BerniK86.
#
# This file is part of track-compute-jobs 
# (see https://github.com/rbi-mtm/track-compute-jobs).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

"""Collection of functions to perform various database actions. Can be called from CLI."""

import os
from datetime import date
from typing import Any
from typing import Optional
import subprocess

# pylint: disable=import-error
import polars as pl

from .aux import convert
from .default import schema_template
from .default import CMD_FN

# pylint: enable=import-error


def _check_if_id_exists(database: pl.DataFrame, job_id: int) -> bool:
    """Check if a job with the given ID exists in the database.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        job_id (int): The job ID to check.

    Returns:
        bool: True if the job ID exists, False otherwise.
    """

    df_test = database.filter(pl.col("ID") == job_id)
    if len(df_test) > 0:
        return True
    return False


def add_job(
    database: pl.DataFrame, job_id: int, job_name: str, directory: str, job_script: str
) -> pl.DataFrame:
    """Add a new job to the database.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        job_id (int): The unique identifier for the job.
        job_name (str): The name of the job.
        directory (str): The directory where the job script is located.
        job_script (str): The path to the job script.

    Returns:
        pl.DataFrame: The updated DataFrame with the new job added.

    Raises:
        ValueError: if there is already a job with job_id in the database.
    """

    id_exists = _check_if_id_exists(database, job_id)
    if id_exists:
        raise ValueError(f"ERROR: Job with ID {job_id} already exists in database!")

    job_data = schema_template()

    job_data["ID"] = job_id
    job_data["Name"] = job_name
    job_data["Directory"] = directory
    job_data["Job_script"] = job_script
    job_data["Status"] = None
    job_data["Checked?"] = False
    job_data["Comments"] = None
    job_data["Date"] = date.today().isoformat()

    df_new = pl.DataFrame(job_data)

    return database.vstack(df_new)


def delete_job(database: pl.DataFrame, job_id: int) -> pl.DataFrame:
    """Delete a job from the database.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        job_id (int): The ID of the job to delete.

    Returns:
        pl.DataFrame: The updated DataFrame with the job removed.
    """
    return database.filter(pl.col("ID") != job_id)


def get_dir(database: pl.DataFrame, job_id: int) -> str:
    """Get the directory of a specific job from the database.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        job_id (int): The ID of the job to retrieve the directory for.

    Returns:
        str: The directory of the job or the current working directory
            if no job with given ID was found.
    """

    row = database.filter(pl.col("ID") == job_id)
    if len(row) == 0:
        print("WARNING: no job with specified ID was found! Returning current directory!")
        return os.getcwd()
    return row["Directory"][0]


def set_status(
    database: pl.DataFrame,
    job_id: int,
    status: str,
    comment: Optional[str] = None,
    checked: bool = False,
) -> pl.DataFrame:
    """Set the status of a specific job in the database.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        job_id (int): The ID of the job to update.
        status (str): The new status for the job.
        comment (Optional[str]): An optional comment to add (appends comment to
            existing comments). Defaults to None.
        checked (bool): Flag indicating if the job was checked by the user. Defaults to False.

    Returns:
        pl.DataFrame: The updated DataFrame with the job status and optionally a comment.
    """

    row_idx = database.with_row_index().filter(pl.col("ID") == job_id)[0, 0]
    status_col = database.get_column_index("Status")
    database[row_idx, status_col] = status

    if comment is not None:
        comm_col = database.get_column_index("Comments")

        if database[row_idx, comm_col] is not None:
            database[row_idx, comm_col] = f"{database[row_idx, comm_col]}{comment}"
        else:
            database[row_idx, comm_col] = f"{comment}"

    if checked:
        fin_col = database.get_column_index("Checked?")
        database[row_idx, fin_col] = True

    return database


def set_value(
    database: pl.DataFrame, job_id: int, column: str, value: Any, convert_type=True
) -> pl.DataFrame:
    """Set a value for a specific column of a job in the database.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        job_id (int): The ID of the job to update.
        column (str): The name of the column to update.
        value (Any): The new value for the column.
        convert_type (bool): Flag indicating whether to convert the value to the
            correct type. Defaults to True.

    Returns:
        pl.DataFrame: The updated DataFrame with the job value set.

    Raises:
        ValueError: When attempting to update job_id with a value that is used
            as job_id for a different job.
    """

    if not _check_if_id_exists(database, job_id):
        raise ValueError(f"ERROR: Cannot update job. No job with ID {job_id} exists in database!")

    if column == "ID":  # make sure that new ID is not used yet in database.
        id_exists = _check_if_id_exists(database, value)
        if id_exists:
            raise ValueError(
                f"ERROR: Cannot update ID. Another job with ID {value} already exists in database!"
            )

    row_idx = database.with_row_index().filter(pl.col("ID") == job_id)[0, 0]
    col_idx = database.get_column_index(column)
    if convert_type:
        dtype = database.dtypes[col_idx]
        value = convert(value, str(dtype))
    database[row_idx, col_idx] = value

    return database


def filter_jobs(database: pl.DataFrame, key: str, value: str) -> pl.DataFrame:
    """Filter the jobs in the database based on a specific column and value.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        key (str): The column to filter by.
        value (Any): The value to filter for.

    Returns:
        pl.DataFrame: The filtered DataFrame.
    """

    col_idx = database.get_column_index(key)
    dtype = str(database.dtypes[col_idx])
    value = convert(value, dtype)
    print(f"{key=}, {value=}, {dtype=}")
    match dtype:
        case "Boolean":
            database = database.filter(pl.col(key).eq(value))
        case "Int64" | "Int32":
            database = database.filter(pl.col(key) == value)
        case _:
            database = database.filter(pl.col(key).str.contains(value))

    return database


def check_status(database: pl.DataFrame) -> pl.DataFrame:
    """Query the queueing system for the status of unchecked jobs.

    This function requires a file storing the command to query the queueing system. The first line
    of this file gives the command, then every following line can provide additional arguments.
    If the file does not exist, it will be created with a default command for the slurm scheduler.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.

    Returns:
        pl.DataFrame: The updated DataFrame.
    """
    unchecked = database.filter(pl.col("Checked?").eq(False)).select(pl.col("ID"))
    unchecked = set(unchecked["ID"])

    if not os.path.isfile(CMD_FN):
        os.makedirs(os.path.dirname(CMD_FN), exist_ok=True)
        with open(CMD_FN, "wt", encoding="utf-8") as cmd_fn:
            cmd_fn.write('squeue\n--noheader\n--format="%.18i %.9T"')

    with open(CMD_FN, "rt", encoding="utf-8") as cmd_fn:
        cmd = cmd_fn.readlines()
        cmd = [line.strip() for line in cmd]

    result = subprocess.run(cmd, stdout=subprocess.PIPE, check=True)
    result_list = result.stdout.decode('utf-8').strip().split("\n")

    for line in result_list:
        job_id, status = line.replace('"', '').strip().split(maxsplit=1)

        # should account for pbs and slurm array jobs:
        job_id = int(job_id.split("_")[0].split(".")[0].split("[")[0])

        if job_id in unchecked:
            database = set_status(database, job_id, status)
            unchecked.remove(job_id)

    for job_id in unchecked:
        database = set_status(database, job_id, "Finished?")

    return database
