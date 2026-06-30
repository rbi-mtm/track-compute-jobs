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
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

# pylint: disable=import-error
import polars as pl

from .aux import convert
from .default import CMD_FN
from .default import schema_template

# pylint: enable=import-error


def _check_if_id_exists(database: pl.DataFrame, job_id: str) -> bool:
    """Check if a job with the given ID exists in the database.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        job_id (str): The job ID to check.

    Returns:
        bool: True if the job ID exists, False otherwise.
    """

    return len(database.filter(pl.col("ID") == job_id)) > 0


def add_job(
    database: pl.DataFrame, job_id: str, job_name: str, directory: str, job_script: str
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

    missing_cols = set(database.schema).difference(set(job_data))
    if len(missing_cols) > 0:
        job_data.update({k: None for k in missing_cols})

    df_new = pl.DataFrame(job_data).select(database.columns)

    return database.vstack(df_new)


def delete_job(database: pl.DataFrame, job_id: str) -> pl.DataFrame:
    """Delete a job from the database.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        job_id (int): The ID of the job to delete.

    Returns:
        pl.DataFrame: The updated DataFrame with the job removed.
    """
    return database.filter(pl.col("ID") != job_id)


def get_dir(database: pl.DataFrame, job_id: str) -> Path:
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
        return Path(os.getcwd())
    return Path(row["Directory"][0])


def set_status(
    database: pl.DataFrame,
    job_id: str,
    status: str,
    comment: str | None = None,
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
    database = database.with_columns(
        pl.when(pl.col("ID") == job_id)
        .then(pl.lit(status))
        .otherwise(pl.col("Status"))
        .alias("Status")
    )

    if comment is not None:
        database = database.with_columns(
            pl.when(pl.col("ID") == job_id)
            .then(
                pl.when(pl.col("Comments").is_null())
                .then(pl.lit(comment))
                .otherwise(pl.col("Comments") + pl.lit(f"\n{comment}"))
            )
            .otherwise(pl.col("Comments"))
            .alias("Comments")
        )

    if checked:
        database = database.with_columns(
            pl.when(pl.col("ID") == job_id)
            .then(pl.lit(True))
            .otherwise(pl.col("Checked?"))
            .alias("Checked?")
        )

    return database


def set_value(
    database: pl.DataFrame, job_id: str, column: str, value: Any, convert_type=True
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
        if _check_if_id_exists(database, value):
            raise ValueError(
                f"ERROR: Cannot update ID. Another job with ID {value} already exists in database!"
            )

    if convert_type:
        dtype = database[column].dtype
        value = convert(value, dtype)

    database = database.with_columns(
        pl.when(pl.col("ID") == job_id).then(pl.lit(value)).otherwise(pl.col(column)).alias(column)
    )

    return database


def filter_jobs(
    database: pl.DataFrame, key: str, value: str, match_exactly: bool = False
) -> pl.DataFrame:
    """Filter the jobs in the database based on a specific column and value.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        key (str): The column to filter by.
        value (Any): The value to filter for.

    Returns:
        pl.DataFrame: The filtered DataFrame.
    """

    dtype = database[key].dtype
    value = convert(value, dtype)

    if isinstance(dtype, pl.Boolean):
        database = database.filter(pl.col(key).eq(value))
    elif isinstance(dtype, (pl.Int64, pl.Int32)) or match_exactly:
        database = database.filter(pl.col(key) == value)
    else:
        database = database.filter(pl.col(key).str.contains(value))

    return database


def check_status(database: pl.DataFrame, result_list: list[str] | None) -> pl.DataFrame | None:
    """Query the queueing system for the status of unchecked jobs.

    This function requires a file storing the command to query the queueing system. The first line
    of this file gives the command, then every following line can provide additional arguments.
    If the file does not exist, it will be created with a default command for the slurm scheduler.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        result_list (list): list with IDs of jobs running/queueing.

    Returns:
        pl.DataFrame: The updated DataFrame or None if the command for querrying the queueing
            sysem fails.
    """
    if result_list is None:
        result_list = []

    df_unchecked = database.filter(pl.col("Checked?").eq(False)).select(pl.col("ID"))
    unchecked = set(df_unchecked["ID"])

    for line in result_list:
        if not line:
            continue

        job_id, status = line.replace('"', "").strip().split(maxsplit=1)

        # should account for pbs and slurm array jobs:
        job_id = job_id.split("_")[0].split(".")[0].split("[")[0]

        if job_id in unchecked:
            database = set_status(database, job_id, status)
            unchecked.remove(job_id)

    for job_id in unchecked:
        database = set_status(database, job_id, "Finished?")

    return database


def set_status_jobs(
    database: pl.DataFrame,
    job_ids: list[str],
    status: str,
    comment: str | None,
    this: bool = False,
) -> pl.DataFrame:
    """Set the same status to one or more jobs in the database.

    Args:
        database (pl.DataFrame): The DataFrame containing the database.
        job_ids (List[int]): The IDs of thes job to update.
        status (str): The new status for the jobs.
        comment (Optional[str]): An optional comment to add (appends comment to existing comments).
            Defaults to None.
        this (bool): Flag indicating that the status should be set to jobs in current directory.

    Returns:
        pl.DataFrame: The updated DataFrame with the job status and optionally a comment.
    """

    if this:
        cwd = os.getcwd()
        db = filter_jobs(database, "Directory", cwd, True)
        if len(db) == 0:
            print(f"ERROR: No job found in the current working directory ({cwd})!")
            return database
        db = filter_jobs(db, "Checked?", "False")
        job_ids = db["ID"].to_list()

    for job_id in job_ids:
        database = set_status(database, job_id, status, comment, True)

    return database


def compare_jobs(database: pl.DataFrame, job_ids: list[str], this: bool) -> pl.DataFrame:
    """Compare multiple jobs and return differences.

    Filters the database to show selected jobs, then identifies which
    columns have differing values across the selected jobs. Fields that
    are identical across all jobs are printed to stdout and excluded
    from the returned DataFrame.
    Args:
        database (pl.DataFrame): The DataFrame containing the job database.
        job_ids (List[int]): List of job IDs to compare. Can be empty if
            `this` is True.
        this (bool): If True, select jobs from current working directory.
            When True, job_ids parameter is ignored.
    Returns:
        pl.DataFrame: DataFrame containing only columns with differences
            (plus ID and Date columns). Identical fields are printed to stdout.
    Raises:
        ValueError: If `this` is True but no jobs found in current directory.
    """
    if this:
        job_ids = _get_jobs_pwd(database)

    db_comp = database.filter(pl.col("ID").is_in(job_ids))

    if len(db_comp) == 0:
        return db_comp

    key = ["ID"]
    for k in db_comp.drop("ID").schema:
        if len(db_comp[k].unique()) > 1:
            key.append(k)
        else:
            print(f"{k} same for all jobs: {db_comp[k][0]}")

    return db_comp.select(pl.col(key))


def _get_jobs_pwd(
    database: pl.DataFrame, filter_key: str | None = None, filter_value: str | None = None
) -> list[str]:
    """Get job IDs from jobs in the current working directory.

    Filters database for jobs in the current working directory and
    optionally applies an additional filter.

    Args:
        database (pl.DataFrame): The DataFrame containing the job database.
        filter_key (Optional[str]): Optional column name for additional filtering.
        filter_value (Optional[str]): Optional value for additional filtering.
            Must be provided if filter_key is provided.
    Returns:
        List[int]: List of job IDs found in current directory.
    Raises:
        ValueError: If no jobs found in current directory.
    """

    cwd = os.getcwd()
    db = filter_jobs(database, "Directory", cwd, True)
    if len(db) == 0:
        raise ValueError(f"No jobs found in directory: {cwd}")

    if filter_key is not None and filter_value is not None:
        db = filter_jobs(db, filter_key, filter_value)
    return db["ID"].to_list()


def get_unchecked_ids():
    """Retrieve job status lines from the scheduler (e.g., SLURM).

    Reads a command template from CMD_FN (creating the file with a default
    ``squeue`` invocation if it does not exist), executes it as a subprocess,
    and returns the raw stdout lines.

    Returns:
        List[str] or None: List of job-status lines (e.g. ``["12345 PENDING", "12346 RUNNING"]``),
            one per job reported by the scheduler command, or ``None`` if the subprocess failed
            (e.g. squeue not available).
    """

    if not os.path.isfile(CMD_FN):
        os.makedirs(os.path.dirname(CMD_FN), exist_ok=True)
        with open(CMD_FN, "wt", encoding="utf-8") as cmd_fn:
            cmd_fn.write('squeue\n--noheader\n--format="%.18i %.9T"')

    with open(CMD_FN, "rt", encoding="utf-8") as cmd_fn:
        cmd = cmd_fn.readlines()
        cmd = [line.strip() for line in cmd]

    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, check=True)
    except FileNotFoundError:
        print(f"\nERROR: Command ({cmd[0]}) for checking running jobs failed!")
        return None
    except subprocess.CalledProcessError:
        print("\nERROR: Command to check running jobs failed!", end=" ")
        print(f"Make sure the command specified in {CMD_FN} runs without errors!")
        return None

    return result.stdout.decode("utf-8").strip().split("\n")
