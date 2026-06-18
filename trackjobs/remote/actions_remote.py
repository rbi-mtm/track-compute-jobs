#
# Copyright (c) 2026 BerniK86.
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

"""Collection of functions to perform various remote actions via SSH."""

import json
import os
import tempfile
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

# pylint: disable=import-error
import fabric
import polars as pl

# pylint: enable=import-error

from trackjobs.lib import actions
from trackjobs.lib import io


def load_host(host: str) -> Dict[str, Any]:
    """Reads the host configuration from `~/.config/track_jobs/hosts.json`

    Args:
        host (str): The name or identifier of the host to load.

    Returns:
        Dict[str, str]: A dictionary containing the configuration settings for the host.

    Raises:
        KeyError: If the specified host is not found in the hosts configuration file.
    """

    path = os.path.expanduser("~/.config/track_jobs/hosts.json")
    with open(path, "rt", encoding="utf-8") as hosts_file:
        hosts = json.load(hosts_file)

    if host not in hosts:
        raise KeyError(f"Host {host} not found in hosts file: {path}")

    return hosts[host]


def _remote_fetch(conn: fabric.Connection, host_conf: Dict[str, Any]) -> pl.DataFrame:
    """Downloads a remote CSV database file from a host and loads it into a Polars DataFrame.

    This helper function creates a local temporary file to store the remote data
    before reading it into memory. The temporary file is deleted after the
    DataFrame is loaded.

    Args:
        conn (fabric.Connection): An active Fabric connection object (SSH) to the remote host.
        host_conf (Dict[str, Any]): Configuration dictionary containing:
            - 'remote_job_db_path': The absolute path to the CSV file on the remote host.
            - 'hostname': The name of the host (used for error reporting).

    Returns:
        pl.DataFrame: A Polars DataFrame containing the data from the remote CSV.

    Raises:
        FileNotFoundError: If the file at `remote_job_db_path` does not exist on the host.
        ValueError: If the `remote_job_db_path` key is missing or invalid in `host_conf`.
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
        temp_path = tmp_file.name

    try:
        conn.get(host_conf["remote_job_db_path"], temp_path)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Could not find file {host_conf["remote_job_db_path"]} on host {host_conf["hostname"]}"
        ) from exc
    except ValueError as exc:
        raise ValueError("Path for remote job database not set!") from exc
    df_remote = io.load(temp_path)

    if os.path.exists(temp_path):
        os.remove(temp_path)

    return df_remote


def _add_remote_cols(df_remote: pl.DataFrame, host_conf: Dict[str, Any]) -> pl.DataFrame:
    """Renames remote columns and adds host-specific identifiers and localized paths.

    This function transforms the remote DataFrame by renaming generic columns to
    'Remote' versions, generating a unique global ID by combining the hostname
    with the remote ID, and optionally translating remote directory paths
    to local equivalents.

    Args:
        df_remote (pl.DataFrame): The Polars DataFrame fetched from the remote host.
        host_conf (Dict[str, Any]): Configuration dictionary containing:
            - 'hostname': The name of the host.
            - 'remote_base_path': The base directory path on the remote system
              (can be None).
            - 'local_base_path': The corresponding base directory path on the
              local system (can be None).

    Returns:
        pl.DataFrame: The transformed DataFrame with renamed columns, a
            unique 'ID', a 'Host' column, and an updated 'Directory' column.
    """
    df_remote = df_remote.rename({"Directory": "Remote_directory", "ID": "Remote_ID"})
    df_remote = df_remote.with_columns(
        pl.lit(host_conf["hostname"]).alias("Host"),
        (host_conf["hostname"] + pl.col("Remote_ID").cast(pl.Utf8)).alias("ID"),
    )

    if host_conf["remote_base_path"] is not None and host_conf["local_base_path"] is not None:
        df_remote = df_remote.with_columns(
            pl.col("Remote_directory")
            .str.replace(
                host_conf["remote_base_path"].strip("/"), host_conf["local_base_path"].strip("/")
            )
            .alias("Directory")
        )

    return df_remote


def _remote_put(conn: fabric.Connection, host_conf: Dict[str, Any], database: pl.DataFrame):
    """Writes a Polars DataFrame to a CSV and uploads it to a remote host.

    This helper function saves the provided DataFrame to a local temporary CSV file,
    transfers that file to the remote path specified in the host configuration,
    and then cleans up the temporary local file.

    Args:
        conn (fabric.Connection): An active Fabric connection object to the remote host.
        host_conf (Dict[str, Any]): Configuration dictionary containing:
            - 'remote_job_db_path': The destination path on the remote host.
            - 'hostname': The name of the host (used for error reporting).
        database (pl.DataFrame): The Polars DataFrame to be uploaded.

    Raises:
        FileNotFoundError: If the destination path `remote_job_db_path` is invalid
            or unreachable on the remote host.
        ValueError: If the `remote_job_db_path` key is missing or not set in `host_conf`.
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
        temp_path = tmp_file.name

    database.write_csv(temp_path)

    try:
        conn.put(temp_path, host_conf["remote_job_db_path"])
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Could not find file {host_conf["remote_job_db_path"]} on host {host_conf["hostname"]}"
        ) from exc
    except ValueError as exc:
        raise ValueError("Path for remote job database not set!") from exc

    if os.path.exists(temp_path):
        os.remove(temp_path)


def remote_merge_from(
    conn: fabric.Connection, host_conf: Dict[str, Any], db_local: pl.DataFrame
) -> pl.DataFrame:
    """Fetches a remote database and merges its contents into the local DataFrame.

    This function retrieves a remote CSV, processes it to add host-specific
    identifiers, ensures the local DataFrame has all necessary columns to
    prevent schema mismatches, and performs a full outer join (update) based
    on the unique 'ID'.

    Args:
        conn (fabric.Connection): An active Fabric connection object to the remote host.
        host_conf (Dict[str, Any]): Configuration dictionary containing the
            remote path and hostname.
        db_local (pl.DataFrame): The current local Polars DataFrame to be updated.

    Returns:
        pl.DataFrame: A merged Polars DataFrame containing the combined records
            from both the local and remote sources.

    See Also:
        _remote_fetch: Used to retrieve the raw remote data.
        _add_remote_cols: Used to standardize remote columns and IDs.
    """
    db_remote = _remote_fetch(conn, host_conf)
    db_remote = _add_remote_cols(db_remote, host_conf)
    new_cols = [c for c in db_remote.columns if c not in db_local.columns]

    return db_local.with_columns([pl.lit(None).alias(c) for c in new_cols]).update(
        db_remote, on="ID", how="full"
    )


def remote_merge_to(conn: fabric.Connection, host_conf: Dict[str, Any], db_local: pl.DataFrame):
    """Merges local data into a remote database and uploads the result to the host.

    This function retrieves the current remote database, filters the local
    DataFrame for records belonging to the target host, strips away local
    identifiers to revert to the remote schema, and performs a full outer
    join to synchronize the data. The final merged dataset is then uploaded
    back to the remote host.

    Args:
        conn (fabric.Connection): An active Fabric connection object to the remote host.
        host_conf (Dict[str, Any]): Configuration dictionary containing:
            - 'hostname': The name of the host to filter for and upload to.
            - 'remote_job_db_path': The destination path on the remote host.
        db_local (pl.DataFrame): The local Polars DataFrame containing
            aggregated data from multiple hosts.

    See Also:
        _remote_fetch: Used to retrieve the current remote state.
        _remote_put: Used to upload the synchronized DataFrame.
    """
    db_remote = _remote_fetch(conn, host_conf)
    db_local = (
        db_local.filter(pl.col("Host") == host_conf["hostname"])
        .drop("Directory")
        .drop("ID")
        .drop("Host")
        .rename({"Remote_ID": "ID", "Remote_directory": "Directory"})
    )

    db_remote = db_remote.update(db_local, on="ID", how="full")
    _remote_put(conn, host_conf, db_remote)


def get_unchecked_ids(conn: fabric.Connection, host_conf: Dict[str, Any]) -> Optional[List[str]]:
    """Retrieves a list of unchecked job IDs from a remote host.

    This function executes a specific shell command on the remote host to identify
    jobs that have not yet been checked. The resulting IDs are prefixed with the
    hostname to create unique global identifiers.

    Args:
        conn (fabric.Connection): An active Fabric connection object to the remote host.
        host_conf (Dict[str, Any]): Configuration dictionary containing:
            - 'check_jobs_command': The shell command used to list unchecked jobs.
            - 'hostname': The name of the host used to prefix the resulting IDs.

    Returns:
        Optional[List[str]]: A list of unique global IDs (hostname + ID) if
            unchecked jobs are found; None if the command output is empty.
    """
    result = conn.run(host_conf["check_jobs_command"], hide=True)

    if result.stdout:
        return [f"{host_conf["hostname"]}{i.strip()}" for i in result.stdout.strip().split("\n")]
    return None


def pre_submit_checks(host_conf, array):
    """Validates that the host configuration contains the necessary submission commands.

    This function ensures that the appropriate command for submitting jobs
    (either standard or array-based) is present in the configuration. If the
    submission is an array job, it also aliases the array command to the
    standard submission command key for downstream compatibility.

    Args:
        host_conf (Dict[str, Any]): Configuration dictionary for the host.
            Expected to contain 'remote_submit_cmd' or 'remote_submit_array_cmd'.
        array (Optional[Any]): The array specification for the job. If not None,
            the function validates and uses the array submission command.

    Raises:
        KeyError: If the required submission command is missing from `host_conf`
            based on whether `array` is provided.
    """
    if "remote_submit_cmd" not in host_conf and array is None:
        raise KeyError("""'remote_submit_cmd' not specfied in host configuratin file!
            Needed for remote submission of jobs!""")

    if "remote_submit_array_cmd" not in host_conf and array is not None:
        raise KeyError("""'remote_submit_array_cmd' not specfied in host configuratin file!
            Needed for remote submission of jobs!""")

    if array is not None:
        host_conf["remote_submit_cmd"] = host_conf["remote_submit_array_cmd"]


def submit_job(
    conn, host_conf, job_name, job_dir, job_script, array_job_pars: Optional[Tuple[str, str]] = None
) -> Tuple[Optional[str], Optional[str]]:
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-positional-arguments
    """Constructs and executes a remote job submission command on a target host.

    This function replaces placeholders in the submission command (e.g., $JOBNAME,
    $JOBFILE, $START, $END) with actual values, translates the local job directory
    path to a remote path, and executes the command within that directory using
    a Fabric connection.

    Note:
        This function modifies the `host_conf` dictionary in-place by updating
        the 'remote_submit_cmd' value with the substituted strings.

    Args:
        conn (fabric.Connection): An active Fabric connection object to the remote host.
        host_conf (Dict[str, Any]): Configuration dictionary containing:
            - 'remote_submit_cmd': The command template with placeholders.
            - 'local_base_path': Local root directory for path translation.
            - 'remote_base_path': Remote root directory for path translation.
        job_name (str): The name to replace the '$JOBNAME' placeholder.
        job_dir (str): The local path to the job directory.
        job_script (str): The filename/path to replace the '$JOBFILE' placeholder.
        array_job_pars (Optional[Tuple[str, str]]): A tuple of (start, end)
            values to replace '$START' and '$END' placeholders for array jobs.

    Returns:
        Optional[Tuple[str, str]]: A tuple containing (job_output, remote_job_dir)
            if successful; (None, None) if the submission was not executed.

    Raises:
        ValueError: If the remote directory change (cd) fails or the current
            working directory does not match the expected remote path.
    """

    host_conf["remote_submit_cmd"] = host_conf["remote_submit_cmd"].replace("$JOBNAME", job_name)
    host_conf["remote_submit_cmd"] = host_conf["remote_submit_cmd"].replace("$JOBFILE", job_script)
    if array_job_pars is not None:
        host_conf["remote_submit_cmd"] = host_conf["remote_submit_cmd"].replace(
            "$START", array_job_pars[0]
        )
        host_conf["remote_submit_cmd"] = host_conf["remote_submit_cmd"].replace(
            "$END", array_job_pars[1]
        )

    remote_job_dir = job_dir.replace(
        host_conf["local_base_path"].strip("/"), host_conf["remote_base_path"].strip("/")
    )
    
    with conn.cd(remote_job_dir):
        cwd = conn.run("pwd", hide=True)
        if cwd.stdout.strip() != remote_job_dir:
            raise ValueError("Something went wrong when changing the directory on the remote host!")
        if job_script.find("/") > -1:
            conn.run(f"cp {job_script} .")
        res = conn.run(host_conf["remote_submit_cmd"], hide=True)
        return res.stdout.strip(), remote_job_dir
    return None, None


def post_submit_set_values(
    database, host_conf, job_id, job_name, job_dir, job_script, remote_job_dir, comment
):
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-positional-arguments
    """Updates the job database with metadata and remote identifiers after a successful submission.

    This function converts the remote job ID into a unique global ID, initializes
    the job record via `actions.add_job`, and ensures that the necessary remote-tracking
    columns exist in the DataFrame. It then populates the host, remote ID, and
    remote directory paths.

    Args:
        database (pl.DataFrame): The current job database DataFrame.
        host_conf (Dict[str, Any]): Configuration dictionary containing the 'hostname'.
        job_id (str): The raw job ID returned by the remote scheduler.
        job_name (str): The name assigned to the job.
        job_dir (str): The local directory associated with the job.
        job_script (str): The script file submitted.
        remote_job_dir (str): The path to the job directory on the remote host.
        comment (Optional[str]): An optional comment to associate with the job.

    Returns:
        pl.DataFrame: The updated database DataFrame containing the new job
            record and its associated remote metadata.
    """
    job_id = f"{host_conf["hostname"]}{job_id}"
    database = actions.add_job(database, job_id, job_name, job_dir, job_script)
    for col in ["Host", "Remote_ID", "Remote_directory"]:
        if col not in database.schema:
            database = database.with_columns(pl.lit(None).alias(col))
    database = actions.set_value(database, job_id, "Host", host_conf["hostname"])
    database = actions.set_value(
        database, job_id, "Remote_ID", job_id.replace(host_conf["hostname"], "")
    )
    database = actions.set_value(database, job_id, "Remote_directory", remote_job_dir)
    database = actions.set_value(database, job_id, "Status", "submitted")

    if comment is not None:
        database = actions.set_value(database, job_id, "Comments", comment)

    return database
