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

"""Module defining remote subcommand of command line interface (CLI)."""

import os
import shlex

# pylint: disable=import-error
# import click
import fabric
import polars as pl
import rich_click as click

# pylint: enable=import-error

from trackjobs.lib import actions
from trackjobs.lib.cli_flags import opt_comment
from trackjobs.lib.cli_flags import opt_dir
from trackjobs.lib.cli_flags import requires_id
from trackjobs.lib.cli_flags import requires_name
from trackjobs.lib.cli_flags import requires_script
from trackjobs.remote import actions_remote


@click.group(
    "remote",
    short_help="Remote actions via ssh",
    help="CLI entry point for remote host operations via SSH. ",
)
@click.pass_context
@click.option("-H", "--host", required=True)
def remote(ctx, host: str):
    """CLI entry point for remote host operations via SSH.

    Initializes the remote connection and loads the host-specific
    configuration. By utilizing the Click context object (`ctx.obj`), it
    makes the connection and configuration available to all nested
    subcommands (e.g., push, pull, submit).

    Args:
        ctx (click.Context): The Click context object used to pass state
            between the group and its subcommands.
        host (str): The host identifier as specfied in ~/.ssh/config

    Note:
        The following keys are added to `ctx.obj`:
            - 'host_conf': The configuration dictionary loaded via `actions_remote.load_host`.
            - 'conn': An active `fabric.Connection` instance for the specified host.
    """
    ctx.obj["host_conf"] = actions_remote.load_host(host)
    ctx.obj["host_conf"]["hostname"] = host
    ctx.obj["conn"] = fabric.Connection(host)


@remote.command(
    "pull",
    short_help="""Pull jobs database from remote host and merge with local one.
    Note that the job_db.polars file must exist on the remote host.""",
)
@click.pass_obj
def remote_pull(obj: dict) -> pl.DataFrame:
    """Command to synchronize the local job database with data from a remote host.

    This command extracts the connection, host configuration, and local database
    from the Click context object and invokes the `remote_merge_from` logic.
    It ensures that the local database is updated with all records currently
    existing on the remote server.

    Args:
        obj (Dict): The Click context object containing:
            - 'db': The current local Polars DataFrame.
            - 'conn': The active Fabric connection to the remote host.
            - 'host_conf': The configuration dictionary for the target host.

    Returns:
        pl.DataFrame: The merged Polars DataFrame containing synchronized
            data from both local and remote sources.

    Note:
        This command assumes that the remote database file exists at the path
        specified in the host configuration. If the file is missing, a
        FileNotFoundError will be raised by the underlying fetch logic.
    """
    database = obj["db"]
    return actions_remote.remote_merge_from(obj["conn"], obj["host_conf"], database)


@remote.command(
    "push",
    short_help="""Push jobs from local database to remote host databaes.
        Only jobs from the selected host will be pushed. Note that the job_db.polars file must
        exist on the remote host.""",
)
@click.pass_obj
def remote_push(obj: dict):
    """Command to synchronize a remote host's database with a subset of the local database.

    This command extracts the connection and host configuration from the Click context
    object and invokes the `remote_merge_to` logic. It filters the local database to
    isolate records belonging to the target host, merges them with the existing remote
    database, and uploads the result back to the server.

    Args:
        obj (Dict): The Click context object containing:
            - 'db': The current local Polars DataFrame.
            - 'conn': The active Fabric connection to the remote host.
            - 'host_conf': The configuration dictionary for the target host.

    Note:
        This operation is selective; only records where the 'Host' column matches
        the current `host_conf["hostname"]` will be pushed to the remote server.
        As with the pull command, the remote database file must already exist on the
        host to be updated.
    """
    database = obj["db"]
    actions_remote.remote_merge_to(obj["conn"], obj["host_conf"], database)


@remote.command(
    "check-status",
    short_help="Remotely check status of jobs.",
    help="""This command performs a multi-step synchronization process:
    1. If a remote database path is configured, it first pulls and merges
       the remote data into the local database.
    2. It queries the remote host for a list of job IDs that have not yet
       been checked.
    3. It updates the local database by checking the status of those
       specific IDs via `actions.check_status`.
4. Optionally, it filters and prints all currently unchecked jobs associated with the target host
to the console.""",
)
@click.pass_obj
@click.option(
    "--print", "print_unchecked", is_flag=True, help="Print unchecked jobs after checking status"
)
def remote_check_status(obj: dict, print_unchecked: bool):
    """Remotely verify the status of jobs and update the local database.

    This command performs a multi-step synchronization process:
    1. If a remote database path is configured, it first pulls and merges
       the remote data into the local database.
    2. It queries the remote host for a list of job IDs that have not yet
       been checked.
    3. It updates the local database by checking the status of those
       specific IDs.
    4. Optionally, it filters and prints all currently unchecked jobs
       associated with the target host to the console.

    Args:
        obj (Dict): The Click context object containing:
            - 'db': The current local Polars DataFrame.
            - 'conn': The active Fabric connection to the remote host.
            - 'host_conf': The configuration dictionary for the target host.
        print_unchecked (bool): A flag indicating whether to print the
            list of unchecked jobs for the current host after processing.

    Returns:
        pl.DataFrame: The updated database DataFrame with current job statuses.

    Note:
        When `print_unchecked` is True, the function adjusts Polars configuration
        to ensure all rows and columns are displayed in the terminal, and
        strips directory-related columns for cleaner output.
    """
    database = obj["db"]
    host_conf = obj["host_conf"]
    conn = obj["conn"]
    if host_conf["remote_job_db_path"] is not None:
        try:
            result = actions_remote.remote_merge_from(conn, host_conf, database)
        except FileNotFoundError:
            pass
        else:
            database = result
    unchecked_ids = actions_remote.get_unchecked_ids(conn, host_conf)
    db_host = database.filter(pl.col("Host") == host_conf["hostname"])
    db_host = actions.check_status(db_host, unchecked_ids)
    database = database.update(db_host, on="ID", how="left")  # all jobs in db_host are in database

    if print_unchecked:
        print_database = (
            database.filter(pl.col("Checked?").eq(False))
            .filter(pl.col("Host").eq(host_conf["hostname"]))
            .select(pl.col("*").exclude("Directory"))
            .select(pl.col("*").exclude("Remote_directory"))
        )
        with pl.Config(tbl_cols=-1, set_tbl_rows=-1):
            click.echo(print_database)

    return database


@remote.command(
    "submit",
    short_help="""Remotely submit a job on the specified host. Note that the
    command for submission needs to be specified in ~/.config/track_jobs/hosts.json. Also note that
    the data needed to run the job must already be present on the host in the specfied directory
    (or rather, the directory on the host corresponding to the specified directory).""",
)
@click.pass_obj
@requires_script
@requires_name
@opt_dir
@opt_comment
@click.option(
    "-a",
    "array",
    nargs=2,
    type=str,
    help="If set, will start an array job with array indices from a[0] to a[1].",
)
def remote_submit(obj, job_script, job_name, job_dir, comment, array) -> pl.DataFrame | None:
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-positional-arguments
    """Submits a job to a remote host and records the submission in the local database.

    This command manages the end-to-end submission process:
    1. Validates the host configuration for the required submission commands.
    2. Translates the local job directory to its remote counterpart.
    3. Executes the submission command on the remote host via Fabric.
    4. Updates the local job database with the resulting remote job ID and
       associated metadata.

    Args:
        obj (Dict): The Click context object containing:
            - 'db': The current local Polars DataFrame.
            - 'conn': The active Fabric connection to the remote host.
            - 'host_conf': The configuration dictionary for the target host.
        job_script (str): The name/path of the script to be submitted.
        job_name (str): The name assigned to the job.
        job_dir (str): The local path to the job directory.
        comment (Optional[str]): An optional note associated with the job.
        array (Optional[Tuple[str, str]]): A tuple of start and end indices for
            array jobs. Note: Currently raises NotImplementedError if provided.

    Returns:
        Optional[pl.DataFrame]: The updated database DataFrame if submission
            was successful; None if the submission failed.

    Raises:
        NotImplementedError: If an array job is attempted.
        KeyError: If the submission command is missing from the host configuration
            (via `pre_submit_checks`).
        ValueError: If the remote directory change fails during submission.

    Note:
        Input validation for `job_script`, `job_name`, and `job_dir` is handled
        by the `@requires_*` decorators prior to function execution.
    """
    database = obj["db"]
    host_conf = obj["host_conf"]
    conn = obj["conn"]

    if job_dir is None:
        job_dir = "."

    actions_remote.pre_submit_checks(host_conf, array)

    job_dir = os.path.realpath(job_dir)
    job_id, remote_job_dir = actions_remote.submit_job(
        conn, host_conf, job_name, job_dir, job_script, array
    )
    if job_id is None or remote_job_dir is None:
        click.echo("Something went wrong during submission!")
        return None

    database = actions_remote.post_submit_set_values(
        database, host_conf, job_id, job_name, job_dir, job_script, remote_job_dir, comment
    )

    return database


@remote.command("cancel", short_help="Cancel a job remotely.")
@click.pass_obj
@requires_id
@opt_comment
def remote_cancel(obj, job_id, comment) -> pl.DataFrame:
    """Cancels a remote job and updates its status in the local database.

    This command extracts the global job ID, strips the hostname prefix to
    retrieve the original remote job ID, and executes the host-specific
    cancellation command via Fabric. After the remote command is issued,
    the local database is updated to mark the job as 'CANCELLED'.

    Args:
        obj (Dict): The Click context object containing:
            - 'db': The current local Polars DataFrame.
            - 'conn': The active Fabric connection to the remote host.
            - 'host_conf': The configuration dictionary for the target host.
        job_id (str): The unique global job ID (including hostname prefix).
        comment (Optional[str]): An optional note explaining why the job
            was cancelled.

    Returns:
        pl.DataFrame: The updated database DataFrame with the job status
            set to 'CANCELLED'.

    Note:
        This function relies on the 'remote_cancel_cmd' key in the host
        configuration, which must contain the '$JOBID' placeholder for
        proper substitution. Input validation for `job_id` is handled
        by the `@requires_id` decorator.
    """
    database = obj["db"]
    host_conf = obj["host_conf"]
    conn = obj["conn"]

    remote_job_id = job_id.removeprefix(host_conf["hostname"])
    conn.run(host_conf["remote_cancel_cmd"].replace("$JOBID", shlex.quote(remote_job_id)))
    database = actions.set_status(database, job_id, "CANCELLED", comment, True)

    return database
