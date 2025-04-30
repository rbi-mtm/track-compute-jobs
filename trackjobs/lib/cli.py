#
# Copyright (c) 2025 Berni_K.
#
# This file is part of track-compute-jobs
# (see https://codeberg.org/Berni_K/track-compute-jobs).
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

"""Module defining command line interface (CLI).

__Note__: due to `click` not setting `__wrapped__` attribute for decorated functions
pdoc3 cannot create the documentation for this module properly. Please check
source code.
"""

import os
import sys
from typing import Optional

# pylint: disable=import-error
import click
import polars as pl

from . import actions
from .default import JOB_DB
from .io import load
from .io import save

# pylint: enable=import-error


requires_id = click.option("-I", "job_id", required=True, type=click.INT, help="Job ID")
requires_multiple_ids = click.option(
    "-I", "job_ids", required=True, type=click.INT, help="Job ID", multiple=True
)
requires_name = click.option("-N", "job_name", required=True, help="Job Name")
requires_key = click.option("--key", required=True, help="Field to select column.")
requires_value = click.option("--value", required=True, help="New value for field.")

opt_id = click.option("-I", "job_id", type=click.INT, help="Job ID")
opt_multiple_ids = click.option("-I", "job_ids", type=click.INT, help="Job ID", multiple=True)
opt_name = click.option("-N", "job_name", help="Job Name")
opt_script = click.option("-S", "job_script", help="Job Script")
opt_dir = click.option("-D", "job_dir", help="Job Directory")
opt_comment = click.option("-C", "comment", help="Comments")
opt_status = click.option("-T", "job_status", help="Job status")


@click.group(
    "cli",
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True,
)
@click.pass_context
def cli(ctx):
    """CLI entry point"""

    ctx.obj = load(JOB_DB)
    if ctx.invoked_subcommand is None:
        ctx.invoke(show_unfinished)


@cli.result_callback()
def results(database: Optional[pl.DataFrame] = None):
    """Callback function called before program ends. Used to save database.

    Args:
        database (Optional[pl.DataFrame]): Dataframe to write to disk.
    """
    if database is not None:
        save(JOB_DB, database)


@cli.command(
    short_help="""Add job to database. Requires specification of ID (-I), Name (-N).
                  Optionally, a submission script (-S), a job directory (-D), comments (-C)
                  and a job status (-T) can be specified."""
)
@click.pass_obj
@requires_id
@requires_name
@opt_script
@opt_dir
@opt_comment
@opt_status
def add(database, job_id, job_name, job_script, job_dir, comment, job_status):
    # pylint: disable=too-many-arguments, too-many-positional-arguments
    """Add job to database."""
    if job_dir is None:
        job_dir = os.getcwd()
    database = actions.add_job(database, job_id, job_name, job_dir, job_script)
    if comment is not None:
        database = actions.set_value(database, job_id, "Comments", comment)
    if job_status is not None:
        database = actions.set_status(database, job_id, job_status, None, False)

    return database


@cli.command("del", short_help="""Delete job from database. Requires specification of ID (-I).""")
@click.pass_obj
@requires_multiple_ids
def delete(database, job_ids):
    """Delete job from database."""
    for job_id in job_ids:
        database = actions.delete_job(database, job_id)
    return database


@cli.command(
    short_help="""Modify job in database. Select job by ID (-I), specify the filed to
                  modify (--key) and its new value (--value)."""
)
@click.pass_obj
@requires_id
@requires_key
@requires_value
def mod(database, job_id, key, value):
    """Modify job in database."""
    database = actions.set_value(database, job_id, key, value)
    return database


@cli.command("print-dir", short_help="Print directory of job selected by ID (-I).")
@click.pass_obj
@requires_id
def print_dir(database, job_id):
    """Print directory of job."""
    directory = actions.get_dir(database, job_id)
    click.echo(directory)


@cli.command(
    short_help="""Show job selected by ID (-I). -I can be specified multiple times
                  to show multiple jobs."""
)
@click.pass_obj
@requires_multiple_ids
def show(database, job_ids):
    """Show selected job."""
    with pl.Config(tbl_cols=-1, set_tbl_rows=-1, fmt_str_lengths=500):
        click.echo(database.filter(pl.col("ID").is_in(job_ids)))


@cli.command("show-all", short_help="Show all jobs in database.")
@click.pass_obj
def show_all(database):
    """Show all jobs in database."""
    with pl.Config(tbl_cols=-1, set_tbl_rows=-1, fmt_str_lengths=80):
        click.echo(database.select(pl.col("*").exclude("Directory")))


@cli.command(
    "show-unfinished",
    short_help="""Show all jobs with status 'unfinished'. This is the
                  default mode in case no subcommand is specified.""",
)
@click.pass_obj
def show_unfinished(database):
    """Show all jobs with status 'unfinished'."""
    pl.Config(tbl_rows=-1)
    click.echo(
        database.filter(pl.col("Finished").eq(False)).select(pl.col("*").exclude("Directory"))
    )


@cli.command(
    "filter",
    short_help="""Show filtered database by selecting column (--key) and
                  specifying string or value (--value) to filter for.""",
)
@click.pass_obj
@requires_key
@requires_value
@click.option("-v", "--verbose", is_flag=True, help="Show longer output.")
def filter_jobs(database, key, value, verbose):
    """Filter database."""
    n_chars = 500 if verbose else 80
    database = actions.filter_jobs(database, key, value)
    with pl.Config(tbl_cols=-1, set_tbl_rows=-1, fmt_str_lengths=n_chars):
        click.echo(database.select(pl.col("*").exclude("Directory")))


@cli.command(
    "set-fail",
    short_help="""Set job status of specific job(s) selected by ID (-I, more than one can be
                  specified) to FAILED and mark as finished. Optionally, a comment can be
                  appended to any existing comments (-C, same comment for all selected jobs)""",
)
@click.pass_obj
@requires_multiple_ids
@opt_comment
def set_fail(database, job_ids, comment):
    """Set job status to FAILED and mark as finished.

    Note: multiple jobs can be selected to set the status, but the comment will be the same for
    all selected jobs.
    """
    for job_id in job_ids:
        database = actions.set_status(database, job_id, "FAILED", comment, True)
    return database


@cli.command(
    "set-ok",
    short_help="""Set job status of specific job(s) selected by ID (-I, more than one can be
                  specified) to OK and mark as finished. Optionally, a comment can be
                  appended to any existing comments (-C, same comment for all selected jobs)""",
)
@click.pass_obj
@requires_multiple_ids
@opt_comment
def set_ok(database, job_ids, comment):
    """Set job status to OK and mark as finished.

    Note: multiple jobs can be selected to set the status, but the comment will be the same for
    all selected jobs.
    """
    for job_id in job_ids:
        database = actions.set_status(database, job_id, "OK", comment, True)
    return database


@cli.command(
    "update-id",
    short_help="Replace ID (-I) with new value (--value; must be of type int).",
)
@click.pass_obj
@requires_id
@requires_value
def update_id(database, job_id, value):
    """Replace job ID with a new one."""
    try:
        new_id = int(value)
    except ValueError:
        click.echo("ERROR: New ID must be integer!")
        sys.exit(-1)
    database = actions.set_value(database, job_id, "ID", new_id)
    return database


@cli.command(
    short_help="""Sort database by selected column (--key) in ascending order
                  and print it on screen.
                  Descending order can be requested using --desc flag.
                  Use -s/--save to write sorted database back to file."""
)
@click.pass_obj
@requires_key
@click.option("--desc", is_flag=True, help="Sort in descending order")
@click.option("-s", "--save", "save_sorted", is_flag=True, help="Save sorted database")
def sort(database, key, desc, save_sorted):
    """Sort database."""
    database = database.sort(key, descending=desc)

    with pl.Config(tbl_cols=-1, set_tbl_rows=-1, fmt_str_lengths=80):
        click.echo(database.select(pl.col("*").exclude("Directory")))

    if save_sorted:
        return database
    return None
