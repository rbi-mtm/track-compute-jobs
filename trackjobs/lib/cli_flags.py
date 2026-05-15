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

"""Definition of often used CLI flags."""

# pylint: disable=import-error
# import click
import rich_click as click

requires_id = click.option("-I", "job_id", required=True, help="Job ID")
requires_multiple_ids = click.option("-I", "job_ids", required=True, help="Job ID", multiple=True)
requires_name = click.option("-N", "job_name", required=True, help="Job Name")
requires_script = click.option("-S", "job_script", required=True, help="Job Script")
requires_dir = click.option("-D", "job_dir", required=True, help="Job Directory")
requires_key = click.option("--key", required=True, help="Field to select column.")
requires_value = click.option("--value", required=True, help="New value for field.")

opt_id = click.option("-I", "job_id", help="Job ID")
opt_multiple_ids = click.option("-I", "job_ids", help="Job ID", multiple=True)
opt_name = click.option("-N", "job_name", help="Job Name")
opt_script = click.option("-S", "job_script", help="Job Script")
opt_dir = click.option("-D", "job_dir", help="Job Directory")
opt_comment = click.option("-C", "comment", help="Comments")
opt_status = click.option("-T", "job_status", help="Job status")

flg_this = click.option(
    "--this",
    "this",
    is_flag=True,
    help="""Enabling this flag filters jobs based on current directory. Note: the function used to
            determine the current directory (os.getcwd) may give a different path than the 'pwd'
            shell command if the path contains symbolic links. If you use 'pwd' to add jobs to
            the database, make sure that it produces the same output as 'os.getcwd()'. If this is
            not the case, use 'pwd -P' to determine the current directory when adding jobs to
            the database.""",
)
