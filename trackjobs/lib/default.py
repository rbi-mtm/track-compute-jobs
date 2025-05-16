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

"""
Module defining default values
"""

import os

__pydoc__ = {}
__pydoc__["JOB_DB"] = True


JOB_DB = os.path.expanduser("~/job_db.polars")
"""Path where database is stored"""

CMD_FN = os.path.expanduser("~/.config/track_jobs/check_status_command")
"""Path to file that has command that gives job status (e.g. squeue)"""


def schema_template() -> dict:
    """Return a template for a data schema.

    Returns:
        dict: A dictionary where each key is a field name and each value is the expected data
        type for that field.
    """
    schema = {
        "ID": int,
        "Name": str,
        "Job_script": str,
        "Status": str,
        "Checked?": bool,
        "Comments": str,
        "Directory": str,
        "Date": str,
    }

    return schema
