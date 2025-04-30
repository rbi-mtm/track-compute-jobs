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

"""
Module for io operations.
"""

import os

# pylint: disable=import-error
import polars as pl

# pylint: enable=import-error
from .default import schema_template


def load(filename: str) -> pl.DataFrame:
    """Load data from a file into a Polars DataFrame or create empty DataFrame.

    Args:
        filename (str): The path to the file from which data should be loaded.
            If the file exists, it is read as a CSV using `polars.read_csv`.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the data read from the specified file
            or empty DataFrame with specified schema.
    """

    if os.path.isfile(filename):
        return pl.read_csv(filename, has_header=True)

    schema = schema_template()

    return pl.DataFrame(schema=schema)


def save(filename: str, database: pl.DataFrame):
    """Save a Polars DataFrame to a CSV file.

    Args:
        filename (str): The path to the file where the data should be saved.
            The DataFrame is written as a CSV using `polars.write_csv`, including the header row.
        database (pl.DataFrame): The Polars DataFrame to be saved.
    """

    database.write_csv(filename, include_header=True)
