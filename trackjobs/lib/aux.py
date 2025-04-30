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
Module containing auxiliary functions.
"""

from typing import Any
from typing import Callable


def convert(value: str, dtype: str) -> Any:
    """Convert a string value to a specified data type.

    Args:
        value (str): The string value to be converted.
        dtype (str): The target data type to which the value should be converted. Supported
            types include "Boolean", "Float64", "Float32", and "Int64".

    Returns:
        Any: The converted value in the specified data type.

    Raises:
        ValueError: If the conversion fails or if an unsupported data type is provided.
    """

    match dtype:
        case "Boolean":
            if value.lower().strip() not in {"true", "false"}:
                raise ValueError(f"ERROR: Cannot convert '{value}' to bool!")
            if value.lower().startswith("t"):
                return True
            return False
        case "Float64" | "Float32":
            return __try_convert(value, float)
        case "Int64" | "Int32":
            return __try_convert(value, int)
        case _:
            return value


def __try_convert(value: str, conv_fct: Callable[[str], int | float]) -> int | float:
    """Attempt to convert a string value to a specified numeric type.

    Args:
        value (str): The string value to be converted.
        conv_fct (Callable): The conversion function to use.
            Should be either `int` or `float`.

    Returns:
        Any: The converted value using the provided conversion function.

    Raises:
        ValueError: If the conversion fails due to an invalid value format.
    """

    try:
        value_conv = conv_fct(value)
    except ValueError as exc:
        raise ValueError(f"ERROR: Cannot convert {value} to {conv_fct}") from exc

    return value_conv
