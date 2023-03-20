from datetime import timedelta
import re
from typing import Tuple

from jobmon.core.exceptions import InvalidMemoryFormat, InvalidMemoryUnit


class TimeUnit:
    """A helper class provides static functions to switch among seconds, minutes, and hours.

    It also can be initialized with combined input of seconds, minutes, and hours,
    and converts them to any single time unit.
    """

    @staticmethod
    def min_to_sec(minutes: float) -> int:
        """Static helper function to turn minutes into seconds. Accept pointers."""
        return int(minutes * 60)

    @staticmethod
    def hour_to_sec(hour: float) -> int:
        """Static helper function to turn minutes into seconds."""
        return int(hour * 60 * 60)

    @staticmethod
    def hour_to_min(hour: float) -> float:
        """Static helper function to turn minutes into seconds."""
        return round(hour * 60.0, 2)

    @staticmethod
    def min_to_hour(min: float) -> float:
        """Static helper function to turn minutes into seconds."""
        return round(min / 60.0, 2)

    @staticmethod
    def sec_to_hour(sec: int) -> float:
        """Static helper function to turn minutes into seconds."""
        return round(sec / 3600.0, 2)

    @staticmethod
    def sec_to_min(sec: int) -> float:
        """Static helper function to turn minutes into seconds."""
        return round(sec / 60.0, 2)

    def __init__(self, sec: int = 0, min: float = 0.0, hour: float = 0.0) -> None:
        """The constructor."""
        self.seconds = sec + TimeUnit.min_to_sec(min) + TimeUnit.hour_to_sec(hour)
        self.minutes = TimeUnit.sec_to_min(sec) + min + TimeUnit.hour_to_min(hour)
        self.hours = TimeUnit.sec_to_hour(sec) + TimeUnit.min_to_hour(min) + hour
        self.readable = str(timedelta(seconds=self.seconds))


class MemUnit:
    """A helper class to convert memory units between B, k, K, m, M, g, G, t, T."""

    base_chart_to_B = {"B": 1, "K": 1024, "M": 1.049e6, "G": 1.074e9, "T": 1.1e12}

    @staticmethod
    def _split_unit(input: str) -> Tuple[int, str]:
        """Split the number and the unit. Return 0 M for invalid; use M when no unit."""
        values = re.findall(r"^\d+", input)
        if len(values) == 0:
            return 0, "M"
        value = values[0]
        units = re.findall(r"\D+$", input)
        if len(units) == 0:
            unit = "M"
        else:
            unit = units[0]
            unit = re.sub("[B|b].*", "B", unit)
            unit = re.sub("[K|k].*", "K", unit)
            unit = re.sub("[M|m].*", "M", unit)
            unit = re.sub("[G|g].*", "G", unit)
            unit = re.sub("[T|t].*", "T", unit)
            if unit not in MemUnit.base_chart_to_B.keys():
                unit = "M"
        return int(value), unit

    @staticmethod
    def _to_B(input: str) -> int:
        value, unit = MemUnit._split_unit(input)
        return int(value * MemUnit.base_chart_to_B[unit])

    @staticmethod
    def convert(input: str, to: str = "M") -> int:
        """Convert memory to different units.

        input: a string in format "1G", "1g", "1Gib", "1gb",etc.
        to: the unit to convert to; takes B, K, M, G, T.
        Lower case unit will be treated as capital.
        Return: an int of the memory value of your specified unit
        """
        input = str(input)
        # match 100, 100m, 100M, 100 Gbetc
        r1 = r"^\d+[bBkKmMgGtT]?[bB]?$"
        # match 100Gib, 100gib, etc
        r2 = r"^\d+[bBkKmMgGtT]i[bB]$"
        if re.search(r1, input) is None and re.search(r2, input) is None:
            raise InvalidMemoryFormat(
                f"Input {input} is invalide. Please use format "
                f"such as 100, 100M, 100Gb, or 100Gib."
            )
        value = MemUnit._to_B(input)
        # apply same input check logic to "to"
        if to.upper() in ("B", "BB", "BIB"):
            to = "B"
        if to.upper() in ("K", "KB", "KIB"):
            to = "K"
        if to.upper() in ("M", "MB", "MIB"):
            to = "M"
        if to.upper() in ("G", "GB", "GIB"):
            to = "G"
        if to.upper() in ("T", "TB", "TIB"):
            to = "T"
        if to not in MemUnit.base_chart_to_B.keys():
            raise InvalidMemoryUnit(
                f"Jobmon can only convert memory unit to "
                f"{MemUnit.base_chart_to_B.keys()}. {to} is invalid."
            )
        return round(value / MemUnit.base_chart_to_B[to])
