import csv
from pathlib import Path
from typing import Generator, List, Union

import pandas as pd
from rich.progress import Progress

DEFAULT_NEWLINE_CHAR = "\r\n"


def _merge_rows(
    row1: List[str], row2: List[str], newline_char: str = DEFAULT_NEWLINE_CHAR
) -> List[str]:
    """For combining two rows of a "malformed" record when stepping through an
    RPDR *.txt file.

    This situation mainly occurs when there is an unexpected line break in the
    middle of a string/text field (e.g. Report_Text).

    The lists are merged by concatenating the two with their "innermost"
    elements (strings) concatenated, separated by a newline.

    Example:

        Expected number of fields: 4

        ```python
        row1 = ["A", "B", "C"]
        row2 = ["D", "E"]

        result = ["A", "B", "C\r\nD", "E"]
        ```
    """

    if len(row1) == 0:
        raise ValueError("First row should not be empty.")

    if len(row2) == 0:
        row2 = [""]

    return [*row1[:-1], newline_char.join([row1[-1], row2[0]]), *row2[1:]]


def _get_bytes(row: List[str], newline_char: str = DEFAULT_NEWLINE_CHAR) -> int:
    """Return the number of bytes of a given row as read by csv.reader."""

    return len(("|".join(row) + newline_char).encode("utf-8"))


def reader(
    path: Union[Path, str],
    include_header: bool = False,
    newline_char: str = DEFAULT_NEWLINE_CHAR,
) -> Generator[List[str], None, None]:
    """Returns a generator that yields the records of a RPDR *.txt file.

    Currently, RPDR files cannot be correctly read by regular CSV tools (e.g.
    csv.reader, pd.read_csv).

    Due to some formatting quirks with line breaks/quoting, records may be
    split up over several rows within the CSV.

    This function attempts to piece together the rows into complete records.
    If it is unable to do this, it will throw an error.

    Assumes the file uses \r\n for new lines. (This can be modified by
    specifying newline_char.)"""

    if not isinstance(path, Path):
        path = Path(path)

    file_size = path.stat().st_size

    with open(path, "r", newline="") as rpdr_file, Progress() as progress:
        task = progress.add_task(f"Reading {path.name}...", total=file_size)
        reader = csv.reader(rpdr_file, delimiter="|", quoting=csv.QUOTE_NONE)

        # assume there is always a header row
        header = next(reader)

        if include_header:
            yield header

        progress.update(task, advance=_get_bytes(header, newline_char))

        # first entry
        try:
            record = next(reader)

        except StopIteration:
            return

        for row in reader:
            # if the row is length 0 or 1 we assume it belongs to the previous record
            if len(record) == len(header) and len(row) > 1:
                yield record
                progress.update(task, advance=_get_bytes(record, newline_char))

                # start new record
                record = row[:]

            elif len(record) > len(header):
                raise RuntimeError(
                    "Could not piece together record. Row lengths do not add "
                    "up to the expected number of fields."
                )

            # merge row
            else:
                record = _merge_rows(record, row, newline_char)

        # handle last record
        if len(record) != len(header):
            raise RuntimeError(
                "Final record does not appear to have the expected number of fields."
            )

        yield record
        progress.update(task, advance=_get_bytes(record, newline_char))


def read_file(
    path: Union[Path, str], newline_char: str = DEFAULT_NEWLINE_CHAR
) -> pd.DataFrame:
    """Reads an RPDR *.txt file into a Pandas DataFrame.

    Assumes the file uses \r\n for new lines. (This can be modified by specifying
    the newline_char parameter.)"""

    records = reader(path, include_header=True, newline_char=newline_char)

    header = next(records)

    return pd.DataFrame(records, columns=header)
