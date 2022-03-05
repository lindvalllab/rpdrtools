import csv
import logging
from pathlib import Path
from typing import Generator, List, Union

import pandas as pd
from rich.progress import Progress
from ..constants import (
    RPDR_NEWLINE_CHAR,
    RPDR_FILE_DELIMITER,
    RPDR_REPORT_TEXT_FIELD,
    RPDR_REPORT_END_TOKEN,
)
from .types import OnBrokenRecordsType


def _merge_rows(
    row1: List[str], row2: List[str], newline_char: str = RPDR_NEWLINE_CHAR
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


def _get_bytes(row: List[str], newline_char: str = RPDR_NEWLINE_CHAR) -> int:
    """Return the number of bytes of a given row as read by csv.reader."""

    return len(("|".join(row) + newline_char).encode("utf-8"))


def _found_report_end(record: List[str]) -> bool:
    return len(record) > 0 and record[-1].endswith(RPDR_REPORT_END_TOKEN)


def reader(
    path: Union[Path, str],
    on_broken_records: OnBrokenRecordsType = "repair",
    include_header: bool = False,
    newline_char: str = RPDR_NEWLINE_CHAR,
) -> Generator[List[str], None, None]:
    """Returns a generator that yields the records of a RPDR *.txt file
    as a list of strings.

    Currently, RPDR files cannot be correctly read by regular CSV tools (e.g.
    csv.reader, pd.read_csv).

    Due to some formatting quirks with line breaks/quoting, records may be
    split up over several rows within the CSV.

    If on_broken_records is True, this function attempt to piece together the
    rows into complete records. (If it is unable to do this, it will throw an error.)

    Assumes the file uses \r\n for new lines. (This can be modified by
    specifying newline_char.)"""

    if not isinstance(path, Path):
        path = Path(path)

    file_size = path.stat().st_size

    with open(path, "r", newline="") as rpdr_file, Progress() as progress:
        task = progress.add_task(f"Reading {path.name}...", total=file_size)
        reader = csv.reader(
            rpdr_file, delimiter=RPDR_FILE_DELIMITER, quoting=csv.QUOTE_NONE
        )

        # assume there is always a header row
        header = next(reader)

        if include_header:
            yield header

        has_report_text = RPDR_REPORT_TEXT_FIELD in header

        if has_report_text and header.index(RPDR_REPORT_TEXT_FIELD) != len(header) - 1:
            raise IndexError(
                f"{RPDR_REPORT_TEXT_FIELD} is expected to be the last field."
            )

        progress.update(task, advance=_get_bytes(header, newline_char))

        # first entry
        try:
            record = next(reader)

        except StopIteration:
            return

        n_broken_rows = 1 if len(record) < len(header) else 0

        for row_number, row in enumerate(reader):
            in_report_text = (
                has_report_text
                and len(record) == len(header)
                and not _found_report_end(record)
            )

            if in_report_text:
                to_add = RPDR_FILE_DELIMITER.join(row)

                record = _merge_rows(record, [to_add], newline_char)

            elif len(record) == len(header) and len(row) > 0:
                if has_report_text and _found_report_end(record):
                    record[-1] = record[-1].removesuffix(RPDR_REPORT_END_TOKEN)

                yield record
                progress.update(task, advance=_get_bytes(record, newline_char))

                # start new record
                record = row[:]

            elif len(record) > len(header):
                raise RuntimeError(
                    "Could not piece together record. "
                    "Row lengths do not add up to the expected number of fields."
                )

            # prevent extra empty rows/line breaks after RPDR_REPORT_END_TOKEN
            elif _found_report_end(record) and len(row) == 0:
                continue

            # merge row
            else:
                n_broken_rows += 1
                if on_broken_records == "raise":
                    raise RuntimeError(f"Broken record found in row {row_number}.")

                elif on_broken_records == "repair":
                    record = _merge_rows(record, row, newline_char)

                elif on_broken_records == "skip":
                    # start new record
                    record = row[:]

        # handle last record
        if len(record) != len(header):
            if on_broken_records == "raise" or on_broken_records == "repair":
                raise RuntimeError(
                    "Final record does not appear to have the expected "
                    "number of fields."
                )
            else:
                pass

        else:
            if has_report_text and _found_report_end(record):
                record[-1] = record[-1].removesuffix(RPDR_REPORT_END_TOKEN)

            yield record

        if n_broken_rows > 0:
            broken_record_result = (
                "repaired" if on_broken_records == "repair" else "skipped"
            )
            logging.warning(
                f"Found {n_broken_rows} broken rows which were {broken_record_result}."
            )

        else:
            logging.info("No broken rows were found.")

        progress.update(task, advance=_get_bytes(record, newline_char))


def read_file(
    path: Union[Path, str],
    on_broken_records: OnBrokenRecordsType = "repair",
    newline_char: str = RPDR_NEWLINE_CHAR,
) -> pd.DataFrame:
    """Reads an RPDR *.txt file into a Pandas DataFrame.

    Assumes the file uses \r\n for new lines. (This can be modified by specifying
    the newline_char parameter.)"""

    records = reader(
        path,
        on_broken_records=on_broken_records,
        include_header=True,
        newline_char=newline_char,
    )

    header = next(records)

    return pd.DataFrame(records, columns=header)
