import logging
from pathlib import Path
from typing import Callable, Union
import pandas as pd
import pytest
import rpdrtools.io as io
from rpdrtools.io.types import OnBrokenRecordsType

SampleFileFixture = Callable[..., pd.DataFrame]


@pytest.fixture
def sample_file() -> SampleFileFixture:
    def _sample_file(
        path: Union[Path, str], on_broken_records: io.types.OnBrokenRecordsType
    ) -> pd.DataFrame:
        return io.read_file(path, on_broken_records=on_broken_records)

    return _sample_file


class TestMergeRows:
    def test_both_empty(self) -> None:
        with pytest.raises(Exception):
            io._merge_rows([], [])

    def test_first_empty(self) -> None:
        with pytest.raises(Exception):
            io._merge_rows([], ["A"])

    def test_second_empty(self) -> None:
        assert io._merge_rows(["A"], []) == ["A\r\n"]

    def test_empty_string(self) -> None:
        assert io._merge_rows(["A"], [""]) == ["A\r\n"]

    def test_empty_strings(self) -> None:
        assert io._merge_rows([""], [""]) == ["\r\n"]

    def test_basic_example(self) -> None:
        assert io._merge_rows(["A", "B", "C"], ["D", "E"]) == [
            "A",
            "B",
            "C\r\nD",
            "E",
        ]


class TestGetBytes:
    def test_empty_row(self) -> None:
        # should be two for the line break (default \r\n)
        assert io._get_bytes([]) == 2

    def test_row_with_one_element(self) -> None:
        # should be one for the "A" and two for the line break (default \r\n)
        assert io._get_bytes(["A"]) == 3


class TestRead:
    def test_report_text_break_repaired(self, sample_file: SampleFileFixture) -> None:
        result = sample_file(
            path="tests/data/report_text_break.txt",
            on_broken_records="repair",
        )
        expected = pd.DataFrame(
            {
                "EMPI": ["012345", "1515156"],
                "EPIC_PMRN": ["012345", "33445567"],
                "MRN_Type": ["MGH", "BWH"],
                "MRN": ["012345", "444444"],
                "Report_Number": ["999999", "0123456"],
                "Report_Date_Time": ["1/1/2000 9:00:00 AM", "1/1/2012 8:00:00 PM"],
                "Report_Description": ["ABCDE", "ABCDE"],
                "Report_Status": ["F", "F"],
                "Report_Type": ["GHIJK", "GHIJK"],
                "Report_Text": [
                    (
                        "\r\nThis is an example report text. "
                        "The format may not reflect a typical note."
                        "\r\n\r\nPatient name: First Last\r\n\r\n"
                        "Sex: Male\r\n\r\nDOB: 1/1/1950\r\n\r\n"
                        "HPI: ?\r\n[report_end]"
                    ),
                    (
                        "\r\nThis is an example report text. "
                        "The format may not reflect a typical note."
                        "\r\n\r\nPatient name: First Last\r\n\r\n"
                        "Sex: Female\r\n\r\nDOB: 3/6/1980\r\n\r\n"
                        "HPI: ?\r\n[report_end]"
                    ),
                ],
            }
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_report_text_break_skipped(self, sample_file: SampleFileFixture) -> None:
        result = sample_file(
            path="tests/data/report_text_break.txt",
            on_broken_records="skip",
        )
        expected = pd.DataFrame(
            {
                "EMPI": ["012345", "1515156"],
                "EPIC_PMRN": ["012345", "33445567"],
                "MRN_Type": ["MGH", "BWH"],
                "MRN": ["012345", "444444"],
                "Report_Number": ["999999", "0123456"],
                "Report_Date_Time": ["1/1/2000 9:00:00 AM", "1/1/2012 8:00:00 PM"],
                "Report_Description": ["ABCDE", "ABCDE"],
                "Report_Status": ["F", "F"],
                "Report_Type": ["GHIJK", "GHIJK"],
                "Report_Text": [
                    (
                        "\r\nThis is an example report text. "
                        "The format may not reflect a typical note."
                        "\r\n\r\nPatient name: First Last\r\n\r\n"
                        "Sex: Male\r\n\r\nDOB: 1/1/1950\r\n\r\n"
                        "HPI: ?\r\n[report_end]"
                    ),
                    (
                        "\r\nThis is an example report text. "
                        "The format may not reflect a typical note."
                        "\r\n\r\nPatient name: First Last\r\n\r\n"
                        "Sex: Female\r\n\r\nDOB: 3/6/1980\r\n\r\n"
                        "HPI: ?\r\n[report_end]"
                    ),
                ],
            }
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_pipe_in_report_text_repaired(self, sample_file: SampleFileFixture) -> None:
        result = sample_file(
            path="tests/data/pipe_in_report_text.txt",
            on_broken_records="repair",
        )
        expected = pd.DataFrame(
            {
                "EMPI": ["012345", "1515156"],
                "EPIC_PMRN": ["012345", "33445567"],
                "MRN_Type": ["MGH", "BWH"],
                "MRN": ["012345", "444444"],
                "Report_Number": ["999999", "0123456"],
                "Report_Date_Time": ["1/1/2000 9:00:00 AM", "1/1/2012 8:00:00 PM"],
                "Report_Description": ["ABCDE", "ABCDE"],
                "Report_Status": ["F", "F"],
                "Report_Type": ["GHIJK", "GHIJK"],
                "Report_Text": [
                    (
                        "\r\nThis is an example report text. "
                        "The format may not reflect a typical note."
                        "\r\n\r\nPatient name: First Last\r\n\r\n"
                        "|Sex: Male\r\n\r\nDOB: 1/1/1950\r\n\r\n"
                        "HPI: ?\r\n[report_end]"
                    ),
                    (
                        "\r\nThis is an example report text. "
                        "The format may not reflect a typical note."
                        "\r\n\r\nPatient name: First Last\r\n\r\n"
                        "Sex: Female\r\n\r\nDOB: 3/6/1980\r\n\r\n"
                        "HPI: ?\r\n[report_end]"
                    ),
                ],
            }
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_non_report_text_break_repaired(
        self, sample_file: SampleFileFixture
    ) -> None:
        result = sample_file(
            path="tests/data/non_report_text_break.txt",
            on_broken_records="repair",
        )
        expected = pd.DataFrame(
            {
                "EMPI": ["012345"],
                "EPIC_PMRN": ["012345"],
                "MRN_Type": ["MGH"],
                "MRN": ["012345"],
                "Date": ["1/1/2000"],
                "Procedure_Name": [
                    "Procedure name\r\ncan sometimes\r\ntake several\r\nlines"
                ],
                "Code_Type": ["CPT"],
                "Code": ["00000"],
                "Procedure_Flag": [""],
                "Quantity": ["1"],
                "Provider": ["Name"],
                "Clinic": ["Clinic"],
                "Hospital": ["MGH"],
                "Inpatient_Outpatient": ["Inpatient"],
                "Encounter_number": ["012345"],
            }
        )

        pd.testing.assert_frame_equal(result, expected)

    def test_non_report_text_break_skipped(
        self, sample_file: SampleFileFixture
    ) -> None:
        result = sample_file(
            path="tests/data/non_report_text_break.txt",
            on_broken_records="skip",
        )
        expected = pd.DataFrame(
            (),
            columns=[
                "EMPI",
                "EPIC_PMRN",
                "MRN_Type",
                "MRN",
                "Date",
                "Procedure_Name",
                "Code_Type",
                "Code",
                "Procedure_Flag",
                "Quantity",
                "Provider",
                "Clinic",
                "Hospital",
                "Inpatient_Outpatient",
                "Encounter_number",
            ],
        )

        pd.testing.assert_frame_equal(result, expected)

    def test_both_breaks_repaired(self, sample_file: SampleFileFixture) -> None:
        result = sample_file(
            path="tests/data/both_breaks.txt",
            on_broken_records="repair",
        )
        expected = pd.DataFrame(
            {
                "EMPI": ["012345", "1515156"],
                "EPIC_PMRN": ["012345", "33445567"],
                "MRN_Type": ["MGH", "BWH"],
                "MRN": ["012345", "444444"],
                "Report_Number": ["999999", "0123456"],
                "Report_Date_Time": ["1/1/2000 9:00:00 AM", "1/1/2012 8:00:00 PM"],
                "Report_Description": ["ABC\r\nDE", "ABCDE"],
                "Report_Status": ["F", "F"],
                "Report_Type": ["GHIJK", "GHIJK"],
                "Report_Text": [
                    (
                        "\r\nThis is an example report text. "
                        "The format may not reflect a typical note."
                        "\r\n\r\nPatient name: First Last\r\n\r\n"
                        "Sex: Male\r\n\r\nDOB: 1/1/1950\r\n\r\n"
                        "HPI: ?\r\n[report_end]"
                    ),
                    (
                        "\r\nThis is an example report text. "
                        "The format may not reflect a typical note."
                        "\r\n\r\nPatient name: First Last\r\n\r\n"
                        "Sex: Female\r\n\r\nDOB: 3/6/1980\r\n\r\n"
                        "HPI: ?\r\n[report_end]"
                    ),
                ],
            }
        )

        pd.testing.assert_frame_equal(result, expected)

    def test_both_breaks_skipped(self, sample_file: SampleFileFixture) -> None:
        result = sample_file(
            path="tests/data/both_breaks.txt",
            on_broken_records="skip",
        )
        expected = pd.DataFrame(
            {
                "EMPI": ["1515156"],
                "EPIC_PMRN": ["33445567"],
                "MRN_Type": ["BWH"],
                "MRN": ["444444"],
                "Report_Number": ["0123456"],
                "Report_Date_Time": ["1/1/2012 8:00:00 PM"],
                "Report_Description": ["ABCDE"],
                "Report_Status": ["F"],
                "Report_Type": ["GHIJK"],
                "Report_Text": [
                    (
                        "\r\nThis is an example report text. "
                        "The format may not reflect a typical note."
                        "\r\n\r\nPatient name: First Last\r\n\r\n"
                        "Sex: Female\r\n\r\nDOB: 3/6/1980\r\n\r\n"
                        "HPI: ?\r\n[report_end]"
                    ),
                ],
            }
        )

        pd.testing.assert_frame_equal(result, expected)

    def test_no_records(self, sample_file: SampleFileFixture) -> None:
        result = sample_file(
            path="tests/data/no_records.txt",
            on_broken_records="repair",
        )
        expected = pd.DataFrame(
            (),
            columns=[
                "EMPI",
                "EPIC_PMRN",
                "MRN_Type",
                "MRN",
                "Date",
                "Procedure_Name",
                "Code_Type",
                "Code",
                "Procedure_Flag",
                "Quantity",
                "Provider",
                "Clinic",
                "Hospital",
                "Inpatient_Outpatient",
                "Encounter_number",
            ],
        )

        pd.testing.assert_frame_equal(result, expected)

    def test_no_exception_on_report_text_breaks(
        self, sample_file: SampleFileFixture
    ) -> None:
        result = sample_file(
            path="tests/data/report_text_break.txt",
            on_broken_records="raise",
        )
        expected = pd.DataFrame(
            {
                "EMPI": ["012345", "1515156"],
                "EPIC_PMRN": ["012345", "33445567"],
                "MRN_Type": ["MGH", "BWH"],
                "MRN": ["012345", "444444"],
                "Report_Number": ["999999", "0123456"],
                "Report_Date_Time": ["1/1/2000 9:00:00 AM", "1/1/2012 8:00:00 PM"],
                "Report_Description": ["ABCDE", "ABCDE"],
                "Report_Status": ["F", "F"],
                "Report_Type": ["GHIJK", "GHIJK"],
                "Report_Text": [
                    (
                        "\r\nThis is an example report text. "
                        "The format may not reflect a typical note."
                        "\r\n\r\nPatient name: First Last\r\n\r\n"
                        "Sex: Male\r\n\r\nDOB: 1/1/1950\r\n\r\n"
                        "HPI: ?\r\n[report_end]"
                    ),
                    (
                        "\r\nThis is an example report text. "
                        "The format may not reflect a typical note."
                        "\r\n\r\nPatient name: First Last\r\n\r\n"
                        "Sex: Female\r\n\r\nDOB: 3/6/1980\r\n\r\n"
                        "HPI: ?\r\n[report_end]"
                    ),
                ],
            }
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_exception_when_report_text_not_last(
        self, sample_file: SampleFileFixture
    ) -> None:
        with pytest.raises(IndexError):
            sample_file(
                path="tests/data/report_text_not_last.txt",
                on_broken_records="repair",
            )

    def test_exception_on_non_report_text_breaks(
        self, sample_file: SampleFileFixture
    ) -> None:
        with pytest.raises(RuntimeError):
            sample_file(
                path="tests/data/non_report_text_break.txt",
                on_broken_records="raise",
            )

    @pytest.mark.parametrize(
        "on_broken_records, broken_records_result",
        [("skip", "skipped"), ("repair", "repaired")],
    )
    def test_logging_warnings(
        self,
        caplog: pytest.LogCaptureFixture,
        sample_file: SampleFileFixture,
        on_broken_records: OnBrokenRecordsType,
        broken_records_result: str,
    ) -> None:
        sample_file(
            path="tests/data/non_report_text_break.txt",
            on_broken_records=on_broken_records,
        )

        assert f"Found 4 broken rows which were {broken_records_result}." in caplog.text

    def test_logging_info(
        self,
        caplog: pytest.LogCaptureFixture,
        sample_file: SampleFileFixture,
    ) -> None:
        with caplog.at_level(logging.INFO):
            sample_file(
                path="tests/data/report_text_break.txt",
                on_broken_records="skip",
            )

        assert "No broken rows were found." in caplog.text
