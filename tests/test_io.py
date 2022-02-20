import pandas as pd
import pytest
import rpdrtools.io as io


@pytest.fixture
def with_report_text_break() -> pd.DataFrame:
    return io.read_file("tests/data/with_report_text_break.txt")


@pytest.fixture
def with_non_report_text_break() -> pd.DataFrame:
    return io.read_file("tests/data/with_non_report_text_break.txt")


@pytest.fixture
def no_records() -> pd.DataFrame:
    return io.read_file("tests/data/no_records.txt")


class TestMergeRows:
    def test_both_empty(self):
        with pytest.raises(Exception):
            io._merge_rows([], [])

    def test_first_empty(self):
        with pytest.raises(Exception):
            io._merge_rows([], ["A"])

    def test_second_empty(self):
        assert io._merge_rows(["A"], []) == ["A\r\n"]

    def test_empty_string(self):
        assert io._merge_rows(["A"], [""]) == ["A\r\n"]

    def test_empty_strings(self):
        assert io._merge_rows([""], [""]) == ["\r\n"]

    def test_basic_example(self):
        assert io._merge_rows(["A", "B", "C"], ["D", "E"]) == [
            "A",
            "B",
            "C\r\nD",
            "E",
        ]


class TestGetBytes:
    def test_empty_row(self):
        # should be two for the line break (default \r\n)
        assert io._get_bytes([]) == 2

    def test_row_with_one_element(self):
        # should be one for the "A" and two for the line break (default \r\n)
        assert io._get_bytes(["A"]) == 3


class TestRead:
    def test_report_text_break(self, with_report_text_break):
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
                        "HPI: ?\r\n[report_end]\r\n"
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

        pd.testing.assert_frame_equal(with_report_text_break, expected)

    def test_non_report_text_break(self, with_non_report_text_break):
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

        pd.testing.assert_frame_equal(with_non_report_text_break, expected)

    def test_no_records(self, no_records):
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
        pd.testing.assert_frame_equal(no_records, expected)
