from chalicelib.src.reports.models.Report import Report
from chalicelib.src.reports.models.NationalReport import NationalReport
from chalicelib.src.reports.models.StateReport import StateReport
import chalicelib.src.reports.constants as report_constants
import chalicelib.src.general.helpers as helpers
from openpyxl.reader.excel import load_workbook
import tempfile
import io

class ReportFactory:
    def __init__(self):
        pass

    def determine_report_type(self, report_content) -> str:
        """looks at the content of a report and returns a string of the report's type"""
        #peek at Queries[1][1] to see if it says AGGREGATESTO
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        temp_file.write(io.BytesIO(report_content).getbuffer())
        temp_file.close()
        workbook = load_workbook(temp_file.name, data_only=True)
        sheet = workbook[report_constants.REPORT_INPUT_DATA_SHEET_NAME]
        if sheet['A1'].value and helpers.soft_compare_strings(sheet['A1'].value.upper(), report_constants.STATE_REPORT_AGGREGATESTO_COLUMN_HEADER.upper()):
            return report_constants.REPORT_TYPES["STATE"]
        else: # currently, the only other report type is national
            return report_constants.REPORT_TYPES["NATIONAL"]

    def get_report_from_factory(
        self,
        report_id,
        content,
        max_time_series,
        created_by=None,
        read_only=False,
        report_name=None
    ):
        report_type = self.determine_report_type(content) # use determine_report_type to choose the object class to instantiate
        if report_type == report_constants.REPORT_TYPES["NATIONAL"]:
            return NationalReport(
                    report_id,
                    content,
                    max_time_series,
                    created_by=None,
                    read_only=False,
                    report_name=None,
                )
        elif report_type == report_constants.REPORT_TYPES["STATE"]:
            return StateReport(
                    report_id,
                    content,
                    max_time_series,
                    created_by=None,
                    read_only=False,
                    report_name=None,
                )
        else:
            raise ValueError(f"{full_class_name(self)}.get_report_from_factory: Unknown report type found!")