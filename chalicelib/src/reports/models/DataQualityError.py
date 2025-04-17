import chalicelib.src.reports.constants as report_constants

class DataQualityError:
    def __init__(
        self,
        field_name,
        row_number,
        value=None,
        report_id=None,
        description=None,
        created_date=None,
        created_by=None,
        tab_name=report_constants.REPORT_INPUT_DATA_SHEET_NAME
    ):
        # these are known at time of validation
        self.field_name = field_name
        self.value = value
        self.row_number = row_number
        # these are optional, or added later.
        self.report_id = report_id
        self.description = description
        self.created_date = created_date
        self.created_by = created_by
        self.tab_name = tab_name

    def __repr__(self):
        repr = f"DataQualityError -  Row: {self.row_number}, Field: {self.field_name},"
        repr += f"  Value: {self.value}," if self.value is not None else ""
        repr += (
            f"  Report ID: {self.report_id},"
            if self.report_id is not None
            else ""
        )
        repr += (
            f"  Description: {self.description},"
            if self.description is not None
            else ""
        )
        repr += (
            f"  Date Created: {self.created_date},"
            if self.created_date is not None
            else ""
        )
        repr += (
            f"  Source File Author: {self.created_by},"
            if self.created_by is not None
            else ""
        )

        return "(" + repr[:-1] + ")"
