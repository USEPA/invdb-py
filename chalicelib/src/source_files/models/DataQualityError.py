class DataQualityError:
    def __init__(
        self,
        field_name,
        row_number,
        field_value=None,
        attachment_id=None,
        description=None,
        created_date=None,
        created_by=None,
    ):
        # these are known at time of validation
        self.field_name = field_name
        self.field_value = field_value
        self.row_number = row_number
        # these are optional, or added later.
        self.attachment_id = attachment_id
        self.description = description
        self.created_date = created_date
        self.created_by = created_by

    def __repr__(self):
        repr = f"DataQualityError -  Row: {self.row_number}, Field: {self.field_name},"
        repr += f"  Value: {self.field_value}," if self.field_value is not None else ""
        repr += (
            f"  Attachment ID: {self.attachment_id},"
            if self.attachment_id is not None
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
