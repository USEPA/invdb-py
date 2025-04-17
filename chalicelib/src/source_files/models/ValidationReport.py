from chalicelib.src.source_files.models.DataQualityError import DataQualityError
from chalicelib.src.general.helpers import full_class_name
import chalicelib.src.source_files.constants as qc_constants
import chalicelib.src.general.helpers as helpers

class ValidationReport:
    def __init__(self, source_file_id, attachment_id, errors: {int: [DataQualityError]}=None):
        self._source_file_id = source_file_id
        self._attachment_id = attachment_id
        self._validation_result = qc_constants.VALIDATION_RESULTS["NOT_VALIDATED"]

        if errors is not None:
            self.errors = errors
        else:
            self.errors = {}


    def has_errors(self) -> bool:
        """returns whether the validation process detected any errors in the source file"""
        return len(self.errors.keys()) > 0

    def get_error_count(self) -> int:
        """returns the total number of validation errors found"""
        error_count = 0
        for row_num in self.errors.keys():
            error_count += len(self.errors[row_num])
        return error_count

    def add_row(self, row: [DataQualityError]) -> None:
        """adds a new row of DataQualityErrors to the object under the row number
        found in the first element of the input. This method will overwrite any 
        data for the row of this object if it already exists """
        if len(row) > 0:
            self.errors[row[0].row_number] = row

    def update_report(self, input_validation_report) -> None:
        """adds all the errors from another ValidationReport to this object"""
        self.errors.update(input_validation_report.errors)

    def generate_error_list(self) -> [tuple]:
        """returns a list of tuples. Each tuple is used to populate a row in the validation_log_load table"""
        validation_logs = []
        for row_num in self.errors.keys():
            for error in self.errors[row_num]:
                validation_logs.append(
                    (
                        error.attachment_id,
                        error.field_name,
                        error.field_value,
                        error.row_number,
                        error.description,
                        error.created_by,
                    )
                )
        return validation_logs

    def get_id(self):
        return self._attachment_id

    def get_source_file_id(self) -> int:
        """returns the source_file_id of source file that was processed during the validation"""
        return self._source_file_id

    def get_attachment_id(self) -> int:
        """returns the attachment_id of source file that was processed during the validation"""
        return self._attachment_id

    def set_validation_result(self, status_id_string: str) -> None:
        """takes a string that must be a key of qc_constants.VALIDATION_RESULTS"""
        if status_id_string not in qc_constants.VALIDATION_RESULTS.keys():
            raise ValueError(
                f"""{full_class_name(self)}.set_validation_status: Invalid status id string `{status_id_string}`"""
            )
        self._validation_result = qc_constants.VALIDATION_RESULTS[status_id_string]
   
    def get_validation_result(self) -> str:
        """returns a string representing the outcome of the validation process. SUCCESS implies that the 
            validation process ran to completion. FAILURE implies that a fatal error caused for the
            validation process to abort"""
        return self._validation_result

    def __repr__(self) -> str:
        """returns a formatted report of the errors found within the source file."""
        # old code that prints out all the errors (too verbose)
        # error_count = 0
        # repr = f"Validation report for source file with attachment ID: {self._attachment_id}:"
        # if len(self.errors.keys()) > 0:
        #     for row in self.errors:
        #         error_count += len(self.errors[row])
        #         repr += (
        #             f"\n\terrors found on line {row} (count: {len(self.errors[row])}):"
        #         )
        #         for error in self.errors[row]:
        #             repr += f"\n\t\t{str(error)}"
        #     repr += f"\nEnd of validation report for source file with attachment ID: {self._attachment_id} (Total # of errors: {error_count})"
        #     return repr
        # else:
        #     return f"No errors found for source file with attachment ID: {self._attachment_id}"

        # just print the source file and attachment IDs and the number of errors found
        error_count = self.get_error_count()
        return f"""Validation report for national report with source file ID: {self.get_source_file_id()}, attachment ID: {self.get_attachment_id()}, validation status: {self._validation_result}{f", {error_count} {helpers.plurality_agreement('error', 'errors', error_count)} found." if self._validation_result == qc_constants.VALIDATION_RESULTS["SUCCESS"] else ""}"""