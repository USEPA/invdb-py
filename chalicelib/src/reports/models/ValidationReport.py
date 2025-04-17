from chalicelib.src.reports.models.DataQualityError import DataQualityError
from chalicelib.src.general.helpers import full_class_name
import chalicelib.src.reports.constants as qc_constants
import chalicelib.src.general.helpers as helpers

class ValidationReport:
    def __init__(self, report_id, report_type, errors=None):
        self.report_id = report_id
        self.report_type = report_type
        self._validation_result = qc_constants.VALIDATION_RESULTS["NOT_VALIDATED"]

        if errors is not None:
            # assert that the input is of type dict
            if not isinstance(errors, dict):
                return TypeError(
                    f"{full_class_name(ValidationReport(-1))}.__init__: the input must a of type dict<int: DataQualityError>."
                )
            if len(errors) > 0:
                # assert that the keys of the input dict are of type int (they indicate the row numbers)
                if not all([isinstance(key, int) for key in errors.keys()]):
                    return KeyError(
                        f"{full_class_name(ValidationReport(-1))}.__init__: all keys in the input must be of type `int`."
                    )
                # soft check that the values of the input dict are of type `List<QC.ValidationError>`
                if not all([isinstance(value, list) for value in errors.values()]) or (
                    not all(
                        [
                            isinstance(error, DataQualityError)
                            for error in list(errors.values())[
                                0
                            ]  # checks the first row only
                        ]
                    )
                ):
                    return ValueError(
                        f"{full_class_name(ValidationReport(-1))}.__init__: all values in the input must be of type `List<{full_class_name(DataQualityError())}>`."
                    )
            self.errors = errors
        else:
            self.errors = {}

    def has_errors(self):
        return len(self.errors.keys()) > 0

    def get_error_count(self) -> int:
        """returns the total number of validation errors found"""
        error_count = 0
        for row_num in self.errors.keys():
            error_count += len(self.errors[row_num])
        return error_count

    def add_row(self, row):
        """will overwrite any data for the row if it already exists"""
        if not isinstance(row, list) or not all(
            [isinstance(item, DataQualityError) for item in row]
        ):
            raise TypeError(
                f"ValidationReport.add_row: parameter must be of type List<{full_class_name(DataQualityError())}> only."
            )
        if len(row) > 0:  # ignore empty rows
            self.errors[row[0].row_number] = row

    def update_report(self, input_validation_report):
        """add all the errors from another ValidationReport object to self's"""
        if self.report_id != input_validation_report.report_id:
            raise ValueError(
                f"{full_class_name(ValidationReport(-1))}.update_report: Only validation reports of the same report_id can merged."
            )

        self.errors.update(input_validation_report.errors)

    def generate_error_list(self):
        validation_logs = []
        for row_num in self.errors.keys():
            for error in self.errors[row_num]:
                validation_logs.append(
                    (
                        error.report_id,
                        error.field_name,
                        error.value,
                        error.row_number,
                        error.description,
                        error.created_by,
                        error.tab_name,
                    )
                )
        return validation_logs

    def get_id(self):
        return self.report_id

    def get_report_id(self):
        return self.report_id

    def get_report_type(self):
        return self.report_type

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

    def __repr__(self):
        # old code that prints out all the errors (too verbose)
        # error_count = 0
        # repr = f"Validation report for source file with report ID: {self.report_id}:"
        # if len(self.errors.keys()) > 0:
        #     for row in self.errors:
        #         error_count += len(self.errors[row])
        #         repr += (
        #             f"\n\terrors found on line {row} (count: {len(self.errors[row])}):"
        #         )
        #         for error in self.errors[row]:
        #             repr += f"\n\t\t{str(error)}"
        #     repr += f"\nEnd of validation report for source file with report ID: {self.report_id} (Total # of errors: {error_count})"
        #     return repr
        # else:
        #     return f"No errors found for source file with report ID: {self.report_id}"
        
        # just print the report ID and the number of errors found
        error_count = self.get_error_count()
        return f"""Validation report for {self.report_type.lower()} with report ID: {self.report_id}, validation status: {self._validation_result}{f", {error_count} {helpers.plurality_agreement('error', 'errors', error_count)} found." if self._validation_result == qc_constants.VALIDATION_RESULTS["SUCCESS"] else ""}"""
        