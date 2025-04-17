from chalicelib.src.source_files.models.ValidationReport import ValidationReport
import chalicelib.src.general.helpers as helpers
import chalicelib.src.source_files.constants as qc_constants

class BatchValidationReport:
    def __init__(self, reports: {int: ValidationReport} = None):
        if reports is not None:
            self.reports = reports
        else:
            self.reports = {}

    def add_report(self, validation_report: ValidationReport) -> None:
        """Add a single validation report to the object"""
        self.reports[validation_report.get_id()] = validation_report

    def has_reports(self) -> bool:
        """returns whether there are any reports in the object"""
        return len(self.reports.keys()) > 0

    def has_reports_with_errors(self) -> bool:
        """returns whether any of its constituent reports contains errors"""
        return any([validation_report.has_errors() for validation_report in self])

    def get_validation_reports_that_succeeded(self) -> [ValidationReport]:
        """returns a list of reports where the validation process ran to completion"""
        return [
            self.reports[report]
            for report in sorted(self.reports.keys())
            if self.reports[report].get_validation_result() == qc_constants.VALIDATION_RESULTS["SUCCESS"]
        ]

    def get_validation_reports_with_errors(self) -> [ValidationReport]:
        """returns a list of attachment_ids corresponding to constituent reports that contain at least one error"""
        return [
            self.reports[report]
            for report in sorted(self.reports.keys())
            if self.reports[report].get_validation_result() == qc_constants.VALIDATION_RESULTS["SUCCESS"] and self.reports[report].has_errors()
        ]

    def get_validation_reports_without_errors(self) -> [ValidationReport]:
        """returns a list of attachment_ids corresponding to constituent reports that don't contain any errors"""
        return [
            self.reports[report]
            for report in sorted(self.reports.keys())
            if self.reports[report].get_validation_result() == qc_constants.VALIDATION_RESULTS["SUCCESS"] and not self.reports[report].has_errors()
        ]
    
    def get_validation_reports_that_failed(self) -> [ValidationReport]:
        """returns a list of reports where the validation process was aborted by a fatal error"""
        return [
            self.reports[report]
            for report in sorted(self.reports.keys())
            if self.reports[report].get_validation_result() == qc_constants.VALIDATION_RESULTS["FAILED"]
        ]

    def generate_error_list(self) -> [tuple]:
        """returns all the error lists of the object's constituent validation reports
            combined into one flattened list"""
        validation_logs = []
        for report in self.reports:
            validation_logs += self.reports[report].generate_error_list()

        return validation_logs

    def __iter__(self):
        """returns each report as an element in no particular order"""
        for validation_report_id in sorted(self.reports.keys()):
            yield self.reports[validation_report_id]

    def __repr__(self) -> str:
        """returns the a formatted report for all the validation reports
        within the object as a string."""
        report_count = len(self.reports.keys())
        if report_count > 0:
            repr = f"Batch Validation Report for {len(self.reports.keys())} {helpers.plurality_agreement('file', 'files', report_count)}: \n"
            #for report in self.reports:
                # old implementation that shows the errors details found
                # repr += f"\n\n{self.reports[report].__repr__()}"
            repr += "SUCCESSFUL VALIDATIONS:\n"
            successful_reports = self.get_validation_reports_that_succeeded()
            if len(successful_reports) > 0:
                for report in successful_reports:
                    repr += f"    {str(report)}\n"
            else:
                repr += f"""    None\n"""

            repr += "\nFAILED VALIDATIONS:\n"
            failed_reports = self.get_validation_reports_that_failed()
            if len(failed_reports) > 0:
                for report in failed_reports:
                    repr += f"    {str(report)}\n"
            else:
                repr += f"""    None\n"""

            return repr
        else:
            return "<Empty Batch Validation Report>"
