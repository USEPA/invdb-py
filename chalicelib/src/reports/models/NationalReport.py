from chalicelib.src.reports.models.Report import *
from chalicelib.src.jobs.models.Job import *
import chalicelib.src.reports.jobs.processing.queries as processing_queries
from chalicelib.src.general.helpers import full_class_name, tprint
import chalicelib.src.reports.constants as report_constants
from openpyxl.reader.excel import load_workbook
import math


class NationalReportRow(ReportRow):
    pass
    

class NationalReport(Report):
    
    def __init__(
        self,
        report_id,
        content,
        max_time_series,
        created_by=None,
        read_only=False,
        report_name=None,
    ):
        super().__init__(
            report_id,
            content,
            max_time_series,
            created_by,
            read_only,
            report_name,
        )
        self._report_type = report_constants.REPORT_TYPES["NATIONAL"]
        self._width = report_constants.NATIONAL_Y1990_QUANTITY_COL_POS + 1 + self._max_time_series

    def __repr__(self):
        return "<National " + super().__repr__()[1:]

    def process_contents(self, error_rows: [int], query_formula_info, reporting_year: int, layer_id: int, this_job: Job) -> None:
        """takes a single national report and the list of its validation error rows, and updates the contents
        of the national report file to include the processed query results in the Query_Results tab"""
        
        # wipe the existing data in the Query_Results tab
        if self.has_query_results_tab():
            self._workbook.remove(self._workbook[report_constants.REPORT_OUTPUT_DATA_SHEET_NAME])
        self._workbook.create_sheet(title=report_constants.REPORT_OUTPUT_DATA_SHEET_NAME)

        # process the queries and write results in batches
        row = report_constants.FIRST_DATA_ROW
        eof_reached = False # bool for if the last query has been read from the report
        batch_number = 1
        batch_count = math.ceil((len(self) - len(error_rows)) / report_constants.QUERIES_PER_REQUEST)
        while not eof_reached: # each iteration is a new query batch
            helpers.tprint(f"processing batch {batch_number} of {batch_count}")
            this_job.post_event(
                "REPORT_PROCESSING",
                "PROCESSING_QUERY_BATCH",
                batch_number, 
                batch_count,
                self.get_report_id()
            )
            self.switch_to_queries_tab()
            query_batch = []
            formula_inputs = {}
            # gather the next 10 queries from the file (break at EOF)
            while len(query_batch) < report_constants.QUERIES_PER_REQUEST:
                #collect the input on that row (even if there is an error)
                formula_string = self[row][report_constants.FORMULA_COL_POS] #case sensitive for all parts
                if formula_string is not None:
                    formula_string = formula_string.strip()
                if formula_string not in ["", None]:
                    formula_inputs.update({row: formula_string})
                
                # skip if error row
                if row in error_rows:
                    row += 1
                    continue

                # end batch here if EOF is reached
                if self[row][report_constants.FORMULA_COL_POS] is None or helpers.simplify_whitespace(self[row][report_constants.FORMULA_COL_POS]) in ["", " "]:
                    eof_reached = True
                    break
                
                # get the query info
                formula_prefix = formula_string[:formula_string.index("(")]
                input_arguments, _ = helpers.parse_report_formula_arguments(formula_string)
                # add the query info to the batch
                query_batch.append((row, formula_prefix) + tuple(input_arguments))
                row += 1
            
            if len(query_batch) > 0:
                # send the query batch to the database in one SQL statement and get results back
                query_batch_results, width = processing_queries.process_report_query_batch(query_batch, query_formula_info, self._report_type, reporting_year, layer_id)
                
                # write the results into their rows in the Query_Results tab
                this_job.post_event(
                    "REPORT_PROCESSING",
                    "WRITING_BATCH_RESULTS",
                    batch_number, 
                    batch_count,
                    self.get_report_id()
                )
                self.switch_to_results_tab()

                if batch_number == 1: 
                    # write in the Query_Results tab headers
                    self[1][report_constants.FORMULA_COL_POS] = report_constants.REPORT_FORMULA_COLUMN_HEADER
                    for year, i in zip(range(db_constants.EARLIEST_REPORTING_YEAR,db_constants.EARLIEST_REPORTING_YEAR + width), range(1, width+1)):
                        self[1][report_constants.FORMULA_COL_POS + i] = f'Y{year}'

                for row_num in sorted(formula_inputs.keys()):
                    # copy the formula column of the Queries tab to that of the Query_Results tab
                    self[row_num][report_constants.FORMULA_COL_POS] = formula_inputs[row_num]
                    
                    if row not in error_rows: # skip writing the results if this row is an error row
                        # populate the data results for each processed query row_num in the Query_Results tab
                        results_for_this_row_num = [record[2] for record in query_batch_results if record[0] == row_num]
                        if len(results_for_this_row_num) > 0:
                            for query_result, i in zip(results_for_this_row_num, range(width)):
                                self[row_num][report_constants.NATIONAL_Y1990_QUANTITY_COL_POS + i] = (query_result if query_result is not None else 0)
                        else: # populate the row_num with zeros
                            for i in range(width):
                                self[row_num][report_constants.NATIONAL_Y1990_QUANTITY_COL_POS + i] = 0

            batch_number += 1

        self.save()