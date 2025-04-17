import chalicelib.src.database.constants as db_constants
import chalicelib.src.general.helpers as helpers

class OnlineReportQuery:

    def __init__(self, report_row_id: int, query_formula_id: int, formula_prefix: str):
        self._report_row_id = report_row_id
        self._query_formula_id = query_formula_id
        self._formula_prefix = formula_prefix
        
        query_formula_info = self._get_query_type_info()
        self._query_type = query_formula_info["name"]
        self._priority = query_formula_info["priority"]
        self._time_series_name = query_formula_info["time_series_name"]

    def _get_query_type_info(self):
        """determines the type of query and loads the information from constants"""
        print(f"the info is {self._report_row_id}, {self._query_formula_id}, {self._formula_prefix}")
        if self._formula_prefix == "complex" and self._query_formula_id == 99:
            return db_constants.QUERY_TYPES["COMPLEX"]
        if self._formula_prefix[:7] in ("em_nat_", "em_sta_"):
            return db_constants.QUERY_TYPES["SIMPLE"]
        if self._formula_prefix[:8] in ("em_natqc_", "em_staqc_"):
            return db_constants.QUERY_TYPES["QC"]
        else:
            raise ValueError(f"{helpers.full_class_name(self)}._get_query_type_info(): Error: Query type could not be determined.")

    def get_report_row_id(self):
        return self._report_row_id

    def get_query_formula_id(self):
        return self._query_formula_id

    def get_formula_prefix(self):
        return self._formula_prefix
   
    def get_query_type(self):
        return self._query_type

    def get_priority(self): 
        return self._priority

    def get_time_series_name(self): 
        return self._time_series_name