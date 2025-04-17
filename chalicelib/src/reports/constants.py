FIRST_DATA_ROW = 2    # 1-based
FIRST_DATA_COLUMN = 3 # 1-based

# column labels conforming to the national report excel template (2024)
STATE_AGGREGATESTO_COL_LABEL = "A"
STATE_AGGREGATIONSIGNANDFACTOR_COL_LABEL = "B"
FORMULA_COL_LABEL = "C"
NATIONAL_Y1990_QUANTITY_COL_LABEL = "D"
STATE_ROW_IN_NUMBER_LABEL = "D"
STATE_COL_LABEL = "E"
STATE_Y1990_QUANTITY_COL_LABEL = "F"

# column positions
STATE_AGGREGATESTO_COL_POS =  ord(STATE_AGGREGATESTO_COL_LABEL) - ord("A")
STATE_AGGREGATIONSIGNANDFACTOR_COL_POS =  ord(STATE_AGGREGATIONSIGNANDFACTOR_COL_LABEL) - ord("A")
FORMULA_COL_POS =  ord(FORMULA_COL_LABEL) - ord("A")
NATIONAL_Y1990_QUANTITY_COL_POS = ord(NATIONAL_Y1990_QUANTITY_COL_LABEL) - ord("A")
STATE_Y1990_QUANTITY_COL_POS = ord(STATE_Y1990_QUANTITY_COL_LABEL) - ord("A")
STATE_COL_POS = ord(STATE_COL_LABEL) - ord("A")
STATE_ROW_IN_NUMBER_POS = ord(STATE_ROW_IN_NUMBER_LABEL) - ord("A")

# tab names
REPORT_INPUT_DATA_SHEET_NAME = "Queries"
REPORT_OUTPUT_DATA_SHEET_NAME = "Query_Results"

# header names
STATE_REPORT_AGGREGATESTO_COLUMN_HEADER = "Aggregates To"
STATE_REPORT_AGGREGATIONSIGNANDFACTOR_COLUMN_HEADER = "Aggregation Sign and Factor"
REPORT_FORMULA_COLUMN_HEADER = "Formula"
STATE_REPORT_ROW_IN_NUMBER_COLUMN_HEADER = "In Row"
STATE_REPORT_STATE_COLUMN_HEADER = "State"

# validation error messages (must be SQL-compatible)
BLANK_FORMULA_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Empty Formula cell found. Please remove row or populate cell with a formula."
MISSING_QUERY_RESULTS_TAB_ERROR_MSG = "Excel file is missing the Query_Results tab. Please add this tab before processing the report."
FORMULA_PREFIX_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Failed to find Formula Prefix `{field_value}` in lookup table."
UNQUOTED_PARAMETER_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. One or more parameters are missing single quotes. Ensure all parameter values are wrapped in single quotes ('')"
PARAMETER_COUNT_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Expected {expected_param_count} parameters, got {input_param_count}."
#use error messages from general.qc_constants

VALIDATION_RESULTS = {
    "SUCCESS": "success",
    "FAILED": "failed",
    "NOT_VALIDATED": "not yet validated",
}

REPORT_TYPES = {
    "UNSPECIFIED": "Unspecified Report Type",
    "NATIONAL": "National Report", 
    "STATE": "State Report",
}

QUERIES_PER_REQUEST = 10