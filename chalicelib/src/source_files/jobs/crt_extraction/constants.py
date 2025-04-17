CRT_TAB_NAMES = [
    "CRT Input",
    "CRT Reporter Input",
    "CRF Input",
    "CRF Reporter Input",
    "CRF Reporter Input (2)",
]

FIRST_INPUT_LINE_NUMBER=15
CRT_DATA_TYPE_ID = 6
HEADER_LEVEL_INDENT_WIDTH = 1

COLUMN_LABELS_BY_VERSION = {
    2: {
        "UID": "A",
        "CRT_INPUT": "B",
        "ALT_VAR_NAME": "C",
        "GAS": "D",
        "UNIT": "E",
        "Y1990": "F"
    },
    3: {
        "UID": "A",
        "CRT_INPUT": "B",
        "ALT_VAR_NAME": "C",
        "GAS": "D",
        "UNIT": "E",
        "Y1990": "F"
    }
}

# column positions per template
"""positions corresponding to the column labels, but translated to positions in a list 
(helpful when you access a row of the excel sheet (e.g. workbook["17"]))"""
COLUMN_POSITIONS_BY_VERSION = {
    2: {
        'UID': ord(COLUMN_LABELS_BY_VERSION[2]['UID']) - ord("A"),
        'CRT_INPUT': ord(COLUMN_LABELS_BY_VERSION[2]['CRT_INPUT']) - ord("A"),
        'ALT_VAR_NAME': ord(COLUMN_LABELS_BY_VERSION[2]['ALT_VAR_NAME']) - ord("A"),
        'GAS': ord(COLUMN_LABELS_BY_VERSION[2]['GAS']) - ord("A"),
        'UNIT': ord(COLUMN_LABELS_BY_VERSION[2]['UNIT']) - ord("A"),
        'Y1990': ord(COLUMN_LABELS_BY_VERSION[2]['Y1990']) - ord("A")
    },
    3: {
         'UID': ord(COLUMN_LABELS_BY_VERSION[2]['UID']) - ord("A"),
        'CRT_INPUT': ord(COLUMN_LABELS_BY_VERSION[2]['CRT_INPUT']) - ord("A"),
        'ALT_VAR_NAME': ord(COLUMN_LABELS_BY_VERSION[2]['ALT_VAR_NAME']) - ord("A"),
        'GAS': ord(COLUMN_LABELS_BY_VERSION[2]['GAS']) - ord("A"),
        'UNIT': ord(COLUMN_LABELS_BY_VERSION[2]['UNIT']) - ord("A"),
        'Y1990': ord(COLUMN_LABELS_BY_VERSION[2]['Y1990']) - ord("A")
    }
}