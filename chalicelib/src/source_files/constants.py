SOURCE_FILE_DATA_SHEET_NAME = "InvDB"

def TEMPLATE(reporting_year):
    if reporting_year >= 2025:
        return 3
    return 2

# basic source file info per template
INFO = {
    2: { # template 2
        'FIRST_DATA_ROW': 17,
        'FIRST_DATA_COLUMN': 1,
        'NUM_EMISSION_KEY_COLUMNS': 20
    },
    3: { # template 3
        'FIRST_DATA_ROW': 2,
        'FIRST_DATA_COLUMN': 1,
        'NUM_EMISSION_KEY_COLUMNS': 21
    }
}

# column labels per template
LABELS = {
    2: {
        'DATA_TYPE_COL_LABEL': "A",
        'SECTOR_COL_LABEL': "B",
        'SUBSECTOR_COL_LABEL': "C",
        'CATEGORY_COL_LABEL': "D",
        'SUB_CATEGORY_1_COL_LABEL': "E",
        'SUB_CATEGORY_2_COL_LABEL': "F",
        'SUB_CATEGORY_3_COL_LABEL': "G",
        'SUB_CATEGORY_4_COL_LABEL': "H",
        'SUB_CATEGORY_5_COL_LABEL': "I",
        'CARBON_POOL_COL_LABEL': "J",
        'FUEL1_COL_LABEL': "K",
        'FUEL2_COL_LABEL': "L",
        'GEO_REF_COL_LABEL': "M",
        'EXCLUDE_COL_LABEL': "N",
        'CRT_CODE_COL_LABEL': "O",
        'ID_COL_LABEL': "P",
        'CBI_ACTIVITY_COL_LABEL': "Q",
        'UNITS_COL_LABEL': "R",
        'GHG_COL_LABEL': "S",
        'GWP_COL_LABEL': "T",
        'Y1990_COL_LABEL': "U"
    },
    3: {
        'DATA_TYPE_COL_LABEL': "A",
        'SECTOR_COL_LABEL': "B",
        'SUBSECTOR_COL_LABEL': "C",
        'CATEGORY_COL_LABEL': "D",
        'SUB_CATEGORY_1_COL_LABEL': "E",
        'SUB_CATEGORY_2_COL_LABEL': "F",
        'SUB_CATEGORY_3_COL_LABEL': "G",
        'SUB_CATEGORY_4_COL_LABEL': "H",
        'SUB_CATEGORY_5_COL_LABEL': "I",
        'CARBON_POOL_COL_LABEL': "J",
        'FUEL1_COL_LABEL': "K",
        'FUEL2_COL_LABEL': "L",
        'GEO_REF_COL_LABEL': "M",
        'EXCLUDE_COL_LABEL': "N",
        'CRT_CODE_COL_LABEL': "O",
        'ID_COL_LABEL': "P",
        'CBI_ACTIVITY_COL_LABEL': "Q",
        'UNITS_COL_LABEL': "R",
        'GHG_CATEGORY_COL_LABEL': "S",
        'GHG_COL_LABEL': "T",
        'GWP_COL_LABEL': "U",
        'Y1990_COL_LABEL': "V"
    }
}

# column positions per template
"""positions corresponding to the column labels, but translated to positions in a list 
(helpful when you access a row of the excel sheet (e.g. workbook["17"]))"""
POSITIONS = {
    2: {
        'DATA_TYPE_COL_POS': ord(LABELS[2]['DATA_TYPE_COL_LABEL']) - ord("A"),
        'SECTOR_COL_POS': ord(LABELS[2]['SECTOR_COL_LABEL']) - ord("A"),
        'SUBSECTOR_COL_POS': ord(LABELS[2]['SUBSECTOR_COL_LABEL']) - ord("A"),
        'CATEGORY_COL_POS': ord(LABELS[2]['CATEGORY_COL_LABEL']) - ord("A"),
        'SUB_CATEGORY_1_COL_POS': ord(LABELS[2]['SUB_CATEGORY_1_COL_LABEL']) - ord("A"),
        'SUB_CATEGORY_2_COL_POS': ord(LABELS[2]['SUB_CATEGORY_2_COL_LABEL']) - ord("A"),
        'SUB_CATEGORY_3_COL_POS': ord(LABELS[2]['SUB_CATEGORY_3_COL_LABEL']) - ord("A"),
        'SUB_CATEGORY_4_COL_POS': ord(LABELS[2]['SUB_CATEGORY_4_COL_LABEL']) - ord("A"),
        'SUB_CATEGORY_5_COL_POS': ord(LABELS[2]['SUB_CATEGORY_5_COL_LABEL']) - ord("A"),
        'CARBON_POOL_COL_POS': ord(LABELS[2]['CARBON_POOL_COL_LABEL']) - ord("A"),
        'FUEL1_COL_POS': ord(LABELS[2]['FUEL1_COL_LABEL']) - ord("A"),
        'FUEL2_COL_POS': ord(LABELS[2]['FUEL2_COL_LABEL']) - ord("A"),
        'GEO_REF_COL_POS': ord(LABELS[2]['GEO_REF_COL_LABEL']) - ord("A"),
        'EXCLUDE_COL_POS': ord(LABELS[2]['EXCLUDE_COL_LABEL']) - ord("A"),
        'CRT_CODE_COL_POS': ord(LABELS[2]['CRT_CODE_COL_LABEL']) - ord("A"),
        'ID_COL_POS': ord(LABELS[2]['ID_COL_LABEL']) - ord("A"),
        'CBI_ACTIVITY_COL_POS': ord(LABELS[2]['CBI_ACTIVITY_COL_LABEL']) - ord("A"),
        'UNITS_COL_POS': ord(LABELS[2]['UNITS_COL_LABEL']) - ord("A"),
        'GHG_COL_POS': ord(LABELS[2]['GHG_COL_LABEL']) - ord("A"),
        'GWP_COL_POS': ord(LABELS[2]['GWP_COL_LABEL']) - ord("A"),
        'Y1990_COL_POS': ord(LABELS[2]['Y1990_COL_LABEL']) - ord("A")
    },
    3: {
        'DATA_TYPE_COL_POS': ord(LABELS[3]['DATA_TYPE_COL_LABEL']) - ord("A"),
        'SECTOR_COL_POS': ord(LABELS[3]['SECTOR_COL_LABEL']) - ord("A"),
        'SUBSECTOR_COL_POS': ord(LABELS[3]['SUBSECTOR_COL_LABEL']) - ord("A"),
        'CATEGORY_COL_POS': ord(LABELS[3]['CATEGORY_COL_LABEL']) - ord("A"),
        'SUB_CATEGORY_1_COL_POS': ord(LABELS[3]['SUB_CATEGORY_1_COL_LABEL']) - ord("A"),
        'SUB_CATEGORY_2_COL_POS': ord(LABELS[3]['SUB_CATEGORY_2_COL_LABEL']) - ord("A"),
        'SUB_CATEGORY_3_COL_POS': ord(LABELS[3]['SUB_CATEGORY_3_COL_LABEL']) - ord("A"),
        'SUB_CATEGORY_4_COL_POS': ord(LABELS[3]['SUB_CATEGORY_4_COL_LABEL']) - ord("A"),
        'SUB_CATEGORY_5_COL_POS': ord(LABELS[3]['SUB_CATEGORY_5_COL_LABEL']) - ord("A"),
        'CARBON_POOL_COL_POS': ord(LABELS[3]['CARBON_POOL_COL_LABEL']) - ord("A"),
        'FUEL1_COL_POS': ord(LABELS[3]['FUEL1_COL_LABEL']) - ord("A"),
        'FUEL2_COL_POS': ord(LABELS[3]['FUEL2_COL_LABEL']) - ord("A"),
        'GEO_REF_COL_POS': ord(LABELS[3]['GEO_REF_COL_LABEL']) - ord("A"),
        'EXCLUDE_COL_POS': ord(LABELS[3]['EXCLUDE_COL_LABEL']) - ord("A"),
        'CRT_CODE_COL_POS': ord(LABELS[3]['CRT_CODE_COL_LABEL']) - ord("A"),
        'ID_COL_POS': ord(LABELS[3]['ID_COL_LABEL']) - ord("A"),
        'CBI_ACTIVITY_COL_POS': ord(LABELS[3]['CBI_ACTIVITY_COL_LABEL']) - ord("A"),
        'UNITS_COL_POS': ord(LABELS[3]['UNITS_COL_LABEL']) - ord("A"),
        'GHG_CATEGORY_COL_POS': ord(LABELS[3]['GHG_CATEGORY_COL_LABEL']) - ord("A"),
        'GHG_COL_POS': ord(LABELS[3]['GHG_COL_LABEL']) - ord("A"),
        'GWP_COL_POS': ord(LABELS[3]['GWP_COL_LABEL']) - ord("A"),
        'Y1990_COL_POS': ord(LABELS[3]['Y1990_COL_LABEL']) - ord("A")
    }
}

# used for jsons (headers -> constants)
MAPPINGS = {
    'Data Type': 'DATA_TYPE_COL_LABEL',
    'Sector': 'SECTOR_COL_LABEL',
    'Subsector': 'SUBSECTOR_COL_LABEL',
    'Category': 'CATEGORY_COL_LABEL',
    'Subcategory1': 'SUB_CATEGORY_1_COL_LABEL',
    'Subcategory2': 'SUB_CATEGORY_2_COL_LABEL',
    'Subcategory3': 'SUB_CATEGORY_3_COL_LABEL',
    'Subcategory4': 'SUB_CATEGORY_4_COL_LABEL',
    'Subcategory5': 'SUB_CATEGORY_5_COL_LABEL',
    'Carbon Pool': 'CARBON_POOL_COL_LABEL',
    'Fuel1': 'FUEL1_COL_LABEL',
    'Fuel2': 'FUEL2_COL_LABEL',
    'GeoRef': 'GEO_REF_COL_LABEL',
    'Exclude': 'EXCLUDE_COL_LABEL',
    'CRT Code': 'CRT_CODE_COL_LABEL',
    'ID': 'ID_COL_LABEL',
    'Sensitive (Y or N)': 'CBI_ACTIVITY_COL_LABEL',
    'Units': 'nothing',
    'GHG Category': 'GHG_CATEGORY_COL_LABEL',
    'GHG': 'GHG_COL_LABEL',
    'GWP': 'GWP_COL_LABEL'
    # '1990': 'Y1990_COL_LABEL'
}

VALIDATION_RESULTS = {
    "SUCCESS": "success",
    "FAILED": "failed",
    "NOT_VALIDATED": "not yet validated",
}
