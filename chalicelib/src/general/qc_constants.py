EARLIEST_REPORTING_YEAR = 1990

FACTS_ARCHIVE_VALUE_MAX_LENGTH = 50

# max lengths for emissions data TEXT columns in the database
EMISSIONS_UID_MAX_LENGTH = 250
SUB_CATEGORY_MAX_LENGTH = 100
CARBON_POOL_MAX_LENGTH = 100
GEO_REF_MAX_LENGTH = 20
CRT_CODE_MAX_LENGTH = 250
ID_MAX_LENGTH = 250
CBI_ACTIVITY_MAX_LENGTH = 250
UNITS_MAX_LENGTH = 100
GHG_CATEGORY_MAX_LENGTH = 50

YEAR_ALPHA_VALUES = ["NE", "IE", "C", "NA", "NO"]

# validation error messages
DATA_TYPE_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Failed to find Data Type `{field_value}` in lookup table. Only a value from {dim_values} is accepted."
SECTOR_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Failed to find Sector `{field_value}` in lookup table."
SUBSECTOR_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Failed to find Subsector `{field_value}` in lookup table."
CATEGORY_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Failed to find Category `{field_value}` in lookup table."
SUBCATEGORY1_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Failed to find Subcategory1 `{field_value}` in lookup table."
FUEL1_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Failed to find Fuel1 `{field_value}` in lookup table."
FUEL2_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Failed to find Fuel2 `{field_value}` in lookup table."
GHG_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Failed to find GHG `{field_value}` in lookup table."
GHG_LONGNAME_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Failed to find GHG `{field_value}` in lookup table."
GHG_CATEGORY_NAME_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Failed to find GHG Category Name `{field_value}` in lookup table."
YEAR_INVALID_ERROR_MSG = "Error found in Row: {row_number}, Column: {column_label}. Emission quantity error - only numbers or one of the following texts: {dim_values} is accepted."
SUB_CATEGORY_INVALID_ERROR_MSG = (
    "Error found in Row: {row_number}, Column: {column_label}."
    + f" Invalid Subcategory Error - only strings of maximum length {SUB_CATEGORY_MAX_LENGTH} or NULL are accepted."
)
CARBON_POOL_INVALID_ERROR_MSG = (
    "Error found in Row: {row_number}, Column: {column_label}."
    + f" Invalid Carbon Pool Error - only strings of maximum length {CARBON_POOL_MAX_LENGTH} or NULL are accepted."
)
GEO_REF_INVALID_ERROR_MSG = (
    "Error found in Row: {row_number}, Column: {column_label}."
    + f" Invalid Geo Ref Error - only strings of maximum length {GEO_REF_MAX_LENGTH} or NULL are accepted."
)
EXCLUDE_INVALID_ERROR_MSG = (
    "Error found in Row: {row_number}, Column: {column_label}."
    + f" Invalid Exclude Error - only the values `Y`, `N` (case-insensitive) or NULL are accepted."
)
CRT_CODE_INVALID_ERROR_MSG = (
    "Error found in Row: {row_number}, Column: {column_label}."
    + f" Invalid CRT Code Error - only strings of maximum length {CRT_CODE_MAX_LENGTH} or NULL are accepted."
)
ID_INVALID_ERROR_MSG = (
    "Error found in Row: {row_number}, Column: {column_label}."
    + f" Invalid ID Error - only strings of maximum length {ID_MAX_LENGTH} or NULL are accepted."
)
CBI_ACTIVITY_INVALID_ERROR_MSG = (
    "Error found in Row: {row_number}, Column: {column_label}."
    + f" Invalid CBI Activity Error - only strings of maximum length {CBI_ACTIVITY_MAX_LENGTH} or NULL are accepted."
)
UNITS_INVALID_ERROR_MSG = (
    "Error found in Row: {row_number}, Column: {column_label}."
    + f" Invalid Units Error - only strings of maximum length {UNITS_MAX_LENGTH} or NULL are accepted."
)
GHG_CATEGORY_INVALID_ERROR_MSG = (
    "Error found in Row: {row_number}, Column: {column_label}."
    + f" Invalid GHG Category Error - only strings of up to maximum length {GHG_CATEGORY_MAX_LENGTH} or NULL are accepted."
)
GWP_INVALID_ERROR_MSG = (
    "Error found in Row: {row_number}, Column: {column_label}."
    + f" Invalid GWP Error - only numbers or NULL are accepted."
)

EMISSIONS_KEY_COLUMNS = [
    "sector_id",
    "sub_sector_id",
    "category_id",
    "sub_category_1",
    "sub_category_2",
    "sub_category_3",
    "sub_category_4",
    "sub_category_5",
    "carbon_pool",
    "fuel_type_id_1",
    "fuel_type_id_2",
    "geo_ref",
    "EXCLUDE",
    "crt_code",
    "id",
    "cbi_activity",
    "units",
    "ghg_category",
    "ghg_id",
    "gwp"
]

EMISSIONS_KEY_RAW_COLUMNS = [
    "sector",
    "subsector",
    "category",
    "sub_category_1",
    "sub_category_2",
    "sub_category_3",
    "sub_category_4",
    "sub_category_5",
    "carbon_pool",
    "fuel1",
    "fuel2",
    "geo_ref",
    "exclude",
    "crt_code",
    "id",
    "cbi_activity",
    "units",
    "ghg_category",
    "ghg",
    "gwp"
]
