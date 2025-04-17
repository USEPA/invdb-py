DB_TABLES = {
    "ACTIVITY_KEY": "ggds_invdb.activity_key",
    "CALCULATION_FACTOR": "ggds_invdb.calculation_factor",
    "CALCULATION_INPUT": "ggds_invdb.calculation_input",
    "CRT_KEY": "ggds_invdb.crt_key",
    "DIM_CATEGORY": "ggds_invdb.dim_category",
    "DIM_DATA_TYPE": "ggds_invdb.dim_data_type",
    "DIM_EMISSIONSQC_LOAD_TARGET": "ggds_invdb.dim_emissionsqc_load_target",
    "DIM_FUEL_TYPE": "ggds_invdb.dim_fuel_type",
    "DIM_GHG": "ggds_invdb.dim_ghg",
    "DIM_GHG_CATEGORY": "ggds_invdb.dim_ghg_category",
    "DIM_LAYER": "ggds_invdb.dim_layer",
    "DIM_PUBLICATION": "ggds_invdb.dim_publication",
    "DIM_PUBLICATION_YEAR": "ggds_invdb.dim_publication_year",
    "DIM_QC_COMP_REPORT_ROW": "ggds_invdb.dim_qc_comp_report_row",
    "DIM_QC_REPORT": "ggds_invdb.dim_qc_report",
    "DIM_QUERY_FORMULA": "ggds_invdb.dim_query_formula",
    "DIM_QUERY_TYPE": "ggds_invdb.dim_query_type",
    "DIM_REDACTED_GHG": "ggds_invdb.dim_redacted_ghg",
    "DIM_REPORT": "ggds_invdb.dim_report",
    "DIM_REPORT_ROW": "ggds_invdb.dim_report_row",
    "DIM_SECTOR": "ggds_invdb.dim_sector",
    "DIM_SECTOR_TEMP": "ggds_invdb.dim_sector_temp",
    "DIM_SIMPLE_QUERY": "ggds_invdb.dim_simple_query",
    "DIM_SOURCE_NAME": "ggds_invdb.dim_source_name",
    "DIM_STATE": "ggds_invdb.dim_state",
    "DIM_SUBSECTOR": "ggds_invdb.dim_subsector",
    "DIM_TIME_SERIES": "ggds_invdb.dim_time_series",
    "EMISSIONS_KEY": "ggds_invdb.emissions_key",
    "EMISSIONSQC_KEY": "ggds_invdb.emissionsqc_key",
    "FACTS_ARCHIVE": "ggds_invdb.facts_archive",
    "JOB_LIST": "ggds_invdb.job_list",
    "JOB_STATUS": "ggds_invdb.job_status",
    "JOB_EVENT": "ggds_invdb.job_event",
    "PUBLICATION_OBJECT": "ggds_invdb.publication_object",
    "PUBLICATION_VERSION": "ggds_invdb.publication_version",
    "QC_ANALYTICS_VIEWER": "ggds_invdb.qc_analytics_viewer",
    "REPORT": "ggds_invdb.report",
    "REFRESH_STATUS_ROLLUP_TABLE": "ggds_invdb.refresh_status_rollup_table",
    "SOURCE_FILE": "ggds_invdb.source_file",
    "SOURCE_FILE_ATTACHMENT": "ggds_invdb.source_file_attachment",
    "VALIDATION_LOG_LOAD": "ggds_invdb.validation_log_load",
    "VALIDATION_LOG_REPORT": "ggds_invdb.validation_log_report",
    "VALIDATION_LOG_EXTRACT": "ggds_invdb.validation_log_extract"
}

DB_FUNCTIONS = {
    "F_REFRESH_ROLLUP_TABLE": "ggds_invdb.f_refresh_rollup_table"
}

DB_QUERY_FUNCTIONS = {
    "DELETE_ACTIVITY_FACTS_ARCHIVE": "ggds_invdb.delete_activity_facts_archive",
    "DELETE_CRT_FACTS_ARCHIVE" : "ggds_invdb.delete_crt_facts_archive",
    "DELETE_EMISSIONS_FACTS_ARCHIVE" : "ggds_invdb.delete_emissions_facts_archive",
    "DELETE_EMISSIONS_QC_FACTS_ARCHIVE" : "ggds_invdb.delete_emissions_qc_facts_archive",
}

LAYER_IDS = {1: "National", 2: "State"}

"""in each triple of DIM_TABLE_VALUE_MAPPINGS: 
[0]: source file column
[1]: dim table name in the ggds_invdb database schema
[2]: SQL SELECT statement column list for columns needed in 
    validation process"""
DIM_TABLE_VALUE_MAPPINGS = [
    ("data_type", DB_TABLES["DIM_DATA_TYPE"], "data_type_name", "data_type_id"),
    ("sector", DB_TABLES["DIM_SECTOR"], "sector_name", "sector_id"),
    ("subsector", DB_TABLES["DIM_SUBSECTOR"], "subsector_name", "subsector_id"),
    ("category", DB_TABLES["DIM_CATEGORY"], "category_name", "category_id"),
    ("fuel", DB_TABLES["DIM_FUEL_TYPE"], "fuel_type_name", "fuel_type_id"),
    ("ghg", DB_TABLES["DIM_GHG"], "ghg_longname, ghg_code, ghg_shortname, cas_no", "ghg_id"),
    ("ghg_category", DB_TABLES["DIM_GHG_CATEGORY"], "ghg_category_name", "ghg_category_id"),
    ("year", DB_TABLES["DIM_TIME_SERIES"], "year", "year_id"),
]

PUBLICATION_QUERY_FUNCTIONS = {
    # snapshot/prepare functions
    "EM_STA_SECTOR_SNAPSHOT": "ggds_invdb.EM_Sta_Sector_Snapshot" ,
    "EM_STA_ALL_SNAPSHOT": "ggds_invdb.EM_Sta_All_Snapshot",
    "EM_NAT_ALL_SNAPSHOT": "ggds_invdb.EM_Sta_All_Snapshot", # does the same thing as the EM_STA_ALL_SNAPSHOT
    "EM_NAT_SECTOR_SNAPSHOT": "ggds_invdb.EM_Nat_Sector_Snapshot",
    "EM_STA_POWERUSERS_SNAPSHOT": "ggds_invdb.EM_Sta_PowerUsers_Snapshot",
    "EM_NAT_POWERUSERS_SNAPSHOT": "ggds_invdb.EM_Sta_PowerUsers_Snapshot",
    # refined/redact functions
    "EM_NAT_ALL_REFINED": "ggds_invdb.EM_Nat_All_Refined",
    "EM_STA_ECONSECT_BYST_REFINED": "ggds_invdb.EM_Sta_EconSect_BySt_Refined",
    "EM_STA_SUBSECTOR_BYST_REFINED": "ggds_invdb.EM_Sta_Subsector_BySt_Refined",
    "EM_STA_ALL_REFINED": "ggds_invdb.EM_Sta_All_Refined",
    "EM_STA_POWERUSERS_REFINED": "ggds_invdb.EM_Sta_PowerUsers_Refined",
    "EM_NAT_POWERUSERS_REFINED": "ggds_invdb.EM_Nat_PowerUsers_Refined",
    "ACT_STA_POPGDP": "ggds_invdb.ACT_Sta_PopGDP",
}
PUBLICATION_ACTIONS = {"PREPARE": "Prepare", "REFINE": "Refine"}
PUBLICATION_VERSION_NAME_IDS = {1: "Draft", 2: "Interim", 3: "Final"}
EARLIEST_REPORTING_YEAR = 1990
EARLIEST_PUBLICATION_YEAR = 2014

QUERIES_PER_REQUEST = 10 # used for batch processing queries for reports
QUERY_TYPES = {
    "COMPLEX": {"name": "complex", 
                "time_series_name": "emissions"},
    "SIMPLE": {"name": "simple", 
               "time_series_name": "emissions"},
    "SIMPLE_QC":   {"name": "simple_QC", 
             "time_series_name": "QC"},
    "COMPLEX_QC":   {"name": "complex_QC", 
             "time_series_name": "QC"}
}
QUERY_CLASSES = {
    "COMPLEX": {"name": "COMPLEX", },
    "SIMPLE": {"name": "SIMPLE", },
}

REPORT_TYPE_IDS = {1: "Emissions", 2: "QC"}

ROLLUP_TABLES_REFRESH_TIMEOUT = 30 # in seconds
ROLLUP_TABLES_REFRESH_CHECK_INTERVAL = 2 # in seconds