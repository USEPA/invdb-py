# enumeration of statuses
STATUSES = {"IN_PROGRESS": "In Progress", "COMPLETE": "Complete", "ERROR": "Error"}

########## info organized by job: ###############

# source file validation info
SOURCE_FILE_VALIDATION_NAME = "Source File Validation"
SOURCE_FILE_VALIDATION_DESC = "Checks data validity of source files for a specified reporting year layer, and then reports errors in the validation_log_load table"

# source file load info
SOURCE_FILE_LOAD_NAME = "Source File Load"
SOURCE_FILE_LOAD_DESC = "Loads valid source file data for a specified reporting year and layer into the emissions_key and facts_archive tables"
SOURCE_FILE_LOAD_FETCHING_FILES_EVENT = {
    "name": "Gathering excel files",
    "details": "Fetching source files from the database based on specified reporting year and layer",
}
SOURCE_FILE_LOAD_FETCHING_ERRORS_EVENT = {
    "name": "Fetching validation errors",
    "details": "Fetching validation errors for source files",
}
SOURCE_FILE_LOAD_OPENING_FILES_EVENT = {
    "name": "Opening the source files",
    "details": "Opening the source files for reading and writing",
}
SOURCE_FILE_LOAD_REMOVING_INVALID_DATA_EVENT = {
    "name": "Removing invalid data",
    "details": "Omitting rows from source files where validation errors are reported",
}
SOURCE_FILE_LOAD_MAPPING_DATA_EVENT = {
    "name": "Mapping data to foreign keys",
    "details": "Exchanging data values with their foreign key values based on dim tables",
}
SOURCE_FILE_LOAD_GATHERING_DATA_EVENT = {
    "name": "Gathering emission key and facts data",
    "details": "Constructing the rows to be inserted into the emission_key and facts_archive tables",
}
SOURCE_FILE_LOAD_UPDATING_DATABASE_EVENT = {
    "name": "Updating the database",
    "details": "Committing the change block for changes to appear in the database",
}
SOURCE_FILE_LOAD_COMPLETED_LOAD_EVENT = {
    "name": "Completed data load",
    "details": "Completed load process for all source files",
}

SOURCE_FILE_LOAD_UPDATING_SUMMARY_TABLES_EVENT = {
    "name": "Updating Summary Tables",
    "details": "Updating Summary Tables",
}

# qc extraction info
QC_EXTRACTION_NAME = "Quality Control Extraction"
QC_EXTRACTION_DESC = "Pulls QC information from spreadsheets specified by dim_emissionsqc_load_target and stores them into facts_archive and emissionsqc_key"
QC_EXTRACTION_FETCHING_SOURCE_FILES_EVENT = {
    "name": "Fetching the Source Files",
    "details": "Fetching the specified source files",
}
QC_EXTRACTION_EXTRACTING_DATA_EVENT = {
    "name": "Extracting QC Data",
    "details": "Extracting QC data from the source files",
}
QC_EXTRACTION_WRITING_TO_DATABASE_EVENT = {
    "name": "Extracting QC Data",
    "details": "Writing the results of the extraction step to the database",
}
QC_EXTRACTION_UPDATING_VALIDATION_LOGS_EVENT = {
    "name": "Updating Validation Error Logs",
    "details": "Updating the validation error logs with any validation errors found",
}

# report validation info
REPORT_VALIDATION_NAME = "Report Validation"
REPORT_VALIDATION_DESC = "Checks formula validity of reports for a specified reporting year and layer, and then reports errors in the validation_log_report table"

# report processing info
REPORT_PROCESSING_NAME = "Report Processing"
REPORT_PROCESSING_DESC = "Gathers emissions quantities by calling query functions and places the results into the Query_Result excel tab"
REPORT_PROCESSING_FETCHING_FILES_EVENT = {
    "name": "Gathering excel files",
    "details": "Fetching reports from the database based on specified reporting year and layer",
}
REPORT_PROCESSING_FETCHING_ERRORS_EVENT = {
    "name": "Fetching validation errors",
    "details": "Fetching validation errors for the reports",
}
REPORT_PROCESSING_OPENING_REPORTS_EVENT = {
    "name": "Opening the reports",
    "details": "Opening the report files for reading and writing",
}
REPORT_PROCESSING_PROCESSING_QUERY_BATCH_EVENT = {
    "name": "Processing a query batch",
    "details": "Processing the query batch {} of {} for report with ID {}",
}
REPORT_PROCESSING_WRITING_BATCH_RESULTS_EVENT = {
    "name": "Writing the results from a query batch",
    "details": "Writing the results of query batch {} of {} into the report with ID {}",
}
REPORT_PROCESSING_REPORT_PROCESSING_SUCCESSFUL_EVENT = {
    "name": "Report processed successfully",
    "details": "Report with report ID {} processed to completion",
}
REPORT_PROCESSING_REPORT_PROCESSING_FAILED_EVENT = {
    "name": "Report failed to process",
    "details": "Report with report ID {} failed to process due to an unhandled error. Refer to console logs.",
}
REPORT_PROCESSING_COMPLETED_PROCESSING_EVENT = {
    "name": "Report processing completed",
    "details": "Report processing request has been completed",
}

# report processing info
LOAD_ONLINE_REPORT_NAME = "Online Report Loading"
LOAD_ONLINE_REPORT_DESC = "Fetches report queries from database and returns results from query engine"
LOAD_ONLINE_REPORT_FETCHING_QUERY_INFO_EVENT = {
    "name": "Fetching query information",
    "details": "Fetching all pertinent to executing all queries belonging to the input report ID",
}
LOAD_ONLINE_REPORT_PREPARING_QUERIES_EVENT = {
    "name": "Preparing report queries",
    "details": "Preparing query information for processing",
}
LOAD_ONLINE_REPORT_PROCESSING_QUERIES_EVENT = {
    "name": "Processing report queries",
    "details": "Routing query information to query engine for processing",
}
LOAD_ONLINE_REPORT_CONSTRUCTING_RESPONSE_OBJECT_EVENT = {
    "name": "Constucting response object",
    "details": "Combining all query results into a single API response object",
}

# publication processing info
PUBLICATION_PROCESSING_NAME = "Publication Processing"
PUBLICATION_PROCESSING_DESC = "Executes an action for a publication data product as specified by a Publication Object ID and Action button text"
PUBLICATION_PROCESSING_FETCHING_DATA_EVENT = {
    "name": "Fetching Data from the Database",
    "details": "Fetching necessary data from the database",
}
PUBLICATION_PROCESSING_PROCESSING_QUERY_RESULTS_EVENT = {
    "name": "Processing the Query Results",
    "details": "Transforming the fetched data into the desired structure and format",
}
PUBLICATION_PROCESSING_WRITING_TO_DATABASE_EVENT = {
    "name": "Writing the Output Data to the Database",
    "details": "Writing the results of the processing step to the database and updating metadata",
}

# publication download info
PUBLICATION_DOWNLOAD_NAME = "Publication Download"
PUBLICATION_DOWNLOAD_DESC = "Converts one or more publication data products into excel files and sends them to the requestor"
PUBLICATION_DOWNLOAD_FETCHING_PUBLICATION_DATA_EVENT = {
    "name": "Fetching Publication Data from the Database",
    "details": "Fetching necessary data from the database",
}
PUBLICATION_DOWNLOAD_PROCESSING_DATA_INTO_EXCEL_EVENT = {
    "name": "Converting to Excel",
    "details": "Transforming the fetched data into the desired structure and format",
}
PUBLICATION_DOWNLOAD_PREPARING_FILE_EVENT = {
    "name": "Preparing the Download File",
    "details": "Preparing the excel file(s) for download",
}
PUBLICATION_DOWNLOAD_TRANSMITTING_FILE_EVENT = {
    "name": "Transmitting Download File",
    "details": "Sending the download file to the end user",
}
