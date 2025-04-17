import chalicelib.src.general.globals as globals # loads the environment variables first and foremost
from chalicelib.src.source_files.jobs.validation.methods import handle_source_file_validation_request
from chalicelib.src.source_files.jobs.qc_extraction.methods import handle_qc_extraction_request
from chalicelib.src.source_files.jobs.load.methods import handle_source_file_archiving_request
from chalicelib.src.reports.jobs.validation.methods import handle_report_validation_request
from chalicelib.src.reports.jobs.processing.methods import handle_report_processing_request
from chalicelib.src.reports.jobs.load_online_report.methods import handle_load_online_report_request
from chalicelib.src.publications.jobs.handle_action.methods import handle_publication_processing_request
from chalicelib.src.publications.jobs.download_excel.methods import handle_publication_download_request
from chalicelib.src.query_engine.jobs.execute_simple_query.methods import handle_simple_query_request
from chalicelib.src.query_engine.jobs.execute_complex_query.methods import handle_complex_query_request
from chalicelib.src.qc_analytics.jobs.recalculations_report.methods import handle_recalculations_report_request
from chalicelib.src.qc_analytics.jobs.download_recalculations_excel.methods import handle_recalculations_excel_download_request
from chalicelib.src.database.dbPoolConfigurator import DbPoolConfigurator
import chalicelib.src.general.qc_constants as qc_constants
import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.general.helpers as helpers 
from flask import Flask, jsonify, request, Blueprint
from multiprocessing import freeze_support
from datetime import datetime
from waitress import serve
import traceback
import json
import sys
import os


def get_parameters(request) -> dict:
    """Get the parameters passed into the request (first by POST, then by GET).
    Args:
        request: request object from the route handler context
    Returns:
        dict: A dictionary of parameters for the query
    """
    parameters = {}
    for key, value in request.args.items():
        parameters.update({key: value})

    return parameters


def assert_parameter_constraints(parameters, constraints):
    """
    inputs
    parameters: a dictionary    keys -> parameter name
                                values -> parameter value
    contraints: a dictionary    keys -> parameters name
                                values -> dictionary{   required: bool,
                                                        constraints: list of tuples ([0]: condition as a string, [1]: error message if condition evaluates to False)
                                                    }
    output: a string report of all constraint violations
    """

    violations_log = ""
    error_num = 1
    # enforce constraits for each passed parameter
    
    # if the parameter is required and present, report any violated constraints (need to include all unique constraints here)
    for parameter_name, parameter_constraints in constraints.items():
        # report if parameter is required but missing
        if (
            parameter_constraints["required"] == True
            and parameter_name not in parameters.keys()
        ):
            violations_log += (
                f"/t{error_num}.  Missing required parameter `{parameter_name}`./n"
            )
            error_num += 1
            continue

        # skip parameters that are neither required nor passed
        if (
            parameter_constraints["required"] == False
            and parameter_name not in parameters.keys()
        ):
            continue

        # assert lists are passed properly
        if parameter_constraints['type'] == list:
            try:
                [parameter_constraints['element_type'](element) for element in parameters[parameter_name].split(',')] # check to see if the parameter value is a comma-separated list of the expected element type
            except ValueError:
                violations_log += f"/t{error_num}.  Invalid type for parameter: `{parameter_name}`. Expecting a comma-separated list of {str(parameter_constraints['element_type'])[8:-2]}s./n"
                error_num += 1
                continue
        # for bool types, only True and False are accepted (case-insensitive)
        elif parameter_constraints['type'] == bool and not parameters[parameter_name].upper() in ["TRUE", "FALSE"]:
            violations_log += f"/t{error_num}.  Invalid type for element in parameter: `{parameter_name}`. Expecting a boolean value (`true` or `false`)./n"
            error_num += 1
            continue
        else: # for all other parameter types, try to cast to the required type
            try:
                parameter_constraints['type'](parameters[parameter_name])
            except ValueError:
                violations_log += f"/t{error_num}.  Invalid type for parameter: `{parameter_name}`. Expected type `{str(parameter_constraints['type'])[8:-2]}`, but got type `{str(type(parameters[parameter_name]))[8:-2]}`./n"
                error_num += 1
                continue 

        # check allowed values
        if 'allowed_values' in parameter_constraints:
            if parameter_constraints["type"] == str: # make string values case-insensitve
                if parameters[parameter_name].upper() not in [allowed_value.upper() for allowed_value in parameter_constraints['allowed_values']]:
                    violations_log += f"/t{error_num}.  Invalid value for parameter: `{parameter_name}`. The allowed values are {parameter_constraints['allowed_values']}. (without quotes)/n"
                    error_num += 1
                    continue 
            else:
                if parameter_constraints['type'] == list: 
                    for element in [e.strip() for e in parameters[parameter_name].strip(" ,").split(",")]:
                        if element not in parameter_constraints['allowed_values']:
                            violations_log += f"/t{error_num}.  Invalid value for parameter: `{parameter_name}`. The allowed values are {parameter_constraints['allowed_values']}. (without quotes)/n"
                            error_num += 1
                            continue                     
                else: 
                    if parameter_constraints['type'](parameters[parameter_name]) not in parameter_constraints['allowed_values']:
                        violations_log += f"/t{error_num}.  Invalid value for parameter: `{parameter_name}`. The allowed values are {parameter_constraints['allowed_values']}. (without quotes)/n"
                        error_num += 1
                        continue 

        # check value range
        if 'max' in parameter_constraints and int(parameters[parameter_name]) > parameter_constraints['max']:
            violations_log += f"/t{error_num}.  Maximum value exceeded by parameter: `{parameter_name}`. The maximum value is {parameter_constraints['max']}./n"
            error_num += 1
            continue 

        if 'min' in parameter_constraints and int(parameters[parameter_name]) < parameter_constraints['min']:
            violations_log += f"/t{error_num}.  Minimum value subceeded by parameter: `{parameter_name}`. The minimum value is {parameter_constraints['min']}./n"
            error_num += 1

    # return the violations report if there are any
    if violations_log:
        return (
            "Input constraint violations found. See the messages below:/n/n"
            + violations_log[:-1]
        )
    else:
        return


reporting_year_input_validation = {
    "reporting_year": {
        "required": True,
        "type": int,
        "min": qc_constants.EARLIEST_REPORTING_YEAR,
        "max": 999999
    }
}

layer_id_input_validation = {
    "layer_id": {
        "required": True,
        "type": int,
        "allowed_values": list(db_constants.LAYER_IDS.keys())
    }
}

debug_input_validation = {
    "debug": {
        "required": False,
        "type": bool
    },
}

report_id_input_validation = {
    "report_id": {
        "required": True,
        "type": int,
    }
}

report_type_id_input_validation = {
    "report_type_id": {
        "required": True,
        "type": int,
        "allowed_values": list(db_constants.REPORT_TYPE_IDS.keys())
    }
}

by_ids_input_validation = {
    "ids": {
        "required": False,
        "type": list,
        "element_type": int
    }
}

user_id_input_validation = {
    "user_id": {
        "required": True,
        "type": int,
    }
}

publication_object_id_input_validation = {
    "pub_object_id": {
        "required": True,
        "type": int,
    }
}

publication_object_ids_input_validation = {
    "pub_object_id": {
        "required": True,
        "type": list,
        "element_type": int
    }
}

publication_action_input_validation = {
    "action": {
        "required": True,
        "type": str,
        "allowed_values": ["prepare", "refine"]
    }
}

source_name_ids_input_validation = {
    "source_name_ids": {
        "required": True,
        "type": list,
        "element_type": int
    }
}

simple_query_input_validation = {
    "query": {
        "required": True,
        "type": list,
        "element_type": tuple
    }
}

complex_query_input_validation = {
    "query": {
        "required": True,
        "type": list,
        "element_type": str
    }
}

gwp_input_validation = {
    "gwp": {
        "required": False,
        "type": str,
        "allowed_values": ["ar4_gwp", "ar5_gwp", "ar5f_gwp", "ar6_gwp"]
    }
}

baseline_dataset_id_validation = {
    "baseline_dataset_id": {
        "required": True,
        "type": int
    }
}

comparator_dataset_id_validation = {
    "comparator_dataset_id": {
        "required": True,
        "type": int
    }
}

qc_analytics_parameter_validation = {
    "parameter": {
        "required": False,
        "type": str,
        "allowed_values": ["mmt", "percent"]
    }
}

qc_analytics_filter_by_validation = {
    "filter_by": {
        "required": False,
        "type": str,
        "allowed_values": ["ghg_category", "ghg"]
    }
}

qc_analytics_aggregate_at_validation = {
    "aggregate_at": {
        "required": False,
        "type": list,
        "element_type": str,
        "allowed_values": ["sub_category_2", "sub_category_3", "sub_category_4", "sub_category_5", "carbon_pool", "geo_ref"]
    }
}

qc_analytics_output_years_validation = {
    "output_years": {
        "required": False,
        "type": list,
        "element_type": int,
        "allowed_values": [str(element) for element in range(db_methods.fetch_max_time_series_by_reporting_year(datetime.now().year), 
                                                             db_methods.fetch_max_time_series_by_reporting_year(datetime.now().year) - 5, -1)]
    }
}

qca_object_handle_input_validation = {
    "qca_object_handle": {
        "required": True,
        "type": str,
    }
}



# define the API and its endpoints
app = Flask(__name__) 
app_base = Blueprint("python-api", __name__, url_prefix="/" + globals.APP_NAME)

# Configure Celery
# celery = Celery(app.name, broker="pyamqp://guest@localhost//")
# celery.conf.update(app.config)


@app_base.route("/")
def welcome():
    return (
        jsonify(
            {
                "weclome msg": "You've reached the Inventory database backend API. Congrats!"
            }
        ),
        200,
    )


# ===================== SOURCE FILE VALIDATION =====================


@app_base.route("/source-file-validation", methods=["GET", "POST"])
def source_file_validation_endpoint():
    try:
        helpers.tprint("Validation request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        constraints = {}
        constraints.update(reporting_year_input_validation)
        constraints.update(layer_id_input_validation)
        constraints.update(user_id_input_validation)
        constraints.update(debug_input_validation)
        constraints.update(by_ids_input_validation)
        input_violations = assert_parameter_constraints(
            parameters,
            constraints
        )
        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        helpers.tprint(f"Input parameters are valid. Proceeding with source file validation.")
        return handle_source_file_validation_request(
            int(parameters["reporting_year"]),
            int(parameters["layer_id"]),
            None if "user_id" not in parameters else int(parameters["user_id"]),
            None if "debug" not in parameters else bool(parameters["debug"]),
            None if "ids" not in parameters else [int(id.strip()) for id in parameters["ids"].strip(" ,").split(",")]
        )
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500


# ===================== QC EXTRACTION =====================

@app_base.route("/qc-extraction", methods=["GET", "POST"])
def qc_extraction_endpoint():
    try:
        helpers.tprint("QC extraction request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        constraints = {}
        constraints.update(source_name_ids_input_validation)
        constraints.update(reporting_year_input_validation)
        constraints.update(layer_id_input_validation)
        constraints.update(user_id_input_validation)
        constraints.update(debug_input_validation)
        input_violations = assert_parameter_constraints(
            parameters,
            constraints,
        )
        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        helpers.tprint(f"Input parameters are valid. Proceeding with qc extraction.")
        return handle_qc_extraction_request(
            None if "source_name_ids" not in parameters else [int(id.strip()) for id in parameters["source_name_ids"].strip(" ,").split(",")],
            None if "reporting_year" not in parameters else int(parameters["reporting_year"]),
            None if "layer_id" not in parameters else int(parameters["layer_id"]),
            None if "user_id" not in parameters else int(parameters["user_id"]),
            None if "debug" not in parameters else bool(parameters["debug"])
        )
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500


# ===================== SOURCE FILE LOAD =====================

@app_base.route("/source-file-load", methods=["GET", "POST"])
def source_file_load_endpoint():
    try:
        helpers.tprint("Loading request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        constraints = {}
        constraints.update(reporting_year_input_validation)
        constraints.update(layer_id_input_validation)
        constraints.update(user_id_input_validation)
        constraints.update(debug_input_validation)
        constraints.update(by_ids_input_validation)
        
        input_violations = assert_parameter_constraints(
            parameters,
            constraints,
        )
        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        helpers.tprint(f"Input parameters are valid. Proceeding with source file loading.")
        return handle_source_file_archiving_request(
            int(parameters["reporting_year"]),
            int(parameters["layer_id"]),
            None if "user_id" not in parameters else int(parameters["user_id"]),
            None if "debug" not in parameters else bool(parameters["debug"]),
            None if "ids" not in parameters else [int(id.strip()) for id in parameters["ids"].strip(" ,").split(",")]
        )
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500


# ===================== REPORT VALIDATION =====================

@app_base.route("/report-validation", methods=["GET", "POST"])
def report_validation_endpoint():
    try:
        # alert incoming reponse to console
        helpers.tprint("Report validation request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        
        # validate input parameters
        constraints = {}
        constraints.update(reporting_year_input_validation)
        constraints.update(layer_id_input_validation)
        constraints.update(user_id_input_validation)
        constraints.update(debug_input_validation)
        constraints.update(by_ids_input_validation)
        input_violations = assert_parameter_constraints(
            parameters,
            constraints,
        )

        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        # execute the request handler
        helpers.tprint(f"Input parameters are valid. Proceeding with report validation.")
        return handle_report_validation_request(
            int(parameters["reporting_year"]),
            int(parameters["layer_id"]),
            None if "user_id" not in parameters else int(parameters["user_id"]),
            None if "debug" not in parameters else bool(parameters["debug"]),
            None if "ids" not in parameters else [int(id.strip()) for id in parameters["ids"].strip(" ,").split(",")],
        )

    # report any errors to the requestor
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500


# ===================== REPORT PROCESSING =====================

@app_base.route("/report-processing", methods=["GET", "POST"])
def report_processing_endpoint():
    try:
        helpers.tprint("Report processing request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        constraints = {}
        constraints.update(reporting_year_input_validation)
        constraints.update(layer_id_input_validation)
        constraints.update(user_id_input_validation)
        constraints.update(debug_input_validation)
        constraints.update(by_ids_input_validation)
        input_violations = assert_parameter_constraints(
            parameters,
            constraints,
        )
        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        helpers.tprint(f"Input parameters are valid. Proceeding with report processing.")
        return handle_report_processing_request(
            int(parameters["reporting_year"]),
            int(parameters["layer_id"]),
            None if "user_id" not in parameters else int(parameters["user_id"]),
            None if "debug" not in parameters else bool(parameters["debug"]),
            None if "ids" not in parameters else [int(id.strip()) for id in parameters["ids"].strip(" ,").split(",")]
        )
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500


# ===================== LOAD ONLINE REPORT =====================

@app_base.route("/load-online-report", methods=["GET", "POST"])
def load_online_report_endpoint():
    try:
        helpers.tprint("Online report load request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        constraints = {}
        constraints.update(report_id_input_validation)
        constraints.update(report_type_id_input_validation)
        constraints.update(user_id_input_validation)
        constraints.update(gwp_input_validation)
        input_violations = assert_parameter_constraints(
            parameters,
            constraints,
        )
        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        helpers.tprint(f"Input parameters are valid. Proceeding with online report load.")
        return handle_load_online_report_request(
            int(parameters["report_id"]),
            int(parameters["report_type_id"]),
            int(parameters["user_id"]),
            None if "gwp" not in parameters else parameters["gwp"]
        )
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500
        
# ===================== PUBLICATION PROCESSING =====================

@app_base.route("/publication-processing", methods=["GET", "POST"])
def publication_processing_endpoint():
    try:
        helpers.tprint("Publication processing request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        constraints = {}
        constraints.update(publication_object_id_input_validation)
        constraints.update(publication_action_input_validation)
        constraints.update(user_id_input_validation)
        input_violations = assert_parameter_constraints(
            parameters,
            constraints,
        )
        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        helpers.tprint(f"Input parameters are valid. Proceeding with publication processing.")
        return handle_publication_processing_request(
            int(parameters["pub_object_id"]),
            db_constants.PUBLICATION_ACTIONS[parameters["action"].upper()],
            None if "user_id" not in parameters else int(parameters["user_id"]),
        )
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500


# ===================== PUBLICATION DOWNLOAD =====================

@app_base.route("/publication-download", methods=["GET", "POST"])
def publication_download_endpoint():
    try:
        helpers.tprint("Publication download request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        constraints = {}
        constraints.update(publication_object_ids_input_validation)
        constraints.update(user_id_input_validation)
        input_violations = assert_parameter_constraints(
            parameters,
            constraints,
        )
        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        helpers.tprint(f"Input parameters are valid. Proceeding with publication download.")
        return handle_publication_download_request(
            None if "pub_object_id" not in parameters else [int(id.strip()) for id in parameters["pub_object_id"].strip(" ,").split(",")],
            None if "user_id" not in parameters else int(parameters["user_id"])
        )
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500
    
# ===================== SIMPLE QUERY =====================

@app_base.route("/query_engine/simple-query", methods=["GET", "POST"])
def qe_simple_query_endpoint():
    try:
        helpers.tprint("Query engine simple query request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        constraints = {}
        constraints.update(simple_query_input_validation)
        constraints.update(reporting_year_input_validation)
        constraints.update(layer_id_input_validation)
        constraints.update(user_id_input_validation)
        input_violations = assert_parameter_constraints(
            parameters,
            constraints,
        )
        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        helpers.tprint(f"Input parameters are valid. Proceeding with simple query.")
        result = handle_simple_query_request(
            json.loads(parameters["query"].replace("(", "[").replace(")", "]")),
            int(parameters["reporting_year"]),
            int(parameters["layer_id"]),
            int(parameters["user_id"]),
        )
        return jsonify(result), 200
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500

        
# ===================== COMPLEX QUERY =====================

@app_base.route("/query_engine/complex-query", methods=["GET", "POST"])
def qe_complex_query_endpoint():
    try:
        helpers.tprint("Query engine complex query request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        constraints = {}
        constraints.update(complex_query_input_validation)
        constraints.update(reporting_year_input_validation)
        constraints.update(layer_id_input_validation)
        constraints.update(user_id_input_validation)
        input_violations = assert_parameter_constraints(
            parameters,
            constraints,
        )
        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        helpers.tprint(f"Input parameters are valid. Proceeding with complex query.")
        result = handle_complex_query_request(
            json.loads(parameters["query"].replace("(", "[").replace(")", "]")),
            int(parameters["reporting_year"]),
            int(parameters["layer_id"]),
            int(parameters["user_id"]),
        )
        return jsonify(result), 200
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500

# ===================== RECALCUATIONS REPORT =====================

@app_base.route("/qc_analytics/recalculations-report", methods=["GET", "POST"])
def recalculations_report_endpoint():
    try:
        helpers.tprint("Recalculations report request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        constraints = {}
        constraints.update(qca_object_handle_input_validation)
        constraints.update(user_id_input_validation)
        input_violations = assert_parameter_constraints(
            parameters,
            constraints,
        )
        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        helpers.tprint(f"Input parameters are valid. Proceeding with recalculations report.")
        print(parameters)
        result = handle_recalculations_report_request(
            parameters["qca_object_handle"],
            int(parameters["user_id"]),
        )
        return jsonify(result), 200
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500

# ===================== DOWNLOAD RECALCUATIONS EXCEL =====================

@app_base.route("/qc_analytics/download-recalculations-excel", methods=["GET", "POST"])
def download_recalculations_excel_endpoint():
    try:
        

        
        helpers.tprint("Recalculations excel download request received")
        parameters = get_parameters(request)
        helpers.tprint(f"The input parameters are {parameters}")
        constraints = {}
        constraints.update(qca_object_handle_input_validation)
        constraints.update(debug_input_validation)
        constraints.update(user_id_input_validation)
        input_violations = assert_parameter_constraints(
            parameters,
            constraints,
        )
        if input_violations:
            return jsonify({"input_violations": input_violations}), 400

        helpers.tprint(f"Input parameters are valid. Proceeding with recalculations report.")
        print(parameters)
        return handle_recalculations_excel_download_request(
            parameters["qca_object_handle"],
            None if "debug" not in parameters else bool(parameters["debug"]),
            int(parameters["user_id"])
        )
    except Exception:
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500


# ===================== MAIN =====================

app.register_blueprint(app_base)

# entry point for running __init__.py through the command line: `poetry run python __init__.py`
if __name__ == "__main__":
    freeze_support()
    from chalicelib.src.database.methods import *
    
    # check for command line arguments
    if "--local-prod-db" in sys.argv:
        globals.ENV = "LOCAL PRODUCTION"
        pgdb_connection = open_connection_to_postgres_db()

    #initialize db pool
    db_pool = DbPoolConfigurator()
    # run a database connection test
    test_connection_to_postgres_db()
    success = db_pool.test_connection()
    if success:
        print("Connection pool is set up and working.")
    else:
        print("Connection pool setup failed or connections are not working.")

    if "--no-server" not in sys.argv:
        # start up the API web service
        serve(app, host="0.0.0.0", port=5000, url_scheme="")
    else: # execute some test code here
        # handle_source_file_archiving_request(2024, 1, 4, True, [723])

        from chalicelib.src.AWS.S3.methods import get_global_s3_session
        s3_session = get_global_s3_session()
        print(s3_session.get_s3_bucket_contents())
        # with open('./tests/local/qc_analytics/20250409171205/metadata.json', "w") as file: 
        #     file.write(s3_session.download_file_from_s3_bucket('analytics/qc/20250409171205/metadata.json'))  

        # with open('./tests/local/qc_analytics/20250409171205/raw_results.json', "w") as file: 
        #     file.write(s3_session.download_file_from_s3_bucket('analytics/qc/20250409171205/recalculations/raw_results.json'))

        # with open('./tests/local/qc_analytics/20250409171205/aggregate_results.json', "w") as file: 
        #     file.write(s3_session.download_file_from_s3_bucket('analytics/qc/20250409171205/recalculations/aggregate_results.json'))    


        # from chalicelib.src.source_files.models.SourceFile import SourceFile
        # import chalicelib.src.source_files.jobs.crt_extraction.methods as crt_methods
        # import chalicelib.src.source_files.jobs.crt_extraction.queries as crt_extraction_queries

        # source_files_info = [
        #     # (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/2.B.9.b-g_Fluorochemical_Production_other_than_HCFC-22_RY2023_20241207_PR_EH_Edit.xlsx', 229, 1083),
        #     # (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/2.G.OtherProductUsesSF6PFCs_90-23_ER_10-2-2024.xlsx', 192, 1004),
        #     # (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/5.3 Rice Cultivation_1990-2023_ER.xlsx', 191, 917),
        #     # (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/5.5 Liming_1990-2023_ER.xlsx', 221, 999),
        #     # (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/Agricultural Soil Management_1990-2023_PR.xlsx', 210, 1021),
        #     # (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/CO2 from Fossil Fuel Combustion_1990-2023_10_10_2024_ER.xlsx', 223, 1010),
        #     # (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/Cropland-Grassland_1990-2023_ER.xlsx', 200, 980),
        #     # (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/Electronics Inventory_1990-2023_PR_12-12-24-(correctedInvDB).xlsx', 189, 1046),
        #     # (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/IndCalc_Metals_1990-2023_KO.xlsx', 190, 985),
        #     # (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/ODS Substitutes BY23_AR5_ER.xlsx', 218, 1001),
        #     # (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/PetroleumSystems_90-23_PR.xlsx', 227, 1036),
        #     (2025, 1, 'C:/Dev/Projects/invdb-py/tests/local/crt_extraction/SRS-C-N2O_1990-2023_ER.xlsx', 206, 938)
        # ]

        # source_files = []
        # for index, info in enumerate(source_files_info):
        #     with open(info[2], 'rb') as test_file:
        #         test_file_content = test_file.read()
        #     source_files.append(SourceFile(info[3], info[4], test_file_content, info[0] - 2, info[0], 4, True, layer_id=info[1]))

        # results = crt_methods.extract_crt_data_from_source_files(source_files)
        # for result in results:
        #     crt_extraction_queries.update_facts_archive_table(result["facts"])
        #     crt_extraction_queries.update_crt_key_table(result["keys"])

        # print("Done!")

        # import chalicelib.src.unfccc.helpers as unfccc_helpers
        # unfccc_helpers.export_crt_json_from_database((1083,1004,917,999,1021,1010,980,1046,985,1001,1036,938), "./tests/local/crt_export/results.json", stats_output_filename="./tests/local/crt_export/results_stats.txt")
        # print("Done!")

        # import chalicelib.src.unfccc.helpers as unfccc_helpers
        # unfccc_helpers.export_crt_json_from_database(
        #     # (
        #     #     1083,
        #     #     1004
        #     #     917,
        #     #     999,
        #     #     1021,
        #     #     1010,
        #     #     980,
        #     #     1046,
        #     #     985,
        #     #     1001,
        #     #     1036,
        #     #     938
        #     12,1,# ), 
        #     "./tests/local/crt_export/results.json", 
        #     'CRT', 
        #     stats_output_filename="./tests/local/crt_export/results_stats.txt")