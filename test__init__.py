# run by giving the console command 'pytest'

import chalicelib.src.general.globals as globals # loads the environment variables first and foremost
from chalicelib.src.source_files.jobs.validation.methods import handle_source_file_validation_request
from chalicelib.src.source_files.jobs.load.methods import handle_source_file_archiving_request
from chalicelib.src.reports.jobs.validation.methods import handle_report_validation_request
from chalicelib.src.reports.jobs.processing.methods import handle_report_processing_request
from chalicelib.src.reports.jobs.load_online_report.methods import handle_load_online_report_request
from chalicelib.src.publications.jobs.handle_action.methods import handle_publication_processing_request
from chalicelib.src.publications.jobs.download_excel.methods import handle_publication_download_request
from chalicelib.src.query_engine.jobs.execute_simple_query.methods import handle_simple_query_request
from chalicelib.src.query_engine.jobs.execute_complex_query.methods import handle_complex_query_request
from chalicelib.src.qc_analytics.jobs.recalculations_report.methods import handle_recalculations_report_request
from chalicelib.src.database.dbPoolConfigurator import DbPoolConfigurator
import chalicelib.src.general.qc_constants as qc_constants
import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.general.helpers as helpers 
from multiprocessing import freeze_support
from datetime import datetime
import traceback
import json
import sys
import os


@pytest.fixture(scope="module")
def module_fixture():
    # setup code 
    freeze_support()
    from chalicelib.src.database.methods import *
    
    # check for command line arguments
    if "--local-prod-db" in sys.argv:
        globals.ENV = "LOCAL PRODUCTION"
        pgdb_connection = open_connection_to_postgres_db()

    # initialize DB pool
    db_pool = DbPoolConfigurator()
    # run a database connection test
    test_connection_to_postgres_db()
    success = db_pool.test_connection()
    if success:
        print("Connection pool is set up and working.")
    else:
        print("Connection pool setup failed or connections are not working.")

    # instantiate a test source file
    # instantiate a test excel report 
    # instantiate a test publication version
    get_query_results('INSERT INTO ')

    yield # the ID details of the test data objects

    # teardown code
    
    # delete the test source file
    # delete the test excel report 
    # delete the test publication version


def test_source_file_validation(module_fixture):
    pass


def test_source_file_archiving(module_fixture):
    pass


def test_report_validation(module_fixture):
    pass


def test_report_processing(module_fixture):
    pass


def test_load_online_report(module_fixture):
    pass


def test_publication_processing(module_fixture):
    pass


def test_publication_download(module_fixture):
    pass


def test_simple_query(module_fixture):
    pass


def test_complex_query(module_fixture):
    pass


def test_recalculations_report(module_fixture):
    pass