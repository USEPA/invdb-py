import chalicelib.src.database.constants as db_constants
import psycopg2
import chalicelib.src.general.globals as invdb_globals
import chalicelib.src.general.helpers as helpers
from chalicelib.src.database.dbPoolConfigurator import DbPoolConfigurator
import chalicelib.src.database.methods as db_methods
import time

def open_connection_to_postgres_db():
    host = (
        invdb_globals.PRODUCTION_DB_SERVER if invdb_globals.ENV == "PRODUCTION" else invdb_globals.DEVELOPMENT_DB_SERVER if invdb_globals.ENV == "DEVELOPMENT" else "localhost"
    )
    connection = psycopg2.connect(
        database="psql",
        user="invdb_usr",
        password="invdb_usr" if invdb_globals.ENV == "DEVELOPMENT" else "ggds2017qtr2",
        host=host,
        port="5432",
    )
    return connection 


pgdb_connection = open_connection_to_postgres_db()


def get_pgdb_connection():
    global pgdb_connection
    return pgdb_connection


def close_connection_to_postgres_db(connection=pgdb_connection):
    connection.close()


def test_connection_to_postgres_db():
    """gets information from the current postgres database connection and makes a test query"""
    cursor = pgdb_connection.cursor()
    cursor.execute("select version()")
    version_data = cursor.fetchone()
    helpers.tprint(
        f"Database connection established with: {pgdb_connection.get_dsn_parameters()['host']} {'(the production server connection via SSH tunnel)' if pgdb_connection.get_dsn_parameters()['host'] == 'localhost' else ''}"
    )
    helpers.tprint(f"Version information according to remote server connection: {version_data}")


# common code that can be used for all get queries
db_pool = DbPoolConfigurator()

def get_query_results(query, *vars):
    conn = db_pool.get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, *vars)
            results = cursor.fetchall()

        if len(results) == 0:
            return None
        return results
    except Exception as e:
        print(
            f"""An error occurred while executing the a query. See details below: 
            QUERY:      {query}
            PARAMETERS: {vars}
            ERROR:      {e}""")
    finally:
        # Always return the connection to the pool
        if conn:
            db_pool.return_connection(conn)


def perform_query(query, *vars):
    '''does the same as get_query_results() but expects no return value. 
       will commit in case changes were made to the DB.'''
    conn = db_pool.get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, *vars)
            conn.commit()
    except Exception as e:
        print(
            f"""An error occurred while executing the a query. See details below: 
            QUERY:      {query}
            PARAMETERS: {vars}
            ERROR:      {e}""")
    finally:
        if conn:
            db_pool.return_connection(conn)


def perform_query_function(function_name: str, *args) -> None:
    """Same as get_query_results but doesn't give a return value"""
    conn = db_pool.get_connection()
    try:
        with conn.cursor() as cursor:
            query = f"SELECT {function_name}{helpers.get_sql_list_str(list(args))}"
            cursor.execute(query)
            conn.commit()
    except Exception as e:
        argument_string = ' '.join(map(str, args))
        print(f"An error occurred while attempting to run the function {function_name} with the following parameters ({argument_string}): {e}")
        raise
    finally:
        # Always return the connection to the pool
        if conn:
            db_pool.return_connection(conn)

def perform_query_update(query, *vars) -> None:   
    conn = db_pool.get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, vars)
            conn.commit()
            print("Number of rows inserted: ", cursor.rowcount)
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        # Always return the connection to the pool
        if conn:
            db_pool.return_connection(conn)

def get_time_series_with_ids_by_rptyr(reporting_year: int) -> [int]:
    """e.g. input 2024
    output {1: 1990, 2: 1991, ...]"""
    max_time_series = fetch_max_time_series_by_reporting_year(reporting_year)
    query = f"""
        SELECT year, year_id
        FROM {db_constants.DB_TABLES["DIM_TIME_SERIES"]}
        where year <= {max_time_series}"""
    return db_methods.get_query_results(query)


def fetch_dim_table_validation_values(case_insensitive=False) -> {str: list}:
    """returns a dictionary:
    [key]: the name of the data column according to the source file header
    [value]: list of valid values for the data column"""
    validation_values = {}
    for dim_table in db_constants.DIM_TABLE_VALUE_MAPPINGS:
        cursor = pgdb_connection.cursor()
        cursor.execute(f"""SELECT DISTINCT {dim_table[2]}::TEXT FROM {dim_table[1]}""")
        table_values = cursor.fetchall()
        if len(table_values[0]) == 1: # unpack single-element tuples, and convert to lower case if insensitive
            table_values = [(element[0].lower() if case_insensitive else element[0]) for element in table_values]
        elif case_insensitive: # convert multi-element tuples to lower case if insensitive
            table_values = [tuple(map(lambda x: x.lower() if x is not None else x, value)) for value in table_values]
        validation_values[dim_table[0]] = table_values
    return validation_values


def fetch_dim_table_id_to_name_mappings() ->  {str: {int: str}}:
    """returns information needed to convert foreign keys (id values) into their respective string name values (e.g. ghg_id = 1 --> ghg_longname = 'Carbon Dioxide')"""
    validation_values = {}
    for dim_table in db_constants.DIM_TABLE_VALUE_MAPPINGS:
        cursor = pgdb_connection.cursor()
        cursor.execute(f"""SELECT DISTINCT {dim_table[3]}, {dim_table[2]}  FROM {dim_table[1]}""")
        table_mappings = cursor.fetchall()
        mappings_obj = {mapping[0]: mapping[1] for mapping in table_mappings}
        if dim_table[0] in ["fuel", "category"]: # allow null/empty-string values for select tables (optional fields)
            mappings_obj.update({"": "", None: ""})
        validation_values[dim_table[0]] = mappings_obj
    return validation_values


def fetch_query_formula_name_mappings(by_query_formula_id:bool=False, include_parameters=False) -> dict:
    """returns the view_name and formula_prefix for all query formulas in the report"""
    cursor = pgdb_connection.cursor()
    cursor.execute(f"""SELECT formula_prefix, view_name, query_formula_id {'' if not include_parameters else ', parameters'}
                    FROM {db_constants.DB_TABLES["DIM_QUERY_FORMULA"]}
                    WHERE formula_type = 'emission'""")
    results = cursor.fetchall()
    # return as a dict where the formula prefix is the key and parameter list are the value
    mappings = {result[2] if by_query_formula_id else result[0]: (result[1] if not include_parameters else (result[1], result[3])) for result in results}
    return mappings


def fetch_dim_state_list() -> [str]:
    """returns a list of valid state values according to the dim_state table of the database"""
    cursor = pgdb_connection.cursor()
    cursor.execute(f"""SELECT state
                     FROM {db_constants.DB_TABLES["DIM_STATE"]}""")
    return [state[0] for state in cursor.fetchall()]


def fetch_year_id(reporting_year: int) -> int:
    """returns the pub_year_id corresponding to the input reporting_year"""
    return (reporting_year + 1) - db_constants.EARLIEST_REPORTING_YEAR 


def fetch_reporting_year(pub_year_id: int) -> int:
    """returns the reporting_year corresponding to the input pub_year_id"""
    return (pub_year_id - 1) + db_constants.EARLIEST_PUBLICATION_YEAR


def fetch_pub_year_id(reporting_year: int) -> int:
    """returns the pub_year_id corresponding to the input reporting_year"""
    return (reporting_year + 1) - db_constants.EARLIEST_PUBLICATION_YEAR


def fetch_max_time_series_by_reporting_year(reporting_year: int) -> int:
    """returns integer of the latest reporting year according to the max
    "max_time_series" value in the dim_publication_year table of the postgres DB."""
    cursor = pgdb_connection.cursor()
    cursor.execute(
        f"SELECT max_time_series FROM {db_constants.DB_TABLES['DIM_PUBLICATION_YEAR']} WHERE pub_year = {reporting_year}"
    )
    results = cursor.fetchone()[0]
    max_time_series = results
    return max_time_series


def get_time_series_by_reporting_year(reporting_year_id: int) -> [int]:
    """e.g. input 2024
            outputs [1990, 1991, ..., 2021, 2022] """
    max_time_series = fetch_max_time_series_by_reporting_year(reporting_year_id)
    return range(db_constants.EARLIEST_REPORTING_YEAR, max_time_series + 1)


def get_time_series_by_pub_year_id(pub_year_id: int) -> [int]:
    """e.g. input 11
            outputs [1990, 1991, ..., 2021, 2022] """
    max_time_series = fetch_max_time_series_by_reporting_year(fetch_reporting_year(pub_year_id))
    return range(db_constants.EARLIEST_REPORTING_YEAR, max_time_series + 1)


def fetch_redacted_ghg_chemicals(ghg_name_select="ghg_longname"):
    """returns the ghg_longname values for all chemicals listed in ggds_invdb.dim_redacted_ghg"""
    query = f"""SELECT dg.{ghg_name_select}
                FROM {db_constants.DB_TABLES['DIM_REDACTED_GHG']} drg
                    JOIN {db_constants.DB_TABLES['DIM_GHG']} dg ON drg.ghg_id = dg.ghg_id"""
    return [result[0] for result in get_query_results(query)]


def wait_on_rollup_tables_refresh(reporting_year: int, layer_id: int):
    start_time = time.time()
    while True:
        # Query the specific cell
        pub_year_id = db_methods.fetch_pub_year_id(reporting_year)
        query = f"""SELECT refresh_status 
                    FROM {db_constants.DB_TABLES["REFRESH_STATUS_ROLLUP_TABLE"]} 
                    WHERE pub_year_id = {pub_year_id} 
                            AND layer_id = {layer_id} 
                    LIMIT 1;"""
        result = db_methods.get_query_results(query)

        if result and result[0][0] == 'In Progress': # wait condition not met
            helpers.tprint(f"Still waiting on rollup tables to refresh...")
        elif result and result[0][0] == 'Error': # wait condition not met
            helpers.tprint(f"WARNING: Rollup tables for reporting year: {reporting_year} and layer_id: {layer_id} are in an 'Error' state. Please upload a data file for this year and layer to refresh the rollup tables.")
            break
        else: # wait condition met (no entry or entry with refresh_status 'Complete')
            break


        # Check if the timeout has been reached
        if time.time() - start_time > db_constants.ROLLUP_TABLES_REFRESH_TIMEOUT:
            helpers.tprint("WARNING: Timeout reached waiting for rollup tables to refresh. Data may appear outdated.")
            break

        # Wait before checking again
        time.sleep(db_constants.ROLLUP_TABLES_REFRESH_CHECK_INTERVAL)  

    return # if this line is reached, the wait is over


def get_ghg_to_gwp_mappings_by_year(reporting_year: int, ghg_column_select: str="ghg_longname"):
    query = f"""SELECT gwp_column 
                FROM {db_constants.DB_TABLES["DIM_PUBLICATION_YEAR"]} 
                WHERE pub_year = {reporting_year} 
                LIMIT 1;"""
    gwp_column = db_methods.get_query_results(query)[0][0]

    query = f"""SELECT {ghg_column_select}, {gwp_column} 
                FROM {db_constants.DB_TABLES["DIM_GHG"]}"""
    gwp_mappings = db_methods.get_query_results(query)
    dict_mapping = helpers.tuples_to_dict(gwp_mappings)
    return dict_mapping


def get_ghg_to_ghg_category_mappings(ghg_column_select: str="ghg_longname"):
    query = f"""SELECT ghg.{ghg_column_select}, ghg_cat.ghg_category_name 
                FROM {db_constants.DB_TABLES["DIM_GHG"]} ghg
                JOIN {db_constants.DB_TABLES["DIM_GHG_CATEGORY"]} ghg_cat ON ghg.ghg_category_id = ghg_cat.ghg_category_id"""
    ghg_category_pairs = db_methods.get_query_results(query)
    ghg_category_mapping = helpers.tuples_to_dict(ghg_category_pairs)
    return ghg_category_mapping