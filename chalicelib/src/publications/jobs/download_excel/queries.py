import chalicelib.src.database.methods as db_methods
import chalicelib.src.database.constants as db_constants
import chalicelib.src.publications.constants as pub_constants
import chalicelib.src.general.helpers as helpers

pgdb_connection = db_methods.get_pgdb_connection()


def fetch_reporting_year_and_layer_id(pub_object_ids: [int]):
    """pull the redacted publication data, ID, and table name for the publications selected in the input list of integers."""
    cursor = pgdb_connection.cursor()
    cursor.execute(f"""SELECT pub_ver.pub_year AS reporting_year, pub_ver.layer_id
                       FROM {db_constants.DB_TABLES["PUBLICATION_OBJECT"]} pub_obj
                            JOIN {db_constants.DB_TABLES["DIM_PUBLICATION"]} dim_pub ON pub_obj.pub_id = dim_pub.publication_id
                            JOIN {db_constants.DB_TABLES["PUBLICATION_VERSION"]} pub_ver ON pub_obj.pub_version_id = pub_ver.pub_version_id
                       WHERE pub_obj.pub_object_id IN {helpers.get_sql_list_str(pub_object_ids)}""")

    results = cursor.fetchall()

    if len(results) == 0: 
        return None, None

    return results[0] # should all be the same when coming from the UI, otherwise the first pair can represent the values for the job.


def fetch_refined_data_products(pub_object_ids: [int]):
    """pull the redacted publication data, ID, and table name for the publications selected in the input list of integers."""
    cursor = pgdb_connection.cursor()
    cursor.execute(f"""SELECT pub_object_id, refined_tablename, refined_data
                       FROM {db_constants.DB_TABLES["PUBLICATION_OBJECT"]} pub_obj
                            JOIN {db_constants.DB_TABLES["DIM_PUBLICATION"]} dim_pub ON pub_obj.pub_id = dim_pub.publication_id
                            JOIN {db_constants.DB_TABLES["PUBLICATION_VERSION"]} pub_ver ON pub_obj.pub_version_id = pub_ver.pub_version_id
                       WHERE pub_obj.pub_object_id IN {helpers.get_sql_list_str(pub_object_ids)}""")
    
    results = cursor.fetchall()

    if len(results) == 0: 
        return None 

    return [{"pub_object_id": result[0], 
             "refined_tablename": result[1], 
             "refined_data": result[2]} for result in results]