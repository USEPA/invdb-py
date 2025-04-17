import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.source_files.jobs.crt_extraction.constants as crt_constants 
import chalicelib.src.general.helpers as helpers
from chalicelib.src.source_files.models.SourceFile import SourceFile
from chalicelib.src.source_files.models.QCLoadTarget import QCLoadTarget
import chalicelib.src.general.qc_constants as qc_constants
import chalicelib.src.general.globals as globals
from psycopg2 import sql
import json
import os


def update_crt_key_table(crt_key_rows: list) -> None:
    """insert new crt keys into the crt_key table, skip inserting any crt_keys that are already present"""

    if len(crt_key_rows) == 0:
        helpers.tprint("No CRT data for crt_key, skipping")
        return

    #  filter out duplicates in the input
    crt_key_rows = list(set(crt_key_rows))
    
    # for row in [row for row in crt_key_rows if row[0] == '29b65504-be53-1b12-d28e-874f618257c5']:
    #     print(row)

    # fetch all emissions_uids from the database so we can check for duplicates in input
    results = db_methods.get_query_results(
        f"""SELECT crt_uid FROM {db_constants.DB_TABLES['CRT_KEY']}
            WHERE crt_uid IS NOT NULL"""
    )
    if results:
        preexisting_emissions_uids = {
            row[0].replace("-", ""): None for row in results
        }  # place the uids in a dict for fast retrieval time
        
        # remove any emission keys from the source file input that are already present in the database
        i = 0
        while i < len(crt_key_rows):
            if crt_key_rows[i][1] in preexisting_emissions_uids:
                del crt_key_rows[i]
                i -= 1
            i += 1

    # if there are no new emissions keys, exit
    if len(crt_key_rows) == 0:
        return

    # build the 'INSERT' SQL command from the emissions_key_data
    insert_rows_string = ""
    for row in crt_key_rows: # reformat rows as needed to be SQL-compliant
        row = list(row)
        row[1] = f"'{row[1]}'" if row[1] else 'NULL'
        row[2] = f"'{row[2]}'" if row[2] else 'NULL'
        row[3] = row[3] if row[3] else 'NULL'
        row[4] = f"'{row[4]}'" if row[4] else 'NULL'
        row[5] = f"'{row[5]}'" if row[5] else 'NULL'
        row[6] = f"'{row[6]}'" if row[6] else 'NULL'
        row[7] = f"'{row[7]}'" if row[7] else 'NULL'    
        row[8] = f"'{row[8]}'" if row[8] else 'NULL'
        row[9] = f"'{row[9]}'" if row[9] else 'NULL'
        row[10] = f"'{row[10]}'" if row[10] else 'NULL'
        row[11] = f"'{row[11]}'" if row[11] else 'NULL'
        row[12] = "'" + str(row[12]).replace("'", "''") + "'" if row[12] else 'NULL'
        row[13] = "'" + str(row[13]).replace("'", "''") + "'" if row[13] else 'NULL'
        row[14] = row[14] if row[14] else 'NULL'
        insert_rows_string += f"('{row[0]}'::uuid, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]}, {row[8]}, {row[9]}, {row[10]}, {row[11]}, {row[12]}, {row[13]}, {row[14]}), "
    insert_rows_string = insert_rows_string[:-2]
    
    db_methods.perform_query(
        f"""INSERT INTO {db_constants.DB_TABLES["CRT_KEY"]} (crt_uid, unfccc_uid, key_type, step, header_1, header_2, header_3, header_4, crt_input, column_c_info, column_d_info, column_e_info, ne_ie_comment, ie_reported_where, source_file_id)
            VALUES {insert_rows_string}
            ON CONFLICT (crt_uid) DO NOTHING;
        """
    )


def update_facts_archive_table(crt_fact_rows: list) -> None:
    """removes pre-existing facts_archive records for input source files and adds new facts_archive records gathered by the current source file load request"""

    if len(crt_fact_rows) == 0:
        helpers.tprint("No CRT data for facts_archive, skipping")
        return

    layer_id = crt_fact_rows[0][2]
    source_file_id = crt_fact_rows[0][3]

    # delete the existing fact_archive data that corresponds with the incoming data (same crt_uid, reporting_year, and layer_id)
    # calls a query function from the database to handle this
    db_methods.perform_query(
        f"""SELECT * FROM {db_constants.DB_QUERY_FUNCTIONS["DELETE_CRT_FACTS_ARCHIVE"]}(ARRAY[%s])""",
        (source_file_id,)
    )

    # making sure to remove duplicates before updating
    # unique_hashes = tuple(set([fact[1] for fact in crt_fact_rows]))
    # if len(unique_hashes) > 0:
    #     unique_hashes_placeholder = ", ".join(['%s'] * len(unique_hashes))
    #     db_methods.perform_query(f"DELETE FROM {db_constants.DB_TABLES['FACTS_ARCHIVE']} WHERE data_type_id = %s AND key_id IN ({unique_hashes_placeholder})", (crt_constants.CRT_DATA_TYPE_ID,) + unique_hashes)
    #     db_methods.perform_query(f"DELETE FROM {db_constants.DB_TABLES['CRT_KEY']} WHERE crt_uid IN ({unique_hashes_placeholder})", unique_hashes)

    # insert the new facts_archive data
    values = []
    for fact in crt_fact_rows:
        data_type_id = fact[0]
        key_id = fact[1]
        pub_year_id = db_methods.fetch_pub_year_id(fact[3]) # doesn't actually call DB
        attachment_id = fact[5]

        for value, year_id in zip(fact[6:], range(1, len(fact[6:]) + 1)):
            if type(value) == str and "'" in value:
                value = value.replace("'", "''")
            values.append(f"""({data_type_id}, '{key_id}'::uuid, {layer_id}, {pub_year_id}, {year_id}, {f"'{value}'" if value is not None else 'NULL'}, {attachment_id})""")

    values_string = ', '.join(values)

    db_methods.perform_query(
        f"""INSERT INTO {db_constants.DB_TABLES['FACTS_ARCHIVE']} VALUES {values_string}"""
    )