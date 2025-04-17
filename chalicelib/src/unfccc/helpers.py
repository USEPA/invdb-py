from chalicelib.src.database.methods import *
import chalicelib.src.general.helpers as helpers
import pandas as pd
import json
import re


notation_keys = {
    "" :   {"type": "Null",         "value": 0}, 
    "C" :  {"type": "NK",           "value": 1},
    "CR":  {"type": "NK",           "value": 2},
    "CS":  {"type": "drop_down",    "value": 4},
    "D":   {"type": "drop_down",    "value": 8},
    "GCV": {"type": "drop_down",    "value": 16},
    "IE":  {"type": "NK",           "value": 32},
    "IO":  {"type": "NK",           "value": 64},
    "M":   {"type": "drop_down",    "value": 128},
    "NA":  {"type": "NK",           "value": 256},
    "NCV": {"type": "drop_down",    "value": 512},
    "NE":  {"type": "NK",           "value": 1024},
    "NO":  {"type": "drop_down",    "value": 2048},
    # reserved key here---------------------------
    "OTH": {"type": "drop_down",    "value": 8192},
    "PS":  {"type": "drop_down",    "value": 16384},
    "R":   {"type": "NK",           "value": 32768},
    "RA":  {"type": "drop_down",    "value": 65536},
    "T1":  {"type": "drop_down",    "value": 131072},
    "T1a": {"type": "drop_down",    "value": 262144},
    "T1b": {"type": "drop_down",    "value": 524288},
    "T1c": {"type": "drop_down",    "value": 1048576},
    "T2":  {"type": "drop_down",    "value": 2097152},
    "T3":  {"type": "drop_down",    "value": 4194304},
    "FX":  {"type": "NK",           "value": 8388608},
    # reserved key here------------------------------
    # reserved key here------------------------------
}


def extract_common_variables(input_filename: str) -> list:
    '''extracts the list of uids from the common variable excel file'''
    df = pd.read_excel(input_filename, engine='openpyxl')
    min_column_index = ord('B') - ord('A')
    max_column_index = ord('C') - ord('A')
    column_data = df.iloc[:, min_column_index:max_column_index+1]
    column_list = [(var_info[0], var_info[1].lower()) for var_info in column_data.itertuples(index=False, name=None)]
    return column_list


def truncate_decimal(value, decimals=2):
    '''reduces the number of trailing decimals within a float value'''
    factor = 10 ** decimals
    return int(value * factor) / factor


def get_dropdown_selection_sum(selections: list) -> int:
    '''calculates the binary number generated from a list of selections from the standard dropdown list'''
    selection_sum = 0
    for selection in selections: 
        selection_sum += notation_keys[selection]["value"]
    return selection_sum


def export_crt_json_from_database(pub_year_id: int, layer_id: int, outfile_name: str="output.json", data_type: "CRT" or "emissions"="CRT", attachment_ids: tuple[int]=None, stats_output_filename: str=None):
    cursor = pgdb_connection.cursor()
    # define the query
    if data_type == "CRT":
        query = f"""SELECT  LOWER(ck.unfccc_uid),
                            fa.value,
                            fa.year_id,
                            ck.ne_ie_comment, 
                            ck.ie_reported_where
			FROM    ggds_invdb.facts_archive fa 
					    join ggds_invdb.crt_key ck on fa.key_id::text = ck.crt_uid::text 
			WHERE   fa.data_type_id = 6 AND
                    fa.value IS NOT NULL
                    {f' AND fa.attachment_id IN {attachment_ids}' if attachment_ids else f'AND fa.pub_year_id = {pub_year_id} AND fa.layer_id = {layer_id}'}
            ORDER BY ck.crt_uid, year_id"""
    elif data_type == "emissions":
        query = f"""SELECT   ek.id,
                             fa.value,
                             fa.year_id, 
                             NULL AS ne_ie_comment,
                             NULL AS ie_reported_where
                FROM    ggds_invdb.facts_archive fa 
                            join ggds_invdb.emissions_key ek on fa.key_id::text = ek.emissions_uid::text 
                WHERE   fa.data_type_id = 1
                        {f' AND fa.attachment_id IN {attachment_ids}' if attachment_ids else f'AND fa.pub_year_id = {pub_year_id} AND fa.layer_id = {layer_id}'}
            ORDER BY ek.id, year_id"""
    else: 
        print(f"data_type '{data_type}' not recognized")
        return
    
    # execute the query
    print("Fetching the data from the facts_archive table...")
    cursor.execute(query)
    results = cursor.fetchall()
    
    # some grooming of the results:
    results = helpers.remove_duplicates_from_list(results) 
    results = [list(result) for result in results]
    print("after grooming, there are ", len(results), "results.")
    # initialize the output json with the pre-determined version and country-specific data
    print("Constructing export template...")
    output = {}
    with open('tests/static_data/CRT_export/2024v1.1_exported_version_and_country_specific_data v1.30.json', 'r', encoding='utf-8') as base_file:
        output = json.load(base_file)
    output["data"] = {"values": []}

    # populate the value section using the results from the query
    print("Populating the data section...")
    for year_id in sorted(list(set([result[2] for result in results]))):
        year_values = []
        prev_result = None
        for result in [entry for entry in results if entry[2] == year_id]:
            # construct any comments for the row
            comments = []
            if result[3]: 
                comments.append({
                    "comment": result[3],
                    "type": "NK_explanation"
                })
            if result[4]:
                comments.append({
                    "comment": f"IE reported at: {result[4]}",
                    "type": "Allocation_by_Party"
                })

            # construct the value field based on its type 
            # for numeric quantities
            if helpers.is_numeric_string(result[1]):
                current_value = {
                    "variable_uid": result[0].lower() if isinstance(result[0], str) else result[0],
                    "value": {
                        "type": "number", 
                        "value": float(result[1])
                    },
                    "agg_disabled": False,
                }
            
            # for INVDB annotations and dropdowns from the notation_keys object
            elif any([(result.strip() in notation_keys) for result in result[1].split(",")]) : 
                choices = [result.strip() for result in result[1].split(",")]
                if len(choices) > 1:
                    value_type = "drop_down"
                    value_value = get_dropdown_selection_sum(choices)
                else: 
                    value_type = notation_keys[choices[0]]["type"]
                    value_value = notation_keys[choices[0]]["value"]
                current_value = {
                    "variable_uid": result[0].lower() if isinstance(result[0], str) else result[0],
                    "value": {
                        "type": value_type,
                        "value": value_value
                    },
                    "agg_disabled": False,
                }
            
            # for text values
            else: 
                current_value = {
                    "variable_uid": result[0].lower() if isinstance(result[0], str) else result[0],
                    "value": {
                        "type": "text",
                        "value": result[1].strip(),
                    },
                    "agg_disabled": False,
                }

            # remove optional and default attributes to reduce size
            if len(comments) > 0: # comments are optional and defaults to []
                current_value["comments"] = comments
            if not current_value["agg_disabled"]: # agg_disabled is optional and defaults to false
                del current_value["agg_disabled"] 
            year_values.append(current_value)

        if len(year_values) > 0:
            output["data"]["values"].append(
                {"inventory_year": str(1989 + year_id), "values": year_values}
            )

    with open(outfile_name, "w") as file:
        file.write(json.dumps(output, indent=3))
    
    
    # print some statistics about the matching of common variables
    if stats_output_filename:
        print("Running statistical analysis:")
        md5_pattern = r'^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$'
        print("Loading common variables...")
        ipcc_common_variables_info = extract_common_variables('tests/static_data/CRT_export/CRF-CRT_CommonVariables 7-2024.xlsx')
        ipcc_common_variable_names = [info[0] for info in ipcc_common_variables_info] 
        ipcc_common_variable_uids = [info[1] for info in ipcc_common_variables_info]
        
        country_specific_uid_pattern = r'^[a-fA-F0-9]{24}$'
        database_country_specific_uids = {result[0] for result in results if re.match(country_specific_uid_pattern, result[0])}
        country_specific_variable_names = {var["name"] for var in output["country_specific_data"]["variables"]}
        country_specific_variable_uids = {var["uid"] for var in output["country_specific_data"]["variables"]}
        
        print("Calulating statistics...")
        database_ipcc_uids = {result[0] for result in results if re.match(md5_pattern, result[0])}
        matches = {uid for uid in database_ipcc_uids if uid in ipcc_common_variable_uids}
        non_matches = {uid for uid in database_ipcc_uids if uid not in matches}
        percent_match = truncate_decimal(len(matches) / len(database_ipcc_uids) * 100)
        country_specific_matches = {uid for uid in database_country_specific_uids if uid in country_specific_variable_uids}
        country_specific_non_matches = {uid for uid in database_country_specific_uids if uid not in country_specific_variable_uids}
        country_specific_percent_match = truncate_decimal(len(country_specific_matches) / len(database_country_specific_uids) * 100)
        print("Outputing to stats file...")
        stats_msg = "\n".join([
            f"# of ipcc variables in facts_archive data: {len(database_ipcc_uids)}",
            f"# of ipcc common variables provided: {len(ipcc_common_variable_uids)}",
            f"# of matches: {len(matches)}",
            f"# of non-matches: {len(non_matches)}",
            f"match rate: {percent_match}%",
            
            f"# of country-specific variables in facts_archive data: {len(database_country_specific_uids)}",
            f"# of country-specific metadata variables: {len(country_specific_variable_names)}",
            f"# of country-specific matches: {len(country_specific_matches)}",
            f"# of country-specific non-matches: {len(country_specific_non_matches)}",
            f"match rate: {country_specific_percent_match}%",

            f"list of IPCC matches (and their row numbers within the common variables spreadsheet)",
            f"ROW NUMBER\t\t\tUID\t\t\t\tVariable name",
            f"----------\t\t\t---\t\t\t\t-------------",
        ])
        max_row_length = len(str(len(ipcc_common_variable_uids) + 1))
        
        for row_number, common_variable_uid, common_variable_name in zip(range(2, len(ipcc_common_variable_uids) + 2), ipcc_common_variable_uids, ipcc_common_variable_names): #list starts on row 2
            if common_variable_uid in matches:
                padded_row_number = str(row_number).rjust(max_row_length)
                stats_msg += f"\n{padded_row_number}\t\t{common_variable_uid.upper()}\t\t{common_variable_name}"
        
        stats_msg += "\n".join([
            "", 
            f"list of non-matches",
            f"UID",
            f"---",
        ])
        for non_matching_database_uid in enumerate(non_matches, start=2): #list starts on row 2
            stats_msg += f"\n{non_matching_database_uid}"
            
        import random
        # find matching country specific data variable names with data enteries
        data_section_numeric_data_keys = {var["variable_uid"] for var in output["data"]["values"][18]["values"]}
        
        for common_variable_name, common_variable_uid in zip(ipcc_common_variable_names, ipcc_common_variable_uids): 
            if common_variable_uid in data_section_numeric_data_keys:
                if common_variable_name in country_specific_variable_names:
                    stats_msg += f"There should be data in {common_variable_name}\n"
        
        encoded_msg = stats_msg.encode('cp1252', errors='ignore').decode('cp1252')
        with open(stats_output_filename, "w") as stats_file: 
            stats_file.write(encoded_msg)
    print("Done.")