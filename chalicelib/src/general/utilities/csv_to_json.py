import chalicelib.src.database.constants as db_constants
import csv
import json

def cast_to_numeric(value_str):
    try:
        value_float = float(value_str)
        if value_float.is_integer():
            return int(value_float)
        else:
            return float(format(value_float, 'g'))
    except ValueError:
        return 0

def convert_csv_to_portrait_json(csv_file_name: str) -> dict:
    """input CSV is assumed to have report_row_id in the first column immediately followed by the time series data. Data
    begins on the first line."""
    # Open the CSV file
    data = []
    with open(csv_file_name, 'r', newline='', encoding='utf-8-sig') as file:
        reader = csv.reader(file) # create file reader
        next(reader) # skip the header line
        
        # Iterate over the rows in the CSV file
        for row in reader:
            if not row[1]:
                break
            year_id = 1
            for cell in row[1:]:
                data.append({
                    "report_row_id": row[0],
                    "year_id": year_id,
                    "emission_value": cast_to_numeric(cell)
                })
                year_id += 1

    return json.dumps(data, indent=4) 

# test case: 
# from chalicelib.src.general.utilities.csv_to_json import convert_csv_to_portrait_json
# json_string1 = convert_csv_to_portrait_json("tests\static_data\Report_Output test data in landscape form.csv")
# json_string2 = convert_csv_to_portrait_json("tests\static_data\qc_comp_report_output test data in landscape form.csv")
# with open("data1.json", "w") as f:
#     f.write(json_string1)
# with open("data2.json", "w") as f:
#     f.write(json_string2)