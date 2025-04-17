from chalicelib.src.publications.jobs.download_excel.queries import *
import chalicelib.src.database.methods as db_methods
from chalicelib.src.jobs.models.Job import Job
import chalicelib.src.jobs.constants as job_constants
import chalicelib.src.general.helpers as helpers
import chalicelib.src.general.globals as invdb_globals
import chalicelib.src.publications.constants as pub_constants
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import make_response, jsonify
from openpyxl import Workbook
import tempfile
import zipfile
import json
import io
import os


# read the columns of a the first row
def load_json_data_to_excel_sheet(data: [dict], excel_sheet):
    if len(data) == 0:
        return

    # define column order
    column_order = list(data[0].keys())
    # move econ_sector and econ_subsector to the front of the column order if present
    if "econ_subsector" in column_order:
        column_order = ["econ_subsector"] + [
            col for col in column_order if col != "econ_subsector"
        ]
    if "econ_sector" in column_order:
        column_order = ["econ_sector"] + [
            col for col in column_order if col != "econ_sector"
        ]

    # write in the header row
    excel_sheet.append(column_order)

    # sort the data according to its column order (the left-most column is the primary sorting key, the 2nd column is the secondary sorting key, and so on...)
    data.sort(
        key=lambda x: [str(None if key not in x else x[key]) for key in column_order]
    )

    # write in the data:
    for row in range(len(data)):
        excel_sheet.append(
            [
                (None if column not in data[row] else data[row][column])
                for column in column_order
            ]
        )


def convert_single_data_product_to_excel(data_product: dict, time_series: [int]):
    # Create a new Workbook object
    excel_file = Workbook()

    data = data_product["refined_data"]
    download_file_name = data_product["refined_tablename"]

    # convert to python object (or list) if needed
    if isinstance(data, str):
        data = json.loads(data)

    if isinstance(data, list):  # single tab JSON
        sheet = excel_file.active
        sheet.title = "InvDB"
        data = helpers.transpose_json_to_landscape(data, time_series)
        load_json_data_to_excel_sheet(data, sheet)

    elif isinstance(
        data, dict
    ):  # multi-tab JSON, where the top level is a dict that maps the tab name to its data contents
        excel_file.remove(excel_file.active)
        for tab_name in data.keys():
            current_sheet = excel_file.create_sheet(title=tab_name)
            before_len = len(data[tab_name])
            data[tab_name] = helpers.transpose_json_to_landscape(
                data[tab_name], time_series
            )
            load_json_data_to_excel_sheet(data[tab_name], current_sheet)
            after_len = len(data[tab_name])
    else:
        raise ValueError(
            "publications.convert_single_data_product_to_excel: Error: data format not supported."
        )

    return excel_file, download_file_name


def convert_data_products_to_excel(data_products: list, reporting_year: int):
    excel_files = []
    time_series = db_methods.get_time_series_by_reporting_year(reporting_year)
    # ================= multi threaded version =====================
    if invdb_globals.allow_multithreading:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for data_product in data_products:
                futures.append(
                    executor.submit(
                        convert_single_data_product_to_excel, data_product, time_series
                    )
                )

            for future in as_completed(futures):
                excel_files.append(future.result())

            executor.shutdown(wait=True)

    # ================ single threaded version =====================
    else:
        for data_product in data_products:
            excel_files.append(
                convert_single_data_product_to_excel(data_product, time_series)
            )

    return excel_files


def prepare_download_file(excel_files: [(Workbook, str)]):
    """takes the excel files (type: tuple ([0] Workbook content, [1] expected file name) and creates .zip consisting of .xlsx files"""

    buffer = io.BytesIO()
    # if len(excel_files) == 1:
    #     #save the file binary to buffer
    #     excel_files[0][0].save(buffer)
    #     buffer.seek(0)

    #     # formulate the response object
    #     response = make_response(buffer.getvalue())
    #     response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    #     response.headers['Content-Disposition'] = f'attachment; filename={excel_files[0][1]}.xlsx'

    #     return response

    # else: # for multiple excel files
    # create the temp files
    temp_files = []
    for excel_file in excel_files:
        temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
        excel_file[0].save(temp_file.name)
        temp_files.append(temp_file)

    # Create a zip file
    with zipfile.ZipFile(buffer, "w") as zip_file:
        # Add the temporary files to the zip
        for temp_file, file_name in zip(
            temp_files, [file_name for _, file_name in excel_files]
        ):
            zip_file.write(temp_file.name, f"{file_name}.xlsx")

    # Remove the temporary files
    for temp_file in temp_files:
        temp_file.close()

    # return the zip
    buffer.seek(0)

    # formulate the response object
    response = make_response(buffer.getvalue())
    response.headers["Content-Type"] = "application/zip"
    response.headers["Content-Disposition"] = f"attachment; filename=publications.zip"

    return response


def handle_publication_download_request(pub_object_ids: [int], user_id: int):
    try:
        # fetch the reporting_year and layer_id from the database based on the input arguments
        reporting_year, layer_id = fetch_reporting_year_and_layer_id(pub_object_ids)

        if reporting_year is None:
            return {"result": f"No refined publication objects have been found for the provided publication object IDs: {pub_object_ids}"}

        this_job = Job(
            job_constants.PUBLICATION_DOWNLOAD_NAME,
            job_constants.PUBLICATION_DOWNLOAD_DESC,
            reporting_year,
            layer_id,
            user_id,
            misc_info = {"Publication Object IDs": pub_object_ids}
        )

        helpers.tprint("Fetching publication data from the database...")
        this_job.post_event(
            "PUBLICATION_DOWNLOAD",
            "FETCHING_PUBLICATION_DATA",
        )
        data_product_info = fetch_refined_data_products(pub_object_ids) 
        
        

        # report and remove any missing refined_tables from the selection
        nonempty_data_products = []
        for data_product, i in zip(data_product_info, range(len(data_product_info))):
            if data_product["refined_tablename"] and data_product["refined_data"]:
                nonempty_data_products.append(data_product)
            else:
                helpers.tprint(f"NOTE: The publication object with ID {data_product['pub_object_id']} has no refined data. Excluding from download.")

        helpers.tprint("Converting to excel...")
        this_job.post_event(
            "PUBLICATION_DOWNLOAD",
            "PROCESSING_DATA_INTO_EXCEL",
        )
        excel_files = convert_data_products_to_excel(nonempty_data_products, reporting_year) # returns tuples: (file_contents, file_name)

        helpers.tprint("Preparing the download file...")
        this_job.post_event(
            "PUBLICATION_DOWNLOAD",
            "PREPARING_FILE",
        )
        download_file = prepare_download_file(excel_files)
        
        helpers.tprint("File is on the way!")
        this_job.post_event(
            "PUBLICATION_DOWNLOAD",
            "TRANSMITTING_FILE",
        )
        this_job.update_status("COMPLETE")
        return download_file
        
    except Exception:
        from chalicelib.src.database.methods import get_pgdb_connection
        import traceback
        if "this_job" in locals(): 
            this_job.update_status("ERROR")
        pgdb_connection = get_pgdb_connection()
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500