  # Example Usage of Available Endpoints

  ## Source File Operations

  #### Validating source files by reporting year and layer ID  

  e.g. local server: `http://127.0.0.1:5000/invdb-py/source-file-validation?reporting_year=2023&layer_id=1&user_id=1` OR
       test server: `http://ghg-test.saic.com/invdb-py/source-file-validation?reporting_year=2023&layer_id=1&user_id=1`
       will validate all source files with the provided reporting year and layer id.
  
  **additional (optional) parameters:
  - `ids=1,2,3` -> (for debugging/testing only) will target specific source files with the attachment IDs listed in the parameter value. This will cause reporting_year and layer_id parameters to be ignored
  - `user_id: integer` ->  user ID of the requesting user
  - `debug: boolean` -> ignores the `validation_status` database column when selecting files to validate from the database. 
  

  #### Loading source files by reporting year and layer ID

  e.g. local server: `http://127.0.0.1:5000/invdb-py/source-file-load?reporting_year=2023&layer_id=1&user_id=1` OR
       test server: `http://ghg-test.saic.com/invdb-py/source-file-load?reporting_year=2023&layer_id=1&user_id=1` 
       will load all source files with the provided reporting year and layer id. 
  
  **additional (optional) parameters:
  - `user_id: integer` ->  user ID of the requesting user
  - `debug: boolean` -> ignores the `processed_date` database column when selecting files to load from the database. 

  ## Report Operations

  #### Validating reports by reporting year and layer ID

  e.g. local server: `http://127.0.0.1:5000/invdb-py/report-validation?reporting_year=2023&layer_id=1&user_id=1` OR
       test server: `http://ghg-test.saic.com/invdb-py/report-validation?reporting_year=2023&layer_id=1&user_id=1`
       will validate all reports with the provided reporting year and layer id.
  
  **additional (optional) parameters:
  - `ids=1,2,3` -> (for debugging/testing only) will target specific source files with the attachment IDs listed in the parameter value. This will cause reporting_year and layer_id parameters to be ignored
  - `user_id: integer` ->  user ID of the requesting user
  - `debug: boolean` -> ignores the `validation_status` database column when selecting files to validate from the database. 

  #### Processing reports by reporting year and layer ID

  e.g. local server: `http://127.0.0.1:5000/invdb-py/report-processing?reporting_year=2023&layer_id=1&user_id=1` OR
       test server: `http://ghg-test.saic.com/invdb-py/report-processing?reporting_year=2023&layer_id=1&user_id=1` 
       will process all reports with the provided reporting year and layer id. 
  
  **additional (optional) parameters:
  - `ids=1,2,30` -> (for debugging/testing only) will target specific source files with the attachment IDs listed in the parameter value. This will cause reporting_year and layer_id parameters to be ignored
  - `user_id: integer` ->  user ID of the requesting user
  - `debug: boolean` -> ignores the `processed_date` database column when selecting files to load from the database. 

  #### Loading online reports by report_id, and report_type_id

  e.g. local server: `<server_address>/invdb-py/load-online-report?report_id=1&report_type_id=1&user_id=1` OR
  e.g. local server: `<server_address>/invdb-py/load-online-report?report_id=1&report_type_id=2&user_id=1&gwp=ar4_gwp` OR

  **parameters:
  - `report_id: integer` -> specifier for which report to load (fetched from the dim_report or dim_qc_report tables [depending on the requested report type]).
   - `report_type_id: integer` -> specifier for which type of report to load: 
      - `1` -> emissions report
      - `2` -> QC report   
  - `user_id: integer` ->  user ID of the requesting user

 **additional (optional) parameters:
  `gwp: string` -> (for emission reports only) specifier for which GWP factor column from the dim_ghg table to use in the calculation. Value must match the name of the column from the dim_ghg table (e.g. `ar4_gwp`, `ar5_gwp`, `ar5f_gwp`, `ar6_gwp`, etc.)  
  
 ## Publication Operations

 #### Processing publications by publication object ID and action

  e.g. local server: `http://127.0.0.1:5000/invdb-py/publication-processing?pub_object_id=10&action=Prepare&user_id=1` OR
       test server: `http://ghg-test.saic.com/invdb-py/publication-processing?pub_object_id=10&action=Prepare&user_id=1` 
       will process the provided action on the publication with the provided publication object id. 
  
  ##### Parameters:
  - `pub_object_id: integer` -> publication object ID (which maps to the selected publication row (data product) in the UI).
  - `action: string` -> specifies which button was selected in the UI (either `Prepare` or `Refine` (case-insensitive)).
  - `user_id: integer` ->  user ID of the requesting user

  #### Downloading publications as Excel (.xlsx) by publication object ID and action

  e.g. local server: `http://127.0.0.1:5000/invdb-py/publication-download?pub_object_id=1,2,30&user_id=1` OR
       test server: `http://ghg-test.saic.com/invdb-py/publication-download?pub_object_id=1,2,30&user_id=1` 
       will export the refined data for the publication with the provided publication object(s) id into a Excel (.xlsx) file, zipping the files as needed. 
  
  ##### Parameters:
  - `pub_object_id: integer` -> publication object ID(s) (which maps to the selected publication row (data product) in the UI).
  - `user_id: integer` ->  user ID of the requesting user

  ## Query Engine Operations

 #### Processing a simple query

  e.g.`<server_address>/invdb-py/query_engine/simple-query?query=[[1, {"category": "Enteric Fermentation"}], [1, {"category": "Abandoned Wells", "ghg_category_name": "CH4"}]]&reporting_year=2024&layer_id=1&user_id=1` will return the results of the two explicitly defined simple queries defined in the `query` parameter. The queries are ran on the reporting_year
  and layer_id.
  
  ##### Parameters:
  - `query: array of fixed-sized array(s)` -> each subarray represents its own query and contains two values:
    + [0]: the query_formula_id of the query,
    + [1]: the query_parameters object containing the parameter names and their assigned values.
  - `reporting_year: integer` -> reporting_year to run the queries on
  - `layer_id: int` -> layer ID of the data layer to run the queries on
  - `user_id: integer` ->  user ID of the requesting user

  #### Processing a complex query

  e.g.`<server_address>/invdb-py/query_engine/complex-query?query=["[SQ1] %2B [SQ2]", "[SQ3] - [SQ4]"]&reporting_year=2024&layer_id=1&user_id=1` will return the results of the two explicitly defined complex queries defined in the `query` parameter. The queries are ran on the reporting_year
  and layer_id.
  
  ##### Parameters:
  - `query: array of strings` -> each string represents its own complex query formula 
      - Notes: 
        - simple query references must follow the syntax `[SQ<simple_query_id>]`
        - `+` symbols must be encoded as `%2B` 
  - `reporting_year: integer` -> reporting_year to run the queries on
  - `layer_id: int` -> layer ID of the data layer to run the queries on
  - `user_id: integer` ->  user ID of the requesting user

<br><br>
# Project Setup for Developing on the Backend: 

**1.** Install python and its package manager, poetry
  - **a.** Install `Python` version 3.9 (3.10 and above have problems with `poetry`)
    - link to my build: https://www.python.org/ftp/python/3.9.13/python-3.9.13-amd64.exe
    - Do a custom install and install it in your C:/Dev folder to avoid issues with CarbonBlack
  - **b.** Install `Poetry` version 1.6.1 or newer (Python required for installation)
    - link: https://python-poetry.org/docs/#installing-with-the-official-installer
      - If you run into SSL issues, do the following:
        - Obtain the pypi.pem file from Dillon or Archana, store it somewhere permanent, and copy the path.
        - On your desktop, open the start menu, and open the "Edit environment variables for your account" menu (not system variables)
        - Under the 'System variables' section, add a new variable called 'REQUESTS_CA_BUNDLE' and set the value to the path where you placed the pypi.pem file.
        - Close any open shells and VS Code for the changes to take effect.

**2.** Clone the project from repo: 
   https://ghg-gitlab-r8.corp.saic.com/gitlab/inventory/invdb-py.git
   (IMPORTANT: make sure the project's root folder is called 'invdb-py' and is not 
   nested in another folder called 'invdb-py')

**3.** Configure Database connection (instructions based on DBeaver)
  - **a.** Obtain Postgres DB .ppk credentials file from Dillon or Archana
     and store it somewhere permanent. 
  - **b.** Configure a new Postgres connection 
  - **c.** Configure the connection settings of the 'Main' tab: 
    - **i.** Host: (ghg-postgresql.c4f4o4t3zjmk.us-east-1.rds.amazonaws.com for production db) or (ghg-dbora50-r7.corp.saic.com for test db)
    - **ii.** Port: 5432
    - **iii.** Database: psql
    - **iv.** Username: invdb_usr
    - **v.** Password: (ggds2017qtr2 for production db) or (invdb_usr for test db)
  - **d.** Configure the connection settings of the 'SSH' tab: (skip if connecting to the test db)
    - **i.** Host/IP: uat.ccdsupport.com
    - **ii.** Port: 22
    - **iii.** User Name: dbuser
    - **iv.** Authentication Method: Public Key
    - **v.** Private Key: Browse for/Enter path of .ppk file obtained in Step 3a.
  - **e.** Press "Test Connection" at the bottom of the dialog and confirm 
     that the connection is successful.

**4.** To test the application locally while connecting to the production database, you need to open an SSH Tunnel via PuTTY to connect to the Postgres DB: 
  - **a.** Install PuTTY if needed, then launch it
  - **b.** Save a connection configuration to connect to the Postgres DB: 
    - **i.** On the Session menu, set: 
      - HostName (or IP Address): uat.ccdsupport.com
      - Port: 22
    - **ii.** On the Connection >> SSH >> Auth >> Credentials menu, set: 
      - Private key file for authentication: Browse for/Enter path of .ppk file obtained Step 3a.
    - **iii.** On the Connection >> SSH >> Tunnels menu, set: 
      - Source port: 5432
      - Destination: ghg-postgresql.c4f4o4t3zjmk.us-east-1.rds.amazonaws.com:5432
      - Press the 'Add' Button
    vi. Back on the Session menu:
      - In the text box under "Saved Sessions", type an appropriate name for the configuration, such as "InvDB_postgres_db"
      - Press the "Save" button. (Now you can press "InvDB_postgres_db" from the list and press the "Load" button to skip the config process for future sessions.)
  - **c.** Press "Open" to start the connection
  - **d.** Login as "dbuser" (No password needed. Ignore update message)

**5.** Run the application locally
  - **a.** Navigate to the project folder in a bash terminal (perferrably within your IDE)
  - **b.** Configure your poetry image's default virtual environments directory to be somewhere in your Dev folder to avoid CarbonBlack restrictions with command `poetry configure virtualenvs.path C:/Dev/Python Virtual Environments/Poetry`  
  - **c.** Create/Enter your poetry virtual environment with command `poetry shell` 
  - **d.** Update project dependencies with command `poetry install` (if you run into SSL issues, refer to the instructions in Step 1b)
        (This may take a long time when running for the first time. If you encounter TLS/SSL certificiate issues, follow the instructions on this page under Python/PIP >> Environment Variables: https://es3.saic.com/es3?id=kb_article_view&table=kb_knowledge&sys_kb_id=b4aaf7fc1b263994a76ba82fe54bcb78#git)
  - **e.** Make sure your SSH tunnel is open if connecting to the production database (see Step 4). 
  - **f.** Run the application locally with connection to the *production* database with by giving the command `poetry run python __init__.py --prod-db`
    - Or connect to the *test* database with by giving the command `poetry run python __init__.py`
    - then when you make changes to the project, you need to restart the server. Stop the server with "Ctrl + C" and reissue the command).
  - **g.** In step e, the you should see output: "Serving on http://127.0.0.1:5000" or similar. Open this address in your browser to visit the index page of the API service. See section "Example Usage of Available Endpoints" above to see which URL request will trigger which use case. API return values will show in the browser and python console output will show in the bash terminal. Happy coding!

## Notes: 
  - You can exit the poetry virtual environment with the `exit` command
  - You can execute a particular python file from the project with the command `poetry run python <FILEPATH>`

## Setting up a python debugger in VS Code:
  - If you want to use a python debugger through VS Code, refer to the Run Configuration solution below. You'll have to change the python and program values to match your project and virtual environment locations.  
  `{
  "version": "0.2.0",
  "configurations": [
      {
          "name": "Python: Poetry",
          "type": "debugpy",
          "request": "launch",
          "cwd": "${workspaceFolder}",
          "console": "integratedTerminal",
          "python": "C:\\Users\\brownd15\\AppData\\Local\\pypoetry\\Cache\\virtualenvs\\invdb-py-YK_Ef-dw-py3.9\\Scripts\\python.exe",
          "program": "C:\\Dev\\Projects\\invdb-py\\__init__.py", // full path to file to execute
          "redirectOutput": true,
          "justMyCode": false,
          "stopOnEntry": false
        }
  ]
}`

- If you get a message like `...ConnectionRefusedError: [WinError 10061] No connection could be made because the target machine actively refused it`, then you likely need to change your Default Profile in VS Code to `Command Prompt (cmd)` (you can still choose other profiles with the drop down).

## Troubleshooting: 
  - If the API gives the message `The server encountered an internal error and was unable to complete your request. Either the server is overloaded or there is an error in the application.`, make sure you don't have more than one instance of the server running at a time on your machine.
  - If, upon running the server that connects to the production database, the bash terminal gives the message : `connection to server at ... failed: Connection refused (0x0000274D/10061). Is the server running on that host and accepting TCP/IP connections?`, re-establish your SSH tunnel (which times out about once every 12 hours and closes from VPN outages) 
  - If the database failed to complete an SQL operation and subsequent requests give the error: `psycopg2.errors.InFailedSqlTransaction: current transaction is aborted, commands ignored until end of transaction block`, you can rollback the database changes by restarting the server.
  - If the message `psycopg2.OperationalError: could not translate host name "ghg-dbora50-r7.corp.saic.com" to address: No such host is known.` appears, then you likely need to reconnect to your VPN.
