from dotenv import load_dotenv
import sys
import os

# load environment variables
if '--env' not in sys.argv:
    load_dotenv('./env/invdb-py.env')
else:
    load_dotenv(sys.argv[sys.argv.index('--env') + 1])

# define other global values
PRODUCTION_DB_SERVER = "ghg-postgresql.c4f4o4t3zjmk.us-east-1.rds.amazonaws.com"
DEVELOPMENT_DB_SERVER = "ghg-dbora50-r7.corp.saic.com"
APP_NAME = "invdb-py"

AWS_AUTH_PERMISSIONS_SERVICE = "https://data.epa.gov/permissions-service"
APP_AWS_AUTH_TOKEN = None
APP_S3_SESSION = None

ENV = os.environ.get("py-env", "DEVELOPMENT")  # either "DEVELOPMENT" or "UAT"

S3_BUCKET_NAME = "invdb-test-data-files" if ENV == "DEVELOPMENT" else "invdb-prod-data-files";
print("ENVIRONMENT:", ENV)
debug = False

allow_multithreading = True
db_pooling_min_connections = 1
db_pooling_max_connections = 20