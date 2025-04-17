import chalicelib.src.general.globals as globals
from psycopg2 import pool
import psycopg2
import os

"""We are using singleton approach to initialization the database connection pool"""
class DbPoolConfigurator:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DbPoolConfigurator, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            # Run initialization only once
            self.setup_pool()
            self._initialized = True

    def setup_pool(self):
        self.db_config = {
            'database': 'psql',
            'user': 'invdb_usr',
            'password': 'invdb_usr' if globals.ENV == 'DEVELOPMENT' else 'ggds2017qtr2',
            'host': globals.PRODUCTION_DB_SERVER if globals.ENV == "PRODUCTION" else globals.DEVELOPMENT_DB_SERVER if globals.ENV == "DEVELOPMENT" else "localhost",
            'port': '5432'  # default PostgreSQL port is 5432
        }
        self.db_pool = None
        try:
            self.db_pool = pool.ThreadedConnectionPool(
                globals.db_pooling_min_connections,
                globals.db_pooling_max_connections,
                **self.db_config
            )
        except Exception as e:
            print("An error occurred while setting up the connection pool:", e)
            raise

    def get_connection(self):
        if self.db_pool:
            return self.db_pool.getconn()
        else:
            raise ConnectionError("Connection pool is not set up.")

    def return_connection(self, conn):
        if self.db_pool:
            self.db_pool.putconn(conn)
        else:
            raise ConnectionError("Connection pool is not set up.")

    def close_all_connections(self):
        if self.db_pool:
            self.db_pool.closeall()
        else:
            raise ConnectionError("Connection pool is not set up.")
    # From now on, use db_pool_configurator.get_connection() and db_pool_configurator.return_connection()
    # to work with database connections.

    def test_connection(self):
        if self.db_pool:
            try:
                # Get connection from the pool
                conn = self.get_connection()
                if conn:
                    print("Successfully got a connection from the pool.")
                    
                    # Test if you can perform a database operation
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1;")
                    print("Query executed successfully:", cursor.fetchone())
                    cursor.close()
                    return True
                else:
                    print("Failed to retrieve a connection from the pool.")
                    return False
            except Exception as e:
                print("An error occurred while testing the connection:", e)
                return False
            finally:
                # Always return the connection to the pool
                if conn:
                    self.return_connection(conn)
        else:
            print("Connection pool is not set up.")
            return False