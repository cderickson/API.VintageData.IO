import os
from dotenv import load_dotenv
import psycopg2
import time

load_dotenv()
credentials = [os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_NAME")]



# RDS connection parameters
RDS_HOST = credentials[0]
RDS_PORT = credentials[1]
RDS_USER = credentials[2]
RDS_PASSWORD = credentials[3]

# Database names
OLD_DB = "mtgo_data_collection"
NEW_DB = "mtgo_vintage_metagame"

def execute_query(conn, query):
    """Execute a SQL query and commit changes."""
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()

try:
    # Connect to the default "postgres" database to drop/create databases
    conn = psycopg2.connect(
        dbname="postgres", user=RDS_USER, password=RDS_PASSWORD, host=RDS_HOST, port=RDS_PORT
    )
    conn.autocommit = True  # Required for DROP/CREATE DATABASE commands

    # Terminate all active connections to the old database
    print(f"Terminating active connections to {OLD_DB}...")
    execute_query(
        conn,
        f"""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '{OLD_DB}'
        AND pid <> pg_backend_pid();
        """
    )

    # Wait a moment to ensure all sessions are closed
    time.sleep(2)

    # Drop the old database
    print(f"Dropping database {OLD_DB}...")
    execute_query(conn, f"DROP DATABASE IF EXISTS {OLD_DB};")

    # Wait a few seconds to ensure it’s fully removed
    time.sleep(2)

    # Create the new database
    print(f"Creating new database {NEW_DB}...")
    execute_query(conn, f"CREATE DATABASE {NEW_DB};")

    print("Database rename completed successfully.")

except psycopg2.Error as e:
    print(f"Error: {e}")

finally:
    if conn:
        conn.close()
