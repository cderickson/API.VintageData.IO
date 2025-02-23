import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
credentials = [os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_NAME")]

def conn(query, vars=()):
    try:
        conn = psycopg2.connect(
            host=credentials[0],
            port=credentials[1],
            user=credentials[2],
            password=credentials[3],
            database=credentials[4],
            sslmode='require'
        )
        cursor = conn.cursor()

        if len(vars) > 0:
            cursor.execute(query,vars)
        else:
            cursor.execute(query)

        conn.commit()
    except psycopg2.Error as e:
        print('Error:', e)
    finally:
        if conn:
            cursor.close()
            conn.close()

create_api_logging_query = """
CREATE TABLE IF NOT EXISTS "API_LOGGING_STATS" (
    "ID" SERIAL PRIMARY KEY,
    "ENDPOINT" VARCHAR(100) NOT NULL,
    "METHOD" VARCHAR(100) NOT NULL,
    "QUERY_PARAMS" JSONB,
    "STATUS_CODE" INTEGER,
    "CLIENT_IP" TEXT,
    "USER_AGENT" TEXT,
    "REQUEST_START" TIMESTAMP NOT NULL,
    "REQUEST_END" TIMESTAMP,
    "RESPONSE_TIME_MS" DOUBLE PRECISION
);
"""

delete_api_logging_query = 'DROP TABLE IF EXISTS "API_LOGGING_STATS" CASCADE'

conn(delete_api_logging_query)
conn(create_api_logging_query)