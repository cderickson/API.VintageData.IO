import psycopg2
import os

credentials = [os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_NAME")]

# MATCH_ID      = 11000000000
# EVENT_ID      = 12000000000
# DECK_ID       = 13000000000
# EVENT_TYPE_ID = 14000000000
# LOAD_RPT_ID   = 15000000000
# EV_REJ_ID     = 16000000000
# MATCH_REJ_ID  = 17000000000
# EV_STD_REJ_ID = 18000000000
    
def conn(query, vars=()):
    print(f"Connecting to database: {credentials[0]}:{credentials[1]}/{credentials[4]}")
    db_conn = None
    cursor = None
    try:
        db_conn = psycopg2.connect(
            host=credentials[0],
            port=credentials[1],
            user=credentials[2],
            password=credentials[3],
            database=credentials[4],
            sslmode='require'
        )
        cursor = db_conn.cursor()

        if len(vars) > 0:
            cursor.execute(query, vars)
        else:
            cursor.execute(query)

        db_conn.commit()
        return True
    except psycopg2.Error as e:
        print('Error:', e)
        return False
    finally:
        if cursor:
            cursor.close()
        if db_conn:
            db_conn.close()

def delete_table(TABLE):
    if not TABLE or '"' in TABLE:
        raise ValueError(f'Invalid table name: {TABLE}')
    query = f'DROP TABLE IF EXISTS "{TABLE}" CASCADE'
    conn(query)

def create_new_tables():
    print("Starting database table creation...")
    create_valid_decks_query = """
    CREATE TABLE IF NOT EXISTS "VALID_DECKS" (
        "FORMAT" VARCHAR(30),
        "ARCHETYPE" VARCHAR(30),
        "SUBARCHETYPE" VARCHAR(30),
        "DECK_ID" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 13000000000) PRIMARY KEY,
        "PROC_DT" TIMESTAMP WITHOUT TIME ZONE,
        CONSTRAINT unique_deck UNIQUE ("FORMAT", "ARCHETYPE", "SUBARCHETYPE")
    );
    """
    create_valid_event_types_query = """
    CREATE TABLE IF NOT EXISTS "VALID_EVENT_TYPES" (
        "FORMAT" VARCHAR(30),
        "EVENT_TYPE" VARCHAR(30),
        "EVENT_TYPE_ID" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 14000000000) PRIMARY KEY,
        "PROC_DT" TIMESTAMP WITHOUT TIME ZONE,
        CONSTRAINT unique_event_type UNIQUE ("FORMAT", "EVENT_TYPE")
    );
    """
    create_events_query = """
    CREATE TABLE IF NOT EXISTS "EVENTS" (
        "EVENT_ID" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 12000000000) PRIMARY KEY,
        "EVENT_DATE" DATE,
        "EVENT_TYPE_ID" BIGINT,
        "PROC_DT" TIMESTAMP WITHOUT TIME ZONE,
        FOREIGN KEY ("EVENT_TYPE_ID") REFERENCES "VALID_EVENT_TYPES"("EVENT_TYPE_ID") ON UPDATE CASCADE
    );
    """
    create_matches_query = """
    CREATE TABLE IF NOT EXISTS "MATCHES" (
        "MATCH_ID" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 11000000000),
        "P1" VARCHAR(30),
        "P2" VARCHAR(30),
        "P1_WINS" INT,
        "P2_WINS" INT,
        "MATCH_WINNER" VARCHAR(2),
        "P1_DECK_ID" BIGINT,
        "P2_DECK_ID" BIGINT,
        "P1_NOTE" VARCHAR(100),
        "P2_NOTE" VARCHAR(100),
        "EVENT_ID" BIGINT,
        "PROC_DT" TIMESTAMP WITHOUT TIME ZONE,
        PRIMARY KEY ("MATCH_ID", "P1"),
        FOREIGN KEY ("P1_DECK_ID") REFERENCES "VALID_DECKS"("DECK_ID") ON UPDATE CASCADE,
        FOREIGN KEY ("P2_DECK_ID") REFERENCES "VALID_DECKS"("DECK_ID") ON UPDATE CASCADE,
        FOREIGN KEY ("EVENT_ID") REFERENCES "EVENTS"("EVENT_ID") ON UPDATE CASCADE ON DELETE CASCADE
    );
    """
    create_ranks_query = """
    CREATE TABLE IF NOT EXISTS "EVENT_STANDINGS" (
        "EVENT_ID" BIGINT,
        "P1" VARCHAR(30),
        "BYES" INT,
        "EVENT_RANK" INT,
        "PROC_DT" TIMESTAMP WITHOUT TIME ZONE,
        PRIMARY KEY ("EVENT_ID", "EVENT_RANK"),
        FOREIGN KEY ("EVENT_ID") REFERENCES "EVENTS"("EVENT_ID") ON UPDATE CASCADE ON DELETE CASCADE
    );
    """
    create_load_reports_query = """
    CREATE TABLE IF NOT EXISTS "LOAD_REPORTS" (
        "LOAD_RPT_ID" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 15000000000) PRIMARY KEY,
        "START_DATE" DATE,
        "END_DATE" DATE,
        "RECORDS_FULL_DS" INT,
        "RECORDS_TOTAL" INT,
        "EVENTS_IGNORED" INT,
        "RECORDS_PROC" INT,
        "MATCHES_DELETED" INT,
        "MATCHES_INSERTED" INT,
        "MATCHES_SKIPPED" INT,
        "EVENTS_DELETED" INT,
        "EVENTS_INSERTED" INT,
        "EVENTS_SKIPPED" INT,
        "STANDINGS_DELETED" INT,
        "STANDINGS_INSERTED" INT,
        "STANDINGS_SKIPPED" INT,
        "DB_CONN_ERROR_TEXT" VARCHAR(100),
        "PROC_DT" TIMESTAMP WITHOUT TIME ZONE
    );
    """
    create_event_rejections_query = """
    CREATE TABLE IF NOT EXISTS "EVENT_REJECTIONS" (
        "EVENT_REJ_ID" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 16000000000) PRIMARY KEY,
        "LOAD_RPT_ID" BIGINT,
        "EVENT_ID" BIGINT,
        "EVENT_DATE" DATE,
        "EVENT_TYPE_ID" BIGINT,
        "PROC_DT" TIMESTAMP WITHOUT TIME ZONE,
        "REJ_TYPE" VARCHAR(1),
        "EVENT_REJ_TEXT" VARCHAR(100),
        FOREIGN KEY ("LOAD_RPT_ID") REFERENCES "LOAD_REPORTS"("LOAD_RPT_ID") ON UPDATE CASCADE ON DELETE CASCADE
    );
    """
    create_match_rejections_query = """
    CREATE TABLE IF NOT EXISTS "MATCH_REJECTIONS" (
        "MATCH_REJ_ID" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 17000000000) PRIMARY KEY,
        "LOAD_RPT_ID" BIGINT,
        "MATCH_ID" BIGINT,
        "P1" VARCHAR(30),
        "P2" VARCHAR(30),
        "P1_WINS" INT,
        "P2_WINS" INT,
        "MATCH_WINNER" VARCHAR(2),
        "P1_DECK_ID" BIGINT,
        "P2_DECK_ID" BIGINT,
        "P1_NOTE" VARCHAR(100),
        "P2_NOTE" VARCHAR(100),
        "EVENT_ID" BIGINT,
        "PROC_DT" TIMESTAMP WITHOUT TIME ZONE,
        "REJ_TYPE" VARCHAR(1),
        "MATCH_REJ_TEXT" VARCHAR(100),
        FOREIGN KEY ("LOAD_RPT_ID") REFERENCES "LOAD_REPORTS"("LOAD_RPT_ID") ON UPDATE CASCADE ON DELETE CASCADE
    );
    """
    create_ranks_rejections_query = """
    CREATE TABLE IF NOT EXISTS "RANK_REJECTIONS" (
        "EV_STD_REJ_ID" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 18000000000) PRIMARY KEY,
        "LOAD_RPT_ID" BIGINT,
        "EVENT_ID" BIGINT,
        "P1" VARCHAR(30),
        "BYES" INT,
        "EVENT_RANK" INT,
        "PROC_DT" TIMESTAMP WITHOUT TIME ZONE,
        "REJ_TYPE" VARCHAR(1),
        "RANK_REJ_TEXT" VARCHAR(100),
        FOREIGN KEY ("LOAD_RPT_ID") REFERENCES "LOAD_REPORTS"("LOAD_RPT_ID") ON UPDATE CASCADE ON DELETE CASCADE
    );
    """
    create_fkey_indexes = """
    CREATE INDEX IF NOT EXISTS idx_events_event_type_id ON "EVENTS"("EVENT_TYPE_ID");
    CREATE INDEX IF NOT EXISTS idx_matches_p1_deck_id ON "MATCHES"("P1_DECK_ID");
    CREATE INDEX IF NOT EXISTS idx_matches_p2_deck_id ON "MATCHES"("P2_DECK_ID");
    CREATE INDEX IF NOT EXISTS idx_matches_event_id ON "MATCHES"("EVENT_ID");
    CREATE INDEX IF NOT EXISTS idx_ranks_event_id ON "EVENT_STANDINGS"("EVENT_ID");
    CREATE INDEX IF NOT EXISTS idx_event_rejections_load_rpt_id ON "EVENT_REJECTIONS"("LOAD_RPT_ID");
    CREATE INDEX IF NOT EXISTS idx_match_rejections_load_rpt_id ON "MATCH_REJECTIONS"("LOAD_RPT_ID");
    CREATE INDEX IF NOT EXISTS idx_ev_rank_rejections_load_rpt_id ON "RANK_REJECTIONS"("LOAD_RPT_ID");
    """
    operations = [
        ("VALID_DECKS", create_valid_decks_query),
        ("VALID_EVENT_TYPES", create_valid_event_types_query),
        ("EVENTS", create_events_query),
        ("MATCHES", create_matches_query),
        ("EVENT_STANDINGS", create_ranks_query),
        ("LOAD_REPORTS", create_load_reports_query),
        ("EVENT_REJECTIONS", create_event_rejections_query),
        ("MATCH_REJECTIONS", create_match_rejections_query),
        ("RANK_REJECTIONS", create_ranks_rejections_query),
        ("INDEXES", create_fkey_indexes),
    ]
    total_ops = len(operations)
    for i, (name, query) in enumerate(operations, start=1):
        print(f"[{i}/{total_ops}] Creating {name}...")
        if not conn(query):
            print(f"[{i}/{total_ops}] FAILED: {name}")
            print(f'Failed creating database object: {name}')
            return False
        print(f"[{i}/{total_ops}] Done: {name}")
    print("Database table creation complete.")
    return True

def delete_all_tables():
    delete_table('VALID_DECKS')
    delete_table('VALID_EVENT_TYPES')
    delete_table('EVENTS')
    delete_table('MATCHES')
    delete_table('EVENT_STANDINGS')
    delete_table('LOAD_REPORTS')
    delete_table('EVENT_REJECTIONS')
    delete_table('MATCH_REJECTIONS')
    delete_table('RANK_REJECTIONS')