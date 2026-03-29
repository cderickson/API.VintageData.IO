import pandas as pd
import psycopg2
from datetime import datetime
import os

_REQUIRED_DB_VARS = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")
_REQUIRED_SHEET_VARS = ("VINTAGE_SHEET_CURR", "VINTAGE_GID_DECK")

# MATCH_ID      = 11000000000
# EVENT_ID      = 12000000000
# DECK_ID       = 13000000000
# EVENT_TYPE_ID = 14000000000
# LOAD_RPT_ID   = 15000000000
# EV_REJ_ID     = 16000000000
# MATCH_REJ_ID  = 17000000000
    
def parse_class_sheet():
    sheet_id = os.getenv(_REQUIRED_SHEET_VARS[0])
    deck_gid = os.getenv(_REQUIRED_SHEET_VARS[1])
    if not sheet_id or not deck_gid:
        raise ValueError(
            "Missing required environment variables for classification sheet: "
            f"{', '.join(_REQUIRED_SHEET_VARS)}"
        )

    deck_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={deck_gid}"
    df = pd.read_csv(deck_url)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    raw_export_path = os.path.join(log_dir, "classifications_raw.xlsx")
    df.to_excel(raw_export_path, index=False)
    print(f"Exported raw classifications sheet to {raw_export_path}")

    # Create dataframe with valid Deck Names.
    df_decks = df[["Archetype", "Subarchetype"]].copy()
    df_decks.columns = ["ARCHETYPE", "SUBARCHETYPE"]
    df_decks["ARCHETYPE"] = df_decks["ARCHETYPE"].astype(str).str.strip().str.upper()
    df_decks["SUBARCHETYPE"] = df_decks["SUBARCHETYPE"].astype(str).str.strip().str.upper()
    df_decks = df_decks[(df_decks["ARCHETYPE"] != "") & (df_decks["SUBARCHETYPE"] != "")]

    # Adding rows for NA and NO SHOW.
    df_decks = pd.concat(
        [
            df_decks,
            pd.DataFrame(
                {
                    "ARCHETYPE": ["NA", "NA", "NA"],
                    "SUBARCHETYPE": ["NA", "NO SHOW", "INVALID_NAME"],
                }
            ),
        ],
        ignore_index=True,
    )

    # Create dataframe with valid Event Types.
    df_events = df[["Event Types"]].copy()
    df_events.columns = ["EVENT_TYPE"]
    df_events = df_events.dropna(subset=["EVENT_TYPE"])
    df_events["EVENT_TYPE"] = df_events["EVENT_TYPE"].astype(str).str.strip().str.upper()
    df_events = df_events[df_events["EVENT_TYPE"] != ""]

    # Adding row for NA.
    df_events = pd.concat([df_events, pd.DataFrame({"EVENT_TYPE": ["INVALID_TYPE"]})], ignore_index=True)

    # Add Format column to Decks table.
    df_decks["FORMAT"] = "VINTAGE"
    df_decks = df_decks[["FORMAT", "ARCHETYPE", "SUBARCHETYPE"]].drop_duplicates()
    df_decks = df_decks.sort_values(["ARCHETYPE", "SUBARCHETYPE"]).reset_index(drop=True)

    # Add Format column to Events table.
    df_events["FORMAT"] = "VINTAGE"
    df_events = df_events[["FORMAT", "EVENT_TYPE"]].drop_duplicates().sort_values(["EVENT_TYPE"]).reset_index(drop=True)
    
    return (df_decks, df_events)

def class_insert(df_valid_decks=None, df_valid_event_types=None):
    valid_decks_query = """
        INSERT INTO "VALID_DECKS" ("FORMAT", "ARCHETYPE", "SUBARCHETYPE", "PROC_DT")
        VALUES (%s, %s, %s, %s)
        ON CONFLICT ("FORMAT", "ARCHETYPE", "SUBARCHETYPE")
        DO NOTHING
    """
    valid_event_types_query = """
        INSERT INTO "VALID_EVENT_TYPES" ("FORMAT", "EVENT_TYPE", "PROC_DT")
        VALUES (%s, %s, %s)
        ON CONFLICT ("FORMAT", "EVENT_TYPE") 
        DO NOTHING
    """
    credentials = [os.getenv(var) for var in _REQUIRED_DB_VARS]
    missing_db_vars = [name for name, value in zip(_REQUIRED_DB_VARS, credentials) if not value]
    if missing_db_vars:
        raise ValueError(
            "Missing required database environment variables: "
            + ", ".join(missing_db_vars)
        )

    proc_dt = datetime.now()
    conn = None
    cursor = None
    deck_success = 0
    deck_errors = 0
    event_success = 0
    event_errors = 0
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

        # Insert valid_decks
        if df_valid_decks is not None:
            values_list = [
                (row.FORMAT, row.ARCHETYPE, row.SUBARCHETYPE, proc_dt)
                for row in df_valid_decks.itertuples(index=False)
            ]
            for values in values_list:
                try:
                    cursor.execute(valid_decks_query, values)
                    deck_success += 1
                except Exception as e:
                    print(f"Error inserting row into VALID_DECKS: {values} | Error: {e}")
                    deck_errors += 1
                    continue  # Skip the row and continue with the next one

        # Insert valid_event_types
        if df_valid_event_types is not None:
            values_list = [
                (row.FORMAT, row.EVENT_TYPE, proc_dt)
                for row in df_valid_event_types.itertuples(index=False)
            ]
            for values in values_list:
                try:
                    cursor.execute(valid_event_types_query, values)
                    event_success += 1
                except Exception as e:
                    print(f"Error inserting row into VALID_EVENT_TYPES: {values} | Error: {e}")
                    event_errors += 1
                    continue
        conn.commit()
        print(
            f"Classification insert complete. Deck rows processed={deck_success}, deck errors={deck_errors}, "
            f"event type rows processed={event_success}, event type errors={event_errors}."
        )

    except Exception as e:
        print(f"Error occurred while loading data: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()