import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import warnings
import traceback
import os

credentials = [os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_NAME")]
gsheets = [os.getenv("VINTAGE_SHEET_CURR"), os.getenv("VINTAGE_SHEET_ARCHIVE"), os.getenv("VINTAGE_GID_MATCHES"), os.getenv("VINTAGE_GID_DECK"), os.getenv('VINTAGE_GID_STANDINGS')]

# sheet_curr = '1wxR3iYna86qrdViwHjUPzHuw6bCNeMLb72M25hpUHYk'
# sheet_archive = '1PxNYGMXaVrRqI0uyMQF46K7nDEG16WnDoKrFyI_qrvE'
# gid_matches = '2141931777'
# gid_deck = '590005429'
# gid_standings = '1693401931'

warnings.filterwarnings('ignore', category=UserWarning, message="pandas only supports SQLAlchemy connectable")

# MATCH_ID      = 11000000000
# EVENT_ID      = 12000000000
# DECK_ID       = 13000000000
# EVENT_TYPE_ID = 14000000000
# LOAD_RPT_ID   = 15000000000
# EV_REJ_ID     = 16000000000
# MATCH_REJ_ID  = 17000000000
   
def _refresh_env():
    """Refresh environment-backed globals so scripts pick up latest values."""
    global credentials, gsheets
    credentials = [os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_NAME")]
    gsheets = [os.getenv("VINTAGE_SHEET_CURR"), os.getenv("VINTAGE_SHEET_ARCHIVE"), os.getenv("VINTAGE_GID_MATCHES"), os.getenv("VINTAGE_GID_DECK"), os.getenv('VINTAGE_GID_STANDINGS')]


def get_df(query, vars=()):
    _refresh_env()
    conn = psycopg2.connect(
        host=credentials[0],
        port=credentials[1],
        user=credentials[2],
        password=credentials[3],
        database=credentials[4],
        sslmode='require'
    )

    df = pd.read_sql(query,conn,params=vars)

    conn.close()
    return df

def delete_records(start_date, end_date):
    _refresh_env()
    conn = None
    cursor = None
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

        query = """
            DELETE FROM "[vapi].EVENTS"
            WHERE "EVENT_DATE" >= %s AND "EVENT_DATE" < %s
        """

        cursor.execute(query, (start_date, end_date))

        conn.commit()
    except psycopg2.Error as e:
        print('Error:', e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def parse_matchup_sheet(start_date=None, end_date=None, export_debug_excels=True):  
    _refresh_env()
    standings_skipped = 0
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logs_dir = os.path.join(project_root, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    def excel_col_name(col_num):
        """Convert 1-based column number to Excel column letters."""
        name = ""
        while col_num > 0:
            col_num, remainder = divmod(col_num - 1, 26)
            name = chr(65 + remainder) + name
        return name

    def get_top8(df):
            if 'Player' in df.columns:
                return set(df.sort_values(by=['total_matches'], ascending=False)['Player'].iloc[:8])
            elif 'P1' in df.columns:
                return set(df.sort_values(by=['total_matches'], ascending=False)['P1'].iloc[:8])
    
    def get_event_info_dict():
        sheet_url = f'https://docs.google.com/spreadsheets/d/{gsheets[0]}/export?format=csv&gid={gsheets[4]}'
        df1 = pd.read_csv(sheet_url)
        raw_dates = df1['Date'].copy()
        parsed_dates = pd.to_datetime(raw_dates, format='mixed', errors='coerce')
        invalid_date_mask = (
            parsed_dates.isna()
            & raw_dates.notna()
            & (raw_dates.astype(str).str.strip() != '')
        )
        if invalid_date_mask.any():
            date_col_idx = df1.columns.get_loc('Date') + 1
            date_col_letter = excel_col_name(date_col_idx)
            bad_rows = df1.loc[invalid_date_mask].copy()
            bad_rows['BAD_DATE_RAW'] = raw_dates[invalid_date_mask].astype(str)
            bad_rows['SHEET_ROW'] = bad_rows.index + 2
            bad_rows['SHEET_CELL'] = bad_rows['SHEET_ROW'].apply(lambda r: f"{date_col_letter}{r}")
            if export_debug_excels:
                invalid_dates_export_path = os.path.join(logs_dir, f"invalid_standings_dates_{timestamp}.xlsx")
                with pd.ExcelWriter(invalid_dates_export_path, engine="openpyxl") as writer:
                    bad_rows.to_excel(writer, sheet_name="invalid_rows", index=False)
                    df1.to_excel(writer, sheet_name="raw_standings_table", index=False)
                print(
                    f"Warning: {invalid_date_mask.sum()} invalid standings Date value(s) detected. "
                    f"These rows will be ignored for standings/event matching. "
                    f"Details exported to {invalid_dates_export_path}"
                )
            else:
                print(
                    f"Warning: {invalid_date_mask.sum()} invalid standings Date value(s) detected. "
                    f"These rows will be ignored for standings/event matching."
                )
        df1['Date'] = parsed_dates
        df1 = df1[df1['Date'].notna()].copy()
        df1['total_matches'] = df1[['Wins', 'Losses']].fillna(0).sum(axis=1).astype(int)
        df1['Bye'] = df1['Bye'].fillna(0).astype(int)

        event_info_dict = {}

        breakpoints = []
        for index, row in df1[df1['Rank'] == 1].iterrows():
            breakpoints.append(index)
        breakpoints.append(len(df1))

        index = 0
        for i in breakpoints[1:]:
            event_date = df1.iloc[index:i].Date.max()
            if event_date not in event_info_dict:
                event_info_dict[event_date] = []
            event_info_dict[event_date].append(
                (df1.iloc[index:i].Type.max(), get_top8(df1.iloc[index:i]), df1.iloc[index:i][['Player','Bye','Rank']])
            )
            index = i
        return event_info_dict    

    def get_temp_id_starts():
        # EVENT_ID and MATCH_ID are temporary in-memory keys now; DB generates real IDs.
        return 12000000000, 11000000000

    def abstract_events(matches, df_events, format):
        nonlocal standings_skipped
        event_info_dict = get_event_info_dict()

        query = """
            SELECT *
            FROM "[vapi].VALID_EVENT_TYPES"
            WHERE "FORMAT" = %s
        """
        df_event_types = get_df(query,(format,))
        invalid_code = df_event_types.loc[(df_event_types['EVENT_TYPE'] == 'INVALID_TYPE'), 'EVENT_TYPE_ID'].iloc[0]

        df = df_events.copy()
        standings_parts = []
        used_entries_by_date = {}
        match_score_rows = []

        matches_grouped = matches.groupby(['EVENT_ID', 'P1']).agg({'MATCH_ID':'count'}).reset_index()
        matches_grouped = matches_grouped.rename(columns={'MATCH_ID':'total_matches'})

        # Debug export for abstract_events inputs.
        event_info_summary_rows = []
        event_info_standings_rows = []
        for event_date, entries in event_info_dict.items():
            for entry_index, entry in enumerate(entries, start=1):
                event_type = entry[0]
                top8_players = sorted(list(entry[1]))
                standings_df = entry[2].copy()
                standings_df["EVENT_DATE"] = event_date
                standings_df["ENTRY_INDEX"] = entry_index
                event_info_standings_rows.append(standings_df)
                event_info_summary_rows.append(
                    {
                        "EVENT_DATE": event_date,
                        "ENTRY_INDEX": entry_index,
                        "EVENT_TYPE": event_type,
                        "TOP8_COUNT": len(top8_players),
                        "TOP8_PLAYERS": ", ".join(top8_players),
                        "STANDINGS_ROWS": len(entry[2]),
                    }
                )
        event_info_summary_df = pd.DataFrame(event_info_summary_rows)
        event_info_standings_df = (
            pd.concat(event_info_standings_rows, ignore_index=True)
            if event_info_standings_rows
            else pd.DataFrame(columns=["EVENT_DATE", "ENTRY_INDEX", "Player", "Bye", "Rank"])
        )
        abstract_events_export_path = None
        if export_debug_excels:
            abstract_events_export_path = os.path.join(logs_dir, f"abstract_events_debug_{timestamp}.xlsx")
            with pd.ExcelWriter(abstract_events_export_path, engine="openpyxl") as writer:
                matches.to_excel(writer, sheet_name="matches_input", index=False)
                matches_grouped.to_excel(writer, sheet_name="matches_grouped", index=False)
                df.to_excel(writer, sheet_name="df_events_input", index=False)
                event_info_summary_df.to_excel(writer, sheet_name="event_info_summary", index=False)
                event_info_standings_df.to_excel(writer, sheet_name="event_info_standings", index=False)
            print(f"Exported abstract_events debug data to {abstract_events_export_path}")

        def normalize_players(series):
            return set(series.dropna().astype(str).str.strip().str.upper())

        for index, row in df.iterrows():
            if row['EVENT_DATE'] in event_info_dict:
                event_date = row['EVENT_DATE']
                event_id = row['EVENT_ID']
                entries_for_date = event_info_dict[event_date]
                used_entries = used_entries_by_date.setdefault(event_date, set())

                event_matches = matches[matches.EVENT_ID == event_id]
                # Matches are stored as two rows per match; use P1 only to represent unique participants.
                match_players = normalize_players(event_matches["P1"])

                best_idx = None
                best_entry = None
                best_overlap_count = -1
                best_percentage = -1.0

                for entry_idx, entry in enumerate(entries_for_date):
                    if entry_idx in used_entries:
                        continue

                    standings_players = normalize_players(entry[2]["Player"])
                    overlap_players = match_players.intersection(standings_players)
                    union_players = match_players.union(standings_players)
                    percentage_match = (
                        (len(overlap_players) / len(union_players)) * 100.0
                        if len(union_players) > 0
                        else 0.0
                    )

                    match_score_rows.append(
                        {
                            "EVENT_DATE": event_date,
                            "EVENT_ID": event_id,
                            "ENTRY_INDEX": entry_idx,
                            "EVENT_TYPE_CANDIDATE": entry[0],
                            "MATCH_PLAYERS_COUNT": len(match_players),
                            "STANDINGS_PLAYERS_COUNT": len(standings_players),
                            "OVERLAP_COUNT": len(overlap_players),
                            "PERCENTAGE_MATCH": round(percentage_match, 2),
                            "MATCH_PLAYERS_SAMPLE": ", ".join(sorted(list(match_players))[:8]),
                            "STANDINGS_PLAYERS_SAMPLE": ", ".join(sorted(list(standings_players))[:8]),
                        }
                    )

                    if (
                        percentage_match > best_percentage
                        or (
                            percentage_match == best_percentage
                            and len(overlap_players) > best_overlap_count
                        )
                    ):
                        best_idx = entry_idx
                        best_entry = entry
                        best_overlap_count = len(overlap_players)
                        best_percentage = percentage_match

                if best_entry is not None and best_overlap_count > 0:
                    df.loc[df['EVENT_ID'] == event_id, 'EVENT_TYPE'] = best_entry[0].upper()
                    standings_parts.append(best_entry[2].assign(EVENT_ID=event_id))
                    used_entries.add(best_idx)
                    print(
                        f"Matched EVENT_ID {int(event_id)} on {event_date.date()} "
                        f"to standings entry {best_idx} ({best_entry[0].upper()}) "
                        f"with percentage_match={best_percentage:.2f}% overlap={best_overlap_count}"
                    )
                else:
                    # If no overlap is found, skip the best remaining candidate's standings rows.
                    if best_entry is not None:
                        standings_skipped += len(best_entry[2])
                        print(
                            f"Skipping {len(best_entry[2])} standings rows for event {event_id} "
                            f"on {event_date.date()} due to no player overlap."
                        )
            
        df = pd.merge(left=df, right=df_event_types, left_on=['EVENT_TYPE'], right_on=['EVENT_TYPE'], how='left')

        df['EVENT_TYPE_ID'] = df['EVENT_TYPE_ID'].fillna(invalid_code)
        if standings_parts:
            df_standings = pd.concat(standings_parts, ignore_index=True)
        else:
            df_standings = pd.DataFrame(columns=['EVENT_ID', 'Player', 'Bye', 'Rank'])
        df_standings = df_standings.rename(columns={'Player':'P1', 'Bye':'BYES', 'Rank':'EVENT_RANK'})

        # Append score breakdown to debug workbook after matching pass.
        if match_score_rows:
            match_scores_df = pd.DataFrame(match_score_rows)
        else:
            match_scores_df = pd.DataFrame(
                columns=[
                    "EVENT_DATE", "EVENT_ID", "ENTRY_INDEX", "EVENT_TYPE_CANDIDATE",
                    "MATCH_PLAYERS_COUNT", "STANDINGS_PLAYERS_COUNT", "OVERLAP_COUNT",
                    "PERCENTAGE_MATCH", "MATCH_PLAYERS_SAMPLE", "STANDINGS_PLAYERS_SAMPLE"
                ]
            )
        if export_debug_excels and abstract_events_export_path:
            with pd.ExcelWriter(abstract_events_export_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                match_scores_df.to_excel(writer, sheet_name="event_match_scores", index=False)

        return (df[['EVENT_ID','EVENT_DATE','EVENT_TYPE_ID']], df_standings)

    def abstract_decks(df_matches, format):
        query = """
            SELECT *
            FROM "[vapi].VALID_DECKS"
            WHERE "FORMAT" = %s
        """

        df_decks = get_df(query,(format,))
        invalid_code = df_decks.loc[(df_decks['ARCHETYPE'] == 'NA') & (df_decks['SUBARCHETYPE'] == 'INVALID_NAME'), 'DECK_ID'].iloc[0]

        df = pd.merge(left=df_matches, right=df_decks, left_on=['P1_ARCH','P1_SUBARCH'], right_on=['ARCHETYPE','SUBARCHETYPE'], how='left')
        df.rename(columns={'DECK_ID':'P1_DECK_ID'}, inplace=True)

        df = pd.merge(left=df, right=df_decks, left_on=['P2_ARCH','P2_SUBARCH'], right_on=['ARCHETYPE','SUBARCHETYPE'], how='left')
        df.rename(columns={'DECK_ID':'P2_DECK_ID'}, inplace=True)

        df['P1_DECK_ID'] = df['P1_DECK_ID'].fillna(invalid_code)
        df['P2_DECK_ID'] = df['P2_DECK_ID'].fillna(invalid_code)

        df['P1_NOTE'] = df.apply(
            lambda row: "{}-{}: {}".format(row['P1_ARCH'], row['P1_SUBARCH'], row['P1_NOTE'])
            if row['P1_DECK_ID'] == invalid_code else row['P1_NOTE'], 
            axis=1
        )
        df['P2_NOTE'] = df.apply(
            lambda row: "{}-{}: {}".format(row['P2_ARCH'], row['P2_SUBARCH'], row['P2_NOTE'])
            if row['P2_DECK_ID'] == invalid_code else row['P2_NOTE'], 
            axis=1
        )

        return df[['MATCH_ID','P1','P2','P1_WINS','P2_WINS','MATCH_WINNER','P1_DECK_ID','P2_DECK_ID','P1_NOTE','P2_NOTE','EVENT_ID']]

    event_id_start, match_id_start = get_temp_id_starts()
    skipped_events_rej = []
    
    sheet_url = f'https://docs.google.com/spreadsheets/d/{gsheets[0]}/export?format=csv&gid={gsheets[2]}'
    df = pd.read_csv(sheet_url)
    # Drop rows that are completely blank to avoid cross-event contamination during forward-fill.
    fully_blank_mask = df.apply(
        lambda row: row.apply(
            lambda value: pd.isna(value) or (isinstance(value, str) and value.strip() == "")
        ).all(),
        axis=1,
    )
    fully_blank_count = int(fully_blank_mask.sum())
    if fully_blank_count > 0:
        df = df.loc[~fully_blank_mask].copy()
        print(f"Dropped {fully_blank_count} fully blank row(s) from source sheet.")
    if export_debug_excels:
        raw_export_path = os.path.join(logs_dir, f"matchup_raw_{timestamp}.xlsx")
        df.to_excel(raw_export_path, index=False)
        print(f"Exported raw matchup sheet to {raw_export_path}")

    # Full dataset size for (for Load Report).
    records_full_ds = df.shape[0]

    # Rename columns.
    df.columns = ['P1','P2','P1_WINS','P2_WINS','WINNER1','WINNER2','P1_ARCH','P2_ARCH','P1_SUBARCH','P2_SUBARCH','P1_NOTE','P2_NOTE','EVENT_DATE','EVENT_TYPE']

    # Replace null values with 'NA' string.
    df.fillna({'P1_ARCH':'NA','P2_ARCH':'NA','P1_SUBARCH':'NA','P2_SUBARCH':'NA','P1_NOTE':'NA','P2_NOTE':'NA'}, inplace=True)

    # Truncate player names longer than 30 characters.
    # df['P1'] = df['P1'].apply(lambda x: x[:30] if isinstance(x, str) and len(x) > 30 else x)

    # Format EVENT_DATE column and surface invalid source cells clearly.
    raw_event_dates = df['EVENT_DATE'].copy()
    raw_event_dates_stripped = raw_event_dates.astype(str).str.strip()
    blank_event_date_mask = raw_event_dates.isna() | (raw_event_dates_stripped == '')
    parsed_event_dates = pd.to_datetime(raw_event_dates, yearfirst=False, format='mixed', errors='coerce')
    invalid_event_date_mask = (
        parsed_event_dates.isna()
        & (~blank_event_date_mask)
    )
    if invalid_event_date_mask.any():
        event_date_col_idx = df.columns.get_loc('EVENT_DATE') + 1
        event_date_col_letter = excel_col_name(event_date_col_idx)
        bad_rows = df.loc[invalid_event_date_mask].copy()
        bad_rows['BAD_EVENT_DATE_RAW'] = raw_event_dates[invalid_event_date_mask].astype(str)
        bad_rows['SHEET_ROW'] = bad_rows.index + 2  # +2 accounts for header row in row 1.
        bad_rows['SHEET_CELL'] = bad_rows['SHEET_ROW'].apply(lambda r: f"{event_date_col_letter}{r}")
        if export_debug_excels:
            bad_dates_export_path = os.path.join(logs_dir, f"invalid_event_dates_{timestamp}.xlsx")
            bad_rows.to_excel(bad_dates_export_path, index=False)
            print(
                f"Warning: {invalid_event_date_mask.sum()} invalid EVENT_DATE value(s) detected. "
                f"These rows will be stored with NULL EVENT_DATE and NULL EVENT_ID in MATCHES. "
                f"Details exported to {bad_dates_export_path}"
            )
        else:
            print(
                f"Warning: {invalid_event_date_mask.sum()} invalid EVENT_DATE value(s) detected. "
                f"These rows will be stored with NULL EVENT_DATE and NULL EVENT_ID in MATCHES."
            )

    df['EVENT_DATE'] = parsed_event_dates

    # Forward-fill only truly blank dates from the sheet layout.
    ffilled_event_dates = df['EVENT_DATE'].ffill()
    df.loc[blank_event_date_mask, 'EVENT_DATE'] = ffilled_event_dates[blank_event_date_mask]

    # Add 7-14 day lag time in case data is updated/corrected soon after upload.
    if start_date is None:
        # start_date = datetime.today().date() - timedelta(days=14)
        start_date = datetime(2024, 8, 24).date()
        
    if end_date is None:
        # end_date = datetime.today().date() - timedelta(days=7)
        end_date = datetime.today().date() + timedelta(days=1)
        
    df = df[
        ((df['EVENT_DATE'] >= pd.to_datetime(start_date)) & (df['EVENT_DATE'] < pd.to_datetime(end_date)))
        | (df['EVENT_DATE'].isna())
    ]

    # Total records for (for Load Report).
    records_total = df.shape[0]

    # Adding Event_IDs.
    count = event_id_start
    df['EVENT_ID'] = 0
    for index, row in reversed(list(df.iterrows())):
        df.at[index,'EVENT_ID'] = count
        if pd.notna(row['EVENT_TYPE']):
            count += 1

    # Format EVENT_TYPE values.
    df['EVENT_TYPE'] = df['EVENT_TYPE'].str.upper().str.strip()

    # Handle empty EVENT_TYPE values by forward-filling.
    df['EVENT_TYPE'] = df['EVENT_TYPE'].ffill()

    # Ignore events with incomplete required match data and capture details.
    required_match_cols = ['P1', 'P2', 'P1_WINS', 'P2_WINS', 'WINNER1', 'WINNER2']
    incomplete_mask = df[required_match_cols].isna().any(axis=1)
    df_incomplete = df.loc[incomplete_mask].copy()
    if not df_incomplete.empty:
        missing_cols_per_row = df_incomplete[required_match_cols].isna().apply(
            lambda row: [col for col in required_match_cols if row[col]], axis=1
        )
        df_incomplete['MISSING_COLS'] = missing_cols_per_row.apply(lambda cols: ", ".join(cols))
        df_incomplete['SHEET_ROW'] = df_incomplete.index + 2  # +2 for header row in row 1
    else:
        df_incomplete['MISSING_COLS'] = []
        df_incomplete['SHEET_ROW'] = []

    df_skipped = df_incomplete.groupby(['EVENT_ID']).agg({'P1':'count', 'EVENT_DATE':'last'}).reset_index()
    events_to_ignore = df_skipped['EVENT_ID'].tolist()
    df = df[~df.EVENT_ID.isin(events_to_ignore)]

    # Adding skipped events to Event Rejections.
    for index, row in df_skipped.iterrows():
        event_details = df_incomplete[df_incomplete['EVENT_ID'] == row.EVENT_ID]
        missing_cols = sorted(
            {
                col.strip()
                for cols in event_details['MISSING_COLS'].tolist()
                for col in cols.split(',')
                if col.strip()
            }
        )
        sample_rows = ",".join(event_details['SHEET_ROW'].astype(int).astype(str).head(3).tolist())
        rej_text = (
            f"Incomplete event rows={sample_rows}; missing={','.join(missing_cols)}; "
            f"affected_rows={len(event_details)}"
        )[:100]
        skipped_events_rej.append((row.EVENT_ID, row.EVENT_DATE, None, None, 'E', rej_text))

    # Total events ignored (for Load Report).
    events_ignored = len(events_to_ignore)

    # Strip whitespace from player/deck names.
    df.P1 = df.P1.str.strip()
    df.P2 = df.P2.str.strip()
    df.P1_ARCH = df.P1_ARCH.str.strip().str.upper()
    df.P2_ARCH = df.P2_ARCH.str.strip().str.upper()
    df.P1_SUBARCH = df.P1_SUBARCH.str.strip().str.upper()
    df.P2_SUBARCH = df.P2_SUBARCH.str.strip().str.upper()
    df.P1_NOTE = df.P1_NOTE.str.strip().str.upper()
    df.P2_NOTE = df.P2_NOTE.str.strip().str.upper()

    # Filter out rows where data collectors recorded byes.
    df = df[df['P1'].str.upper() != 'BYE']
    df = df[df['P2'].str.upper() != 'BYE']

    # Format No Show deck name values.
    df.loc[df['P1_SUBARCH'].str.strip().str.upper() == 'NO SHOW', 'P1_SUBARCH'] = 'NO SHOW'
    df.loc[df['P2_SUBARCH'].str.strip().str.upper() == 'NO SHOW', 'P2_SUBARCH'] = 'NO SHOW'

    # Replace Winner1/2 columns with single Match_Winner column.
    df['MATCH_WINNER'] = df.apply(lambda row: 'P1' if ((row['WINNER1'] == 1) & (row['WINNER2'] == 0)) else ('P2' if ((row['WINNER1'] == 0) & (row['WINNER2'] == 1)) else 'NA'), axis=1)
    df.drop(columns=['WINNER1','WINNER2'],inplace=True)

    # Convert P1/P2_WINS from float to int.
    df['P1_WINS'] = df['P1_WINS'].fillna(0).astype(int)
    df['P2_WINS'] = df['P2_WINS'].fillna(0).astype(int)

    # Calculate MATCH_IDs for each pair of rows that apply to the same match.
    df['match_key'] = df.apply(lambda row: frozenset([row['P1'],row['P2'],row['EVENT_ID']]), axis=1)
    df = df.reset_index()
    df = df.sort_values(by=['match_key','index'])
    df["group_id"] = df.groupby(['match_key']).cumcount() // 2
    df["MATCH_ID"] = (df.groupby(['match_key','group_id']).ngroup() + match_id_start)
    df = df.sort_values(by=['index'])
    df = df.drop(columns=['match_key','group_id','index'])

    # Any event containing an invalid EVENT_DATE becomes standalone matches (EVENT_ID = NULL).
    invalid_event_ids = set(df.loc[df['EVENT_DATE'].isna(), 'EVENT_ID'].dropna().tolist())
    if invalid_event_ids:
        df.loc[df['EVENT_ID'].isin(invalid_event_ids), 'EVENT_ID'] = pd.NA

    # Abstract out Event info into its own table.
    df_events = df.groupby(['EVENT_ID','EVENT_DATE']).agg({'EVENT_TYPE':'last'}).reset_index()

    # Total records processed (for Load Report).
    records_proc = df.shape[0]

    df = abstract_decks(df,'VINTAGE')
    df_events, df_standings = abstract_events(df, df_events,'VINTAGE')

    summary_df = pd.DataFrame(
        {
            "metric": [
                "start_date",
                "end_date",
                "records_full_ds",
                "records_total",
                "events_ignored",
                "records_proc",
                "standings_skipped",
                "event_skipped_rejections_count",
            ],
            "value": [
                str(start_date),
                str(end_date),
                records_full_ds,
                records_total,
                events_ignored,
                records_proc,
                standings_skipped,
                len(skipped_events_rej),
            ],
        }
    )
    skipped_events_df = pd.DataFrame(
        skipped_events_rej,
        columns=["EVENT_ID", "EVENT_DATE", "EVENT_TYPE_ID", "PROC_DT", "REJ_TYPE", "EVENT_REJ_TEXT"],
    )
    incomplete_event_rows_df = df_incomplete[
        ['EVENT_ID', 'SHEET_ROW', 'MISSING_COLS', 'P1', 'P2', 'P1_WINS', 'P2_WINS', 'WINNER1', 'WINNER2', 'EVENT_DATE']
    ] if not df_incomplete.empty else pd.DataFrame(
        columns=['EVENT_ID', 'SHEET_ROW', 'MISSING_COLS', 'P1', 'P2', 'P1_WINS', 'P2_WINS', 'WINNER1', 'WINNER2', 'EVENT_DATE']
    )
    # Export ETL-stage data with explicit KEY naming to avoid confusion with DB-generated IDs.
    df_export = df.rename(columns={"MATCH_ID": "MATCH_KEY", "EVENT_ID": "EVENT_KEY"})
    df_events_export = df_events.rename(columns={"EVENT_ID": "EVENT_KEY"})
    df_standings_export = df_standings.rename(columns={"EVENT_ID": "EVENT_KEY"})

    if export_debug_excels:
        final_export_path = os.path.join(logs_dir, f"matchup_processed_{timestamp}.xlsx")
        with pd.ExcelWriter(final_export_path, engine="openpyxl") as writer:
            df_export.to_excel(writer, sheet_name="df_matches", index=False)
            df_events_export.to_excel(writer, sheet_name="df_events", index=False)
            df_standings_export.to_excel(writer, sheet_name="df_standings", index=False)
            summary_df.to_excel(writer, sheet_name="summary", index=False)
            skipped_events_df.to_excel(writer, sheet_name="event_skipped_rej", index=False)
            incomplete_event_rows_df.to_excel(writer, sheet_name="event_skip_details", index=False)
        print(f"Exported processed matchup outputs to {final_export_path}")

    return df, df_events, df_standings, [records_full_ds,records_total,events_ignored,records_proc], skipped_events_rej, standings_skipped

def match_insert(
    df_matches=None,
    df_events=None,
    df_standings=None,
    standings_skipped=0,
    start_date=None,
    end_date=None,
    export_debug_excels=True,
):
    _refresh_env()
    standings_skipped_parse = standings_skipped
    standings_skipped_insert = 0
    def check_and_append_match(condition, message, severity='E'):
        if condition:
            match_rej.append(
                (row.MATCH_ID, row.P1, row.P2, row.P1_WINS, row.P2_WINS, row.MATCH_WINNER, 
                row.P1_DECK_ID, row.P2_DECK_ID, row.P1_NOTE, row.P2_NOTE, row.EVENT_ID, 
                proc_dt, severity, message)
            )
            if severity == 'E':
                match_id_rej.add(row.MATCH_ID)
            elif severity == 'W':
                values_list.append((row.MATCH_ID, row.P1, row.P2, row.P1_WINS, row.P2_WINS, row.MATCH_WINNER, row.P1_DECK_ID, 
                    row.P2_DECK_ID, row.P1_NOTE, row.P2_NOTE, row.EVENT_ID, proc_dt))
            return True
        return False
    
    def check_and_append_event(condition, message, severity='E'):
        if condition:
            event_rej.append(
                (row.EVENT_ID, row.EVENT_DATE, row.EVENT_TYPE_ID, proc_dt, severity, message)
            )
            if severity == 'E':
                event_id_rej.add(row.EVENT_ID)
            elif severity == 'W':
                values_list.append((row.EVENT_ID, row.EVENT_DATE, row.EVENT_TYPE_ID, proc_dt))
            return True
        return False
    
    def check_and_append_standing(condition, message, severity='E'):
        if condition:
            if severity == 'E':
                pass
            elif severity == 'W':
                if len(row.P1) > 30:
                    values_list.append((row.EVENT_ID, row.P1[:30], row.BYES, row.EVENT_RANK, proc_dt))
                    standing_rej.append((row.EVENT_ID, row.P1[:30], row.BYES, row.EVENT_RANK, proc_dt, severity, message))
                    return True
                else:
                    values_list.append((row.EVENT_ID, row.P1, row.BYES, row.EVENT_RANK, proc_dt))
            standing_rej.append((row.EVENT_ID, row.P1, row.BYES, row.EVENT_RANK, proc_dt, severity, message))
            return True
        return False

    events_query = """
        INSERT INTO "[vapi].EVENTS" ("EVENT_DATE", "EVENT_TYPE_ID", "PROC_DT")
        VALUES (%s, %s, %s)
        RETURNING "EVENT_ID"
    """
    matches_query = """
        INSERT INTO "[vapi].MATCHES" ("P1", "P2", "P1_WINS", "P2_WINS", "MATCH_WINNER", "P1_DECK_ID", "P2_DECK_ID", "P1_NOTE", "P2_NOTE", "EVENT_ID", "PROC_DT")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    standings_query = """
        INSERT INTO "[vapi].EVENT_STANDINGS" ("EVENT_ID", "P1", "BYES", "EVENT_RANK", "PROC_DT")
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT ("EVENT_ID", "EVENT_RANK")
        DO NOTHING
    """

    matches_inserted = 0
    matches_skipped = 0
    events_inserted = 0
    events_skipped = 0
    standings_inserted = 0
    events_attempted = 0
    matches_attempted = 0
    standings_attempted = 0
    events_processed_input = len(df_events) if df_events is not None else 0
    matches_processed_input = len(df_matches) if df_matches is not None else 0
    standings_processed_input = len(df_standings) if df_standings is not None else 0
    # standings_skipped = 0
    event_rej = []
    match_rej = []
    standing_rej = []
    events_deleted = 0
    matches_deleted = 0
    standings_deleted = 0
    proc_dt = datetime.now()
    conn = None
    cursor = None
    event_id_rej = set()
    match_id_rej = set()
    event_id_map = {}
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

        proc_dt = datetime.now()
        
        # Delete events and matches from date range before re-inserting.
        try:
            # Get number of matches that will be deleted
            query1 = """
                SELECT COUNT(*) FROM "[vapi].MATCHES"
                WHERE "EVENT_ID" IS NULL
                   OR "EVENT_ID" IN (
                        SELECT "EVENT_ID" FROM "[vapi].EVENTS"
                        WHERE "EVENT_DATE" >= %s AND "EVENT_DATE" < %s
                   )
            """
            cursor.execute(query1, (start_date, end_date))
            matches_deleted = cursor.fetchone()[0]

            # Remove standalone matches (no event link) to avoid re-adding them every run.
            query1b = """
                DELETE FROM "[vapi].MATCHES"
                WHERE "EVENT_ID" IS NULL
            """
            cursor.execute(query1b)

            query2 = """
                SELECT COUNT(*) FROM "[vapi].EVENT_STANDINGS"
                WHERE "EVENT_ID" IN (
                    SELECT "EVENT_ID" FROM "[vapi].EVENTS"
                    WHERE "EVENT_DATE" >= %s AND "EVENT_DATE" < %s
                )
            """
            cursor.execute(query2, (start_date, end_date))
            standings_deleted = cursor.fetchone()[0]

            query3 = """
                DELETE FROM "[vapi].EVENTS"
                WHERE "EVENT_DATE" >= %s AND "EVENT_DATE" < %s
                RETURNING "EVENT_ID";
            """
            cursor.execute(query3, (start_date, end_date))
            events_deleted = len(cursor.fetchall())

        except Exception as e:
            print(f"Error deleting records: {e}")
            traceback.print_exc() 
            conn.rollback()
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            return [0,0,0,0,0,0,0,0,0,str(e),proc_dt],[],[],[]

        # Get NA Event_Type_ID and Deck_ID to use when checking business rules.
        try:
            query = """
                SELECT "EVENT_TYPE_ID"
                FROM "[vapi].VALID_EVENT_TYPES"
                WHERE "FORMAT" = %s AND "EVENT_TYPE" = %s
            """
            query2 = """
                SELECT "DECK_ID"
                FROM "[vapi].VALID_DECKS"
                WHERE "FORMAT" = %s AND "ARCHETYPE" = %s AND "SUBARCHETYPE" = %s
            """
            cursor.execute(query, ('VINTAGE', 'INVALID_TYPE'))
            row = cursor.fetchone()
            na_event_type_id = row[0] if row else None

            cursor.execute(query2, ('VINTAGE', 'NA', 'INVALID_NAME'))
            row = cursor.fetchone()
            na_deck_id = row[0] if row else None
        except Exception as e:
            print(f"Error fetching NA EVENT_TYPE/DECK_IDs: {e}")
            traceback.print_exc() 
            conn.rollback()
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            return [0,0,0,0,0,0,0,0,0,str(e),proc_dt],[],[],[]

        # Insert events.
        if df_events is not None:
            values_list = []  
            for row in df_events.itertuples(index=False):
                # Check business rules here.
                if any(
                    check_and_append_event(condition, message, severity)
                    for condition, message, severity in [
                        ((row.EVENT_TYPE_ID == na_event_type_id), 'EVENT_TYPE_ID not found in classification table.', 'W')
                    ]):
                    continue
                
                values_list.append((row.EVENT_ID, row.EVENT_DATE, row.EVENT_TYPE_ID, proc_dt))
            events_attempted = len(values_list)

            for values in values_list:
                # print(values)
                try:
                    temp_event_id = values[0]
                    cursor.execute(events_query, (values[1], values[2], values[3]))
                    inserted_event_id = cursor.fetchone()[0]
                    event_id_map[temp_event_id] = inserted_event_id
                    events_inserted += 1
                except Exception as e:
                    print(f"Error inserting row into EVENTS: {values} | Error: {e}")
                    events_skipped += 1
                    event_rej.append(values + ('E',str(e)))
                    continue
        def map_to_db_event_id(raw_event_id):
            if pd.isna(raw_event_id):
                return None
            return event_id_map.get(raw_event_id)

        # Insert matches.
        if df_matches is not None:
            values_list = []
            for row in df_matches.itertuples(index=False):
                # Check business rules here.
                if any(
                    check_and_append_match(condition, message, severity)
                    for condition, message, severity in [
                        ((row.P1_WINS > 2) or (row.P1_WINS < 0), 'P1_WINS out of range.', 'E'),
                        ((row.P2_WINS > 2) or (row.P2_WINS < 0), 'P2_WINS out of range.', 'E'),
                        ((row.P1_WINS == 2) and (row.MATCH_WINNER == 'P2'), 'P1_WINS = 2, but MATCH_WINNER = P2', 'E'),
                        ((row.P2_WINS == 2) and (row.MATCH_WINNER == 'P1'), 'P2_WINS = 2, but MATCH_WINNER = P1', 'E'),
                        ((row.MATCH_ID in match_id_rej), 'Inverted match record was rejected.', 'E'),
                        ((row.P1_DECK_ID == na_deck_id), 'P1_DECK_ID not found in classification table.', 'W'),
                        ((row.P2_DECK_ID == na_deck_id), 'P2_DECK_ID not found in classification table.', 'W')
                    ]):
                    continue

                if pd.isna(row.EVENT_ID):
                    mapped_event_id = None
                else:
                    mapped_event_id = event_id_map.get(row.EVENT_ID)
                if mapped_event_id is None and not pd.isna(row.EVENT_ID):
                    matches_skipped += 1
                    match_rej.append((row.MATCH_ID, row.P1, row.P2, row.P1_WINS, row.P2_WINS, row.MATCH_WINNER, row.P1_DECK_ID,
                        row.P2_DECK_ID, row.P1_NOTE, row.P2_NOTE, row.EVENT_ID, proc_dt, 'E', 'Missing mapped EVENT_ID'))
                    continue
                values_list.append((row.MATCH_ID, row.P1, row.P2, row.P1_WINS, row.P2_WINS, row.MATCH_WINNER, row.P1_DECK_ID, 
                    row.P2_DECK_ID, row.P1_NOTE, row.P2_NOTE, row.EVENT_ID, proc_dt))
            matches_attempted = len(values_list)

            for values in values_list:
                # print(values)
                try:
                    raw_event_id = values[10]
                    db_event_id = map_to_db_event_id(raw_event_id)
                    if db_event_id is None and not pd.isna(raw_event_id):
                        matches_skipped += 1
                        match_rej.append(values + ('E', 'Missing mapped EVENT_ID'))
                        continue

                    cursor.execute(
                        matches_query,
                        (
                            values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9], db_event_id, values[11]
                        ),
                    )
                    if cursor.rowcount == 0:
                        print(f"Skipped (duplicate): {values}")
                        matches_skipped += 1
                        match_rej.append(values + ('E', 'Duplicate'))
                    else:
                        matches_inserted += 1
                except Exception as e:
                    print(f"Error inserting row into MATCHES: {values} | Error: {e}")
                    matches_skipped += 1
                    match_rej.append(values + ('E', str(e)))
                    continue
        
        # Insert event standings.
        # print(df_standings)
        if df_standings is not None:
            values_list = []
            for row in df_standings.itertuples(index=False):
                # Check business rules here.
                if any(
                    check_and_append_standing(condition, message, severity)
                    for condition, message, severity in [
                        ((row.EVENT_RANK < 1), 'EVENT_RANK out of range.', 'E'),
                        ((row.EVENT_RANK > len(df_standings)), 'EVENT_RANK out of range.', 'E'),
                        ((len(row.P1) > 30), 'P1 value greater than 30 characters.', 'W')
                    ]):
                    continue

                mapped_event_id = event_id_map.get(row.EVENT_ID)
                if mapped_event_id is None:
                    standings_skipped_insert += 1
                    standing_rej.append((row.EVENT_ID, row.P1, row.BYES, row.EVENT_RANK, proc_dt, 'E', 'Missing mapped EVENT_ID'))
                    continue
                values_list.append((row.EVENT_ID, row.P1, row.BYES, row.EVENT_RANK, proc_dt))
            standings_attempted = len(values_list)
            for values in values_list:
                # print(values)
                try:
                    raw_event_id = values[0]
                    db_event_id = map_to_db_event_id(raw_event_id)
                    if db_event_id is None and not pd.isna(raw_event_id):
                        standings_skipped_insert += 1
                        standing_rej.append(values + ('E', 'Missing mapped EVENT_ID'))
                        continue

                    cursor.execute(standings_query, (db_event_id, values[1], values[2], values[3], values[4]))
                    if cursor.rowcount == 0:
                        print(f"Skipped (duplicate): {values}")
                        standings_skipped_insert += 1
                        standing_rej.append(values + ('E', 'Duplicate'))
                    else:
                        standings_inserted += 1
                except Exception as e:
                    print(f"Error inserting row into EVENT_STANDINGS: {values} | Error: {e}")
                    standings_skipped_insert += 1
                    standing_rej.append(values + ('E', str(e)))
                    continue

        # Export DB-mapped IDs for easier reconciliation against inserted data.
        if export_debug_excels:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            logs_dir = os.path.join(project_root, "logs")
            os.makedirs(logs_dir, exist_ok=True)
            mapped_export_path = os.path.join(logs_dir, f"match_insert_db_mapped_{proc_dt.strftime('%Y%m%d_%H%M%S')}.xlsx")
            mapped_matches_df = df_matches.copy() if df_matches is not None else pd.DataFrame()
            mapped_events_df = df_events.copy() if df_events is not None else pd.DataFrame()
            mapped_standings_df = df_standings.copy() if df_standings is not None else pd.DataFrame()
            if not mapped_matches_df.empty:
                mapped_matches_df["EVENT_ID_DB"] = mapped_matches_df["EVENT_ID"].apply(map_to_db_event_id)
            if not mapped_events_df.empty:
                mapped_events_df["EVENT_ID_DB"] = mapped_events_df["EVENT_ID"].apply(map_to_db_event_id)
            if not mapped_standings_df.empty:
                mapped_standings_df["EVENT_ID_DB"] = mapped_standings_df["EVENT_ID"].apply(map_to_db_event_id)
            with pd.ExcelWriter(mapped_export_path, engine="openpyxl") as writer:
                mapped_matches_df.to_excel(writer, sheet_name="df_matches_mapped", index=False)
                mapped_events_df.to_excel(writer, sheet_name="df_events_mapped", index=False)
                mapped_standings_df.to_excel(writer, sheet_name="df_standings_mapped", index=False)
            print(f"Exported DB-mapped IDs to {mapped_export_path}")

        standings_skipped_total = standings_skipped_parse + standings_skipped_insert
        print("Insertion stats:")
        print(
            f"  EVENTS    processed={events_processed_input} attempted={events_attempted} "
            f"inserted={events_inserted} skipped={events_skipped}"
        )
        print(
            f"  MATCHES   processed={matches_processed_input} attempted={matches_attempted} "
            f"inserted={matches_inserted} skipped={matches_skipped}"
        )
        print(
            f"  STANDINGS processed={standings_processed_input} attempted={standings_attempted} "
            f"inserted={standings_inserted} skipped_parse={standings_skipped_parse} "
            f"skipped_insert={standings_skipped_insert} skipped_total={standings_skipped_total}"
        )

        conn.commit()
    except Exception as e:
        print(f"Database connection error: {e}")
        traceback.print_exc() 
        if conn:
            conn.rollback()
        return [0,0,0,0,0,0,0,0,0,str(e),proc_dt],[],[],[]
    finally:
        if conn:
            if cursor:
                cursor.close()
            conn.close()
    standings_skipped_total = standings_skipped_parse + standings_skipped_insert
    return [matches_deleted, matches_inserted, matches_skipped, events_deleted, events_inserted, events_skipped, 
            standings_deleted, standings_inserted, standings_skipped_total, None, proc_dt], event_rej, match_rej, standing_rej

def insert_load_stats(load_report,event_rej,match_rej,standing_rej):
    _refresh_env()
    load_report_query = """
        INSERT INTO "[vapi].LOAD_REPORTS" ("START_DATE", "END_DATE", "RECORDS_FULL_DS", "RECORDS_TOTAL", "EVENTS_IGNORED", "RECORDS_PROC",
            "MATCHES_DELETED", "MATCHES_INSERTED", "MATCHES_SKIPPED", "EVENTS_DELETED", "EVENTS_INSERTED", "EVENTS_SKIPPED", 
            "STANDINGS_DELETED", "STANDINGS_INSERTED", "STANDINGS_SKIPPED", "DB_CONN_ERROR_TEXT", "PROC_DT")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING "LOAD_RPT_ID"
    """
    event_rej_query = """
        INSERT INTO "[vapi].EVENT_REJECTIONS" ("LOAD_RPT_ID", "EVENT_ID", "EVENT_DATE", "EVENT_TYPE_ID", "PROC_DT", "REJ_TYPE", "EVENT_REJ_TEXT")
        SELECT %s, %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM "[vapi].EVENT_REJECTIONS" er
            WHERE er."LOAD_RPT_ID" IS NOT DISTINCT FROM %s
              AND er."EVENT_ID" IS NOT DISTINCT FROM %s
              AND er."EVENT_DATE" IS NOT DISTINCT FROM %s
              AND er."EVENT_TYPE_ID" IS NOT DISTINCT FROM %s
              AND er."PROC_DT" IS NOT DISTINCT FROM %s
              AND er."REJ_TYPE" IS NOT DISTINCT FROM %s
              AND er."EVENT_REJ_TEXT" IS NOT DISTINCT FROM %s
        )
    """
    match_rej_query = """
        INSERT INTO "[vapi].MATCH_REJECTIONS" ("LOAD_RPT_ID", "MATCH_ID", "P1", "P2", "P1_WINS", "P2_WINS", "MATCH_WINNER", "P1_DECK_ID",
            "P2_DECK_ID", "P1_NOTE", "P2_NOTE", "EVENT_ID", "PROC_DT", "REJ_TYPE", "MATCH_REJ_TEXT")
        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM "[vapi].MATCH_REJECTIONS" mr
            WHERE mr."LOAD_RPT_ID" IS NOT DISTINCT FROM %s
              AND mr."MATCH_ID" IS NOT DISTINCT FROM %s
              AND mr."P1" IS NOT DISTINCT FROM %s
              AND mr."P2" IS NOT DISTINCT FROM %s
              AND mr."P1_WINS" IS NOT DISTINCT FROM %s
              AND mr."P2_WINS" IS NOT DISTINCT FROM %s
              AND mr."MATCH_WINNER" IS NOT DISTINCT FROM %s
              AND mr."P1_DECK_ID" IS NOT DISTINCT FROM %s
              AND mr."P2_DECK_ID" IS NOT DISTINCT FROM %s
              AND mr."P1_NOTE" IS NOT DISTINCT FROM %s
              AND mr."P2_NOTE" IS NOT DISTINCT FROM %s
              AND mr."EVENT_ID" IS NOT DISTINCT FROM %s
              AND mr."PROC_DT" IS NOT DISTINCT FROM %s
              AND mr."REJ_TYPE" IS NOT DISTINCT FROM %s
              AND mr."MATCH_REJ_TEXT" IS NOT DISTINCT FROM %s
        )
    """
    standing_rej_query = """
        INSERT INTO "[vapi].RANK_REJECTIONS" ("LOAD_RPT_ID", "EVENT_ID", "P1", "BYES", "EVENT_RANK", "PROC_DT", "REJ_TYPE", "RANK_REJ_TEXT")
        SELECT %s, %s, %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM "[vapi].RANK_REJECTIONS" rr
            WHERE rr."LOAD_RPT_ID" IS NOT DISTINCT FROM %s
              AND rr."EVENT_ID" IS NOT DISTINCT FROM %s
              AND rr."P1" IS NOT DISTINCT FROM %s
              AND rr."BYES" IS NOT DISTINCT FROM %s
              AND rr."EVENT_RANK" IS NOT DISTINCT FROM %s
              AND rr."PROC_DT" IS NOT DISTINCT FROM %s
              AND rr."REJ_TYPE" IS NOT DISTINCT FROM %s
              AND rr."RANK_REJ_TEXT" IS NOT DISTINCT FROM %s
        )
    """
    event_count = 0
    match_count = 0
    standing_count = 0
    load_rpt_id = 0
    conn = None
    cursor = None
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

        # Insert load report first; dependent rejection rows require LOAD_RPT_ID.
        try:
            cursor.execute(load_report_query, tuple(load_report))
            load_rpt_id = cursor.fetchone()[0]
        except Exception as e:
            raise RuntimeError(
                f"Error inserting row into LOAD_REPORTS: {load_report} | Error: {e}"
            ) from e

        # Insert Event_Rejection.
        print(f'Inserting Event Rejections: {len(event_rej)} row(s)')
        for values in event_rej:
            try:
                cursor.execute(event_rej_query, (load_rpt_id,) + values + (load_rpt_id,) + values)
                if cursor.rowcount > 0:
                    event_count += 1
            except Exception as e:
                print(f"Error inserting row into EVENT_REJECTIONS: {(load_rpt_id,) + values} | Error: {e}")
                continue

        # Insert Match_Rejection.
        print(f'Inserting Match Rejections: {len(match_rej)} row(s)')
        for values in match_rej:
            try:
                cursor.execute(match_rej_query, (load_rpt_id,) + values + (load_rpt_id,) + values)
                if cursor.rowcount > 0:
                    match_count += 1
            except Exception as e:
                print(f"Error inserting row into MATCH_REJECTIONS: {(load_rpt_id,) + values} | Error: {e}")
                continue

        # Insert Standing_Rejection.
        print(f'Inserting Rank Rejections: {len(standing_rej)} row(s)')
        for values in standing_rej:
            try:
                cursor.execute(standing_rej_query, (load_rpt_id,) + values + (load_rpt_id,) + values)
                if cursor.rowcount > 0:
                    standing_count += 1
            except Exception as e:
                print(f"Error inserting row into RANK_REJECTIONS: {(load_rpt_id,) + values} | Error: {e}")
                continue

        conn.commit()
        print(
            f"Load stats committed. load_rpt_id={load_rpt_id}, "
            f"event_rejections_inserted={event_count}, "
            f"match_rejections_inserted={match_count}, "
            f"rank_rejections_inserted={standing_count}"
        )
    except Exception as e:
        print(f"Database connection error: {e}")
        traceback.print_exc() 
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def test(df_matches, df_events):
    # Should have 8 Match Rejections (should not be loaded):
    #   P1_WINS out of range. : 3
    #   P2_WINS out of range. : 3
    #   P1_WINS = 2, but MATCH_WINNER = P2 : 1
    #   P2_WINS = 2, but MATCH_WINNER = P1 : 1
    #   P1_DECK_ID not found in classification table. : 1 (should still get loaded)
    #   P1_DECK_ID not found in classification table. : 1 (should still get loaded)
    #
    #   EVENT_TYPE_ID not found in classification table. : 1 (should still get loaded)

    # Checking if >2 game wins are handled correctly.
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000000) & (df_matches['P1'] == 'ScreenwriterNY'), 'P1_WINS'] = 4
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000000) & (df_matches['P2'] == 'ScreenwriterNY'), 'P2_WINS'] = 4
    # Checking if incorrect match winner is handled correctly.
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000001) & (df_matches['P1'] == 'ScreenwriterNY'), 'MATCH_WINNER'] = 'P2'
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000001) & (df_matches['P2'] == 'ScreenwriterNY'), 'MATCH_WINNER'] = 'P1'
    # Checking if missing DECK_ID is handled correctly.
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000002) & (df_matches['P1'] == '_Shatun_'), 'P1_DECK_ID'] = 13000000033
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000002) & (df_matches['P2'] == '_Shatun_'), 'P2_DECK_ID'] = 13000000033
    # Checking if <0 game wins are handled correctly.
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000003) & (df_matches['P1'] == 'ScreenwriterNY'), 'P2_WINS'] = -1
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000003) & (df_matches['P2'] == 'ScreenwriterNY'), 'P1_WINS'] = -1
    # Checking if multiple match errors are handled correctly.
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000004) & (df_matches['P1'] == 'ScreenwriterNY'), 'P1_WINS'] = 4
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000004) & (df_matches['P2'] == 'ScreenwriterNY'), 'P2_WINS'] = 4
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000004) & (df_matches['P1'] == 'ScreenwriterNY'), 'MATCH_WINNER'] = 'P2'
    df_matches.loc[(df_matches['MATCH_ID'] == 11000000004) & (df_matches['P2'] == 'ScreenwriterNY'), 'MATCH_WINNER'] = 'P1'

    # Checking if missing EVENT_ID is handled correctly.
    df_events.loc[(df_events['EVENT_ID'] == 12000000000), 'EVENT_TYPE_ID'] = 14000000005