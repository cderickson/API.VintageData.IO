import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import warnings
import traceback
import os
from dotenv import load_dotenv

load_dotenv()
credentials = [os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_NAME")]
gsheets = [os.getenv("VINTAGE_SHEET_CURR"), os.getenv("VINTAGE_SHEET_ARCHIVE"), os.getenv("VINTAGE_GID_MATCHES"), os.getenv("VINTAGE_GID_DECK"), os.getenv('VINTAGE_GID_STANDINGS')]

sheet_curr = '1wxR3iYna86qrdViwHjUPzHuw6bCNeMLb72M25hpUHYk'
sheet_archive = '1PxNYGMXaVrRqI0uyMQF46K7nDEG16WnDoKrFyI_qrvE'
gid_matches = '2141931777'
gid_deck = '590005429'
gid_standings = '1693401931'

warnings.filterwarnings('ignore', category=UserWarning, message="pandas only supports SQLAlchemy connectable")

# MATCH_ID      = 11000000000
# EVENT_ID      = 12000000000
# DECK_ID       = 13000000000
# EVENT_TYPE_ID = 14000000000
# LOAD_RPT_ID   = 15000000000
# EV_REJ_ID     = 16000000000
# MATCH_REJ_ID  = 17000000000

def read_credentials():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    with open("credentials.txt", "r") as file:
        return [line.strip() for line in file]
    
def get_df(query, vars=()):
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
            DELETE FROM "EVENTS"
            WHERE "EVENT_DATE" >= %s AND "EVENT_DATE" < %s
        """

        cursor.execute(query, (start_date, end_date))

        conn.commit()
    except psycopg2.Error as e:
        print('Error:', e)
    finally:
        if conn:
            cursor.close()
            conn.close()

def parse_matchup_sheet(start_date=None,end_date=None):  
    def get_top8(df):
            if 'Player' in df.columns:
                return set(df.sort_values(by=['total_matches'], ascending=False)['Player'].iloc[:8])
            elif 'P1' in df.columns:
                return set(df.sort_values(by=['total_matches'], ascending=False)['P1'].iloc[:8])
    
    def get_event_info_dict():
        sheet_url = f'https://docs.google.com/spreadsheets/d/{gsheets[0]}/export?format=csv&gid={gsheets[4]}'
        df1 = pd.read_csv(sheet_url)
        df1['Date'] = pd.to_datetime(df1['Date'], format='%m/%d/%y')
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

    def get_max_id():
        conn = psycopg2.connect(
            host=credentials[0],
            port=credentials[1],
            user=credentials[2],
            password=credentials[3],
            database=credentials[4],
            sslmode='require'
        )

        cursor = conn.cursor()

        query1 = """
            SELECT max("EVENT_ID")
            FROM "EVENTS"
        """
        query2 = """
            SELECT max("MATCH_ID")
            FROM "MATCHES"
        """

        cursor.execute(query1)
        max_event_id = cursor.fetchone()[0] or 12000000000
        if max_event_id != 12000000000:
            max_event_id += 1

        cursor.execute(query2)
        max_match_id = cursor.fetchone()[0] or 11000000000
        if max_match_id != 11000000000:
            max_match_id += 1

        conn.close()
        return max_event_id, max_match_id

    def abstract_events(matches, df_events, format):
        event_info_dict = get_event_info_dict()

        query = """
            SELECT *
            FROM "VALID_EVENT_TYPES"
            WHERE "FORMAT" = %s
        """
        df_event_types = get_df(query,(format,))
        invalid_code = df_event_types.loc[(df_event_types['EVENT_TYPE'] == 'INVALID_TYPE'), 'EVENT_TYPE_ID'].iloc[0]

        df = df_events.copy()
        df_standings = pd.DataFrame(columns=['EVENT_ID','Player','Bye','Rank'])

        matches_grouped = matches.groupby(['EVENT_ID', 'P1']).agg({'MATCH_ID':'count'}).reset_index()
        matches_grouped = matches_grouped.rename(columns={'MATCH_ID':'total_matches'})

        for index, row in df.iterrows():
            top8 = get_top8(matches_grouped[(matches_grouped.EVENT_ID == row['EVENT_ID'])])
            if row['EVENT_DATE'] in event_info_dict:
                for index,i in enumerate(event_info_dict[row['EVENT_DATE']]):
                    if (len(i[1].intersection(top8)) >= 6):
                        df.loc[df['EVENT_ID'] == row['EVENT_ID'], 'EVENT_TYPE'] = i[0].upper()
                        print('Overwriting ' + str(row['EVENT_ID']) + ': ' + i[0].upper())
                        df_standings = pd.concat([df_standings, i[2].assign(EVENT_ID=row['EVENT_ID'])], ignore_index=True)
                        print('Adding ranks: ' + str(row['EVENT_ID']))
                        break
                    if (index == len(event_info_dict[row['EVENT_DATE']]) - 1):
                        event_standings_skipped += len(i[2])
            
        df = pd.merge(left=df, right=df_event_types, left_on=['EVENT_TYPE'], right_on=['EVENT_TYPE'], how='left')

        df['EVENT_TYPE_ID'] = df['EVENT_TYPE_ID'].fillna(invalid_code)
        df_standings = df_standings.rename(columns={'Player':'P1', 'Bye':'BYES', 'Rank':'EVENT_RANK'})

        return (df[['EVENT_ID','EVENT_DATE','EVENT_TYPE_ID']], df_standings)

    def abstract_decks(df_matches, format):
        query = """
            SELECT *
            FROM "VALID_DECKS"
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

    event_id_start, match_id_start = get_max_id()
    skipped_events_rej = []
    
    sheet_url = f'https://docs.google.com/spreadsheets/d/{gsheets[0]}/export?format=csv&gid={gsheets[2]}'
    df = pd.read_csv(sheet_url)

    # Full dataset size for (for Load Report).
    records_full_ds = df.shape[0]

    # Rename columns.
    df.columns = ['P1','P2','P1_WINS','P2_WINS','WINNER1','WINNER2','P1_ARCH','P2_ARCH','P1_SUBARCH','P2_SUBARCH','P1_NOTE','P2_NOTE','EVENT_DATE','EVENT_TYPE']

    # Replace null values with 'NA' string.
    df.fillna({'P1_ARCH':'NA','P2_ARCH':'NA','P1_SUBARCH':'NA','P2_SUBARCH':'NA','P1_NOTE':'NA','P2_NOTE':'NA'}, inplace=True)

    # Truncate player names longer than 30 characters.
    # df['P1'] = df['P1'].apply(lambda x: x[:30] if isinstance(x, str) and len(x) > 30 else x)

    # Format EVENT_DATE column.
    df['EVENT_DATE'] = pd.to_datetime(df['EVENT_DATE'], yearfirst=False, format='mixed')

    # Handle empty EVENT_DATE values by forward-filling.
    df['EVENT_DATE'] = df['EVENT_DATE'].ffill()

    # Add 7-14 day lag time in case data is updated/corrected soon after upload.
    if start_date is None:
        # start_date = datetime.today().date() - timedelta(days=14)
        start_date = datetime(2024, 8, 24).date()
        
    if end_date is None:
        # end_date = datetime.today().date() - timedelta(days=7)
        end_date = datetime.today().date() + timedelta(days=1)
        
    df = df[(df['EVENT_DATE'] >= pd.to_datetime(start_date)) & (df['EVENT_DATE'] < pd.to_datetime(end_date))]

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

    # Ignore events with incomplete data.
    df_skipped = df[df['P1_WINS'].isnull()].groupby(['EVENT_ID']).agg({'P1':'count', 'EVENT_DATE':'last'}).reset_index()
    events_to_ignore = df_skipped['EVENT_ID'].tolist()
    df = df[~df.EVENT_ID.isin(events_to_ignore)]

    # Adding skipped events to Event Rejections.
    for index, row in df_skipped.iterrows():
        skipped_events_rej.append((row.EVENT_ID, row.EVENT_DATE, None, None, 'E', 'Event contains incomplete match data.'))

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
    for index, row in df.iterrows():
        if row['P1_SUBARCH'].strip().upper() == 'NO SHOW':
            df.at[index,'P1_SUBARCH'] = 'NO SHOW'
        if row['P2_SUBARCH'].strip().upper() == 'NO SHOW':
            df.at[index,'P2_SUBARCH'] = 'NO SHOW'

    # Replace Winner1/2 columns with single Match_Winner column.
    df['MATCH_WINNER'] = df.apply(lambda row: 'P1' if ((row['WINNER1'] == 1) & (row['WINNER2'] == 0)) else ('P2' if ((row['WINNER1'] == 0) & (row['WINNER2'] == 1)) else 'NA'), axis=1)
    df.drop(columns=['WINNER1','WINNER2'],inplace=True)

    # Convert P1/P2_WINS from float to int.
    df['P1_WINS'] = df['P1_WINS'].fillna(0).astype(int)
    df['P2_WINS'] = df['P2_WINS'].fillna(0).astype(int)

    # Abstract out Event info into its own table.
    df_events = df.groupby(['EVENT_ID','EVENT_DATE']).agg({'EVENT_TYPE':'last'}).reset_index()

    # Calculate MATCH_IDs for each pair of rows that apply to the same match.
    df['match_key'] = df.apply(lambda row: frozenset([row['P1'],row['P2'],row['EVENT_ID']]), axis=1)
    df = df.reset_index()
    df = df.sort_values(by=['match_key','index'])
    df["group_id"] = df.groupby(['match_key']).cumcount() // 2
    df["MATCH_ID"] = (df.groupby(['match_key','group_id']).ngroup() + match_id_start)
    df = df.sort_values(by=['index'])
    df = df.drop(columns=['match_key','group_id','index'])

    # Total records processed (for Load Report).
    records_proc = df.shape[0]

    standings_skipped = 0

    df = abstract_decks(df,'VINTAGE')
    df_events, df_standings = abstract_events(df, df_events,'VINTAGE')

    return df, df_events, df_standings, [records_full_ds,records_total,events_ignored,records_proc], skipped_events_rej, standings_skipped

def match_insert(df_matches=None, df_events=None, df_standings=None, standings_skipped=0, start_date=None, end_date=None):
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
        INSERT INTO "EVENTS" ("EVENT_ID", "EVENT_DATE", "EVENT_TYPE_ID", "PROC_DT")
        VALUES (%s, %s, %s, %s)
        ON CONFLICT ("EVENT_ID")
        DO NOTHING
    """
    matches_query = """
        INSERT INTO "MATCHES" ("MATCH_ID", "P1", "P2", "P1_WINS", "P2_WINS", "MATCH_WINNER", "P1_DECK_ID", "P2_DECK_ID", "P1_NOTE", "P2_NOTE", "EVENT_ID", "PROC_DT")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("MATCH_ID", "P1") 
        DO NOTHING
    """
    standings_query = """
        INSERT INTO "EVENT_STANDINGS" ("EVENT_ID", "P1", "BYES", "EVENT_RANK", "PROC_DT")
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT ("EVENT_ID", "EVENT_RANK")
        DO NOTHING
    """

    matches_inserted = 0
    matches_skipped = 0
    events_inserted = 0
    events_skipped = 0
    standings_inserted = 0
    # standings_skipped = 0
    event_rej = []
    match_rej = []
    standing_rej = []
    events_deleted = 0
    matches_deleted = 0
    event_id_rej = set()
    match_id_rej = set()
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
                SELECT COUNT(*) FROM "MATCHES"
                WHERE "EVENT_ID" IN (
                    SELECT "EVENT_ID" FROM "EVENTS"
                    WHERE "EVENT_DATE" >= %s AND "EVENT_DATE" < %s
                )
            """
            cursor.execute(query1, (start_date, end_date))
            matches_deleted = cursor.fetchone()[0]

            query2 = """
                SELECT COUNT(*) FROM "EVENT_STANDINGS"
                WHERE "EVENT_ID" IN (
                    SELECT "EVENT_ID" FROM "EVENTS"
                    WHERE "EVENT_DATE" >= %s AND "EVENT_DATE" < %s
                )
            """
            cursor.execute(query2, (start_date, end_date))
            standings_deleted = cursor.fetchone()[0]

            query3 = """
                DELETE FROM "EVENTS"
                WHERE "EVENT_DATE" >= %s AND "EVENT_DATE" < %s
                RETURNING "EVENT_ID";
            """
            cursor.execute(query3, (start_date, end_date))
            events_deleted = len(cursor.fetchall())

        except Exception as e:
            print(f"Error deleting records: {e}")
            traceback.print_exc() 
            conn.rollback()
            if conn:
                cursor.close()
                conn.close()
            return [0,0,0,0,0,0,str(e),proc_dt],[],[]

        # Get NA Event_Type_ID and Deck_ID to use when checking business rules.
        try:
            query = """
                SELECT "EVENT_TYPE_ID"
                FROM "VALID_EVENT_TYPES"
                WHERE "FORMAT" = %s AND "EVENT_TYPE" = %s
            """
            query2 = """
                SELECT "DECK_ID"
                FROM "VALID_DECKS"
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
            if conn:
                cursor.close()
                conn.close()
            return [0,0,0,0,0,0,str(e),proc_dt],[],[]

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

            for values in values_list:
                # print(values)
                try:
                    cursor.execute(events_query, values)
                    if cursor.rowcount == 0:
                        print(f"Skipped (duplicate): {values}")
                        events_skipped += 1
                        event_rej.append(values + ('E','Duplicate'))
                    else:
                        events_inserted += 1
                except Exception as e:
                    print(f"Error inserting row into EVENTS: {values} | Error: {e}")
                    events_skipped += 1
                    event_rej.append(values + ('E',str(e)))
                    continue

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

                values_list.append((row.MATCH_ID, row.P1, row.P2, row.P1_WINS, row.P2_WINS, row.MATCH_WINNER, row.P1_DECK_ID, 
                    row.P2_DECK_ID, row.P1_NOTE, row.P2_NOTE, row.EVENT_ID, proc_dt))

            for values in values_list:
                # print(values)
                try:
                    cursor.execute(matches_query, values)
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

                values_list.append((row.EVENT_ID, row.P1, row.BYES, row.EVENT_RANK, proc_dt))
            for values in values_list:
                # print(values)
                try:
                    cursor.execute(standings_query, values)
                    if cursor.rowcount == 0:
                        print(f"Skipped (duplicate): {values}")
                        standings_skipped += 1
                        standing_rej.append(values + ('E', 'Duplicate'))
                    else:
                        standings_inserted += 1
                except Exception as e:
                    print(f"Error inserting row into EVENT_STANDINGS: {values} | Error: {e}")
                    standings_skipped += 1
                    standing_rej.append(values + ('E', str(e)))
                    continue

        conn.commit()
    except Exception as e:
        print(f"Database connection error: {e}")
        traceback.print_exc() 
        conn.rollback()
        return [0,0,0,0,0,0,str(e),proc_dt],[],[]
    finally:
        if conn:
            cursor.close()
            conn.close()
        return [matches_deleted, matches_inserted, matches_skipped, events_deleted, events_inserted, events_skipped, 
                standings_deleted, standings_inserted, standings_skipped, None, proc_dt], event_rej, match_rej, standing_rej

def insert_load_stats(load_report,event_rej,match_rej,standing_rej):
    load_report_query = """
        INSERT INTO "LOAD_REPORTS" ("START_DATE", "END_DATE", "RECORDS_FULL_DS", "RECORDS_TOTAL", "EVENTS_IGNORED", "RECORDS_PROC",
            "MATCHES_DELETED", "MATCHES_INSERTED", "MATCHES_SKIPPED", "EVENTS_DELETED", "EVENTS_INSERTED", "EVENTS_SKIPPED", 
            "STANDINGS_DELETED", "STANDINGS_INSERTED", "STANDINGS_SKIPPED", "DB_CONN_ERROR_TEXT", "PROC_DT")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING "LOAD_RPT_ID"
    """
    event_rej_query = """
        INSERT INTO "EVENT_REJECTIONS" ("LOAD_RPT_ID", "EVENT_ID", "EVENT_DATE", "EVENT_TYPE_ID", "PROC_DT", "REJ_TYPE", "EVENT_REJ_TEXT")
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    match_rej_query = """
        INSERT INTO "MATCH_REJECTIONS" ("LOAD_RPT_ID", "MATCH_ID", "P1", "P2", "P1_WINS", "P2_WINS", "MATCH_WINNER", "P1_DECK_ID",
            "P2_DECK_ID", "P1_NOTE", "P2_NOTE", "EVENT_ID", "PROC_DT", "REJ_TYPE", "MATCH_REJ_TEXT")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    standing_rej_query = """
        INSERT INTO "RANK_REJECTIONS" ("LOAD_RPT_ID", "EVENT_ID", "P1", "BYES", "EVENT_RANK", "PROC_DT", "REJ_TYPE", "RANK_REJ_TEXT")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    event_count = 0
    match_count = 0
    load_rpt_id = 0
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

        # print(f"Load Report: {load_report}")
        # Insert load report.
        try:
            cursor.execute(load_report_query, tuple(load_report))
            load_rpt_id = cursor.fetchone()[0]
        except Exception as e:
            print(f"Error inserting row into LOAD_REPORTS: {load_report} | Error: {e}")

        # Insert Event_Rejection.
        print('Printing Event Rejections:')
        for values in event_rej:
            print(values)
            try:
                cursor.execute(event_rej_query, (load_rpt_id,) + values)
                event_count += 1
            except Exception as e:
                print(f"Error inserting row into EVENT_REJECTIONS: {(load_rpt_id,) + values} | Error: {e}")
                continue

        # Insert Match_Rejection.
        print('Printing Match Rejections:')
        for values in match_rej:
            print(values)
            try:
                cursor.execute(match_rej_query, (load_rpt_id,) + values)
                match_count += 1
            except Exception as e:
                print(f"Error inserting row into MATCH_REJECTIONS: {(load_rpt_id,) + values} | Error: {e}")
                continue

        # Insert Standing_Rejection.
        print('Printing Rank Rejections:')
        for values in standing_rej:
            print(values)
            try:
                cursor.execute(standing_rej_query, (load_rpt_id,) + values)
                match_count += 1
            except Exception as e:
                print(f"Error inserting row into RANK_REJECTIONS: {(load_rpt_id,) + values} | Error: {e}")
                continue

        conn.commit()
    except Exception as e:
        print(f"Database connection error: {e}")
        traceback.print_exc() 
        conn.rollback()
    finally:
        if conn:
            cursor.close()
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