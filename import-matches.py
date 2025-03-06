from modules import match_import as mi
import warnings
from datetime import datetime, timedelta
import time
import sys
warnings.filterwarnings('ignore', category=UserWarning, message="pandas only supports SQLAlchemy connectable")

start_time = time.time()

print('Start Time: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

# Day 0 is 8-25-2024.
if len(sys.argv) > 1:
    start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
else:
    # start_date = datetime(2024, 8, 25).date()
    start_date = datetime.today().date() - timedelta(days=14)

end_date = start_date + timedelta(days=7)

df_matches, df_events, df_standings, load_rep_list, event_skipped_rej = mi.parse_matchup_sheet(start_date=start_date, end_date=end_date)
# mi.test(df_matches, df_events)
load_rep_ins, event_rej, match_rej = mi.match_insert(df_matches, df_events, df_standings, start_date=start_date, end_date=end_date)

load_report = [start_date,end_date - timedelta(days=1)] + load_rep_list + load_rep_ins
mi.insert_load_stats(load_report, event_skipped_rej + event_rej, match_rej)

print(time.time() - start_time)