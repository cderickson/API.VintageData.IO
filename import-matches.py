from modules.match_import import parse_matchup_sheet, match_insert, insert_load_stats
from modules.classifications import parse_class_sheet, class_insert
import warnings
from datetime import datetime, timedelta
import time
import argparse
warnings.filterwarnings('ignore', category=UserWarning, message="pandas only supports SQLAlchemy connectable")

def parse_args():
    default_start = "2024-08-24"
    default_end = (datetime.today().date() + timedelta(days=1)).strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser(
        description="Import MTGO matches for a date range."
    )
    parser.add_argument(
        "--start_date",
        dest="start_date",
        default=None,
        help=f"Start date in YYYY-MM-DD format (default: {default_start}).",
    )
    parser.add_argument(
        "--end_date",
        dest="end_date",
        default=default_end,
        help=f"End date in YYYY-MM-DD format (default: {default_end}).",
    )
    parser.add_argument(
        "--debug_excels",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable/disable debug Excel exports (default: enabled).",
    )
    return parser.parse_args()

def parse_date(date_str, arg_name):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{arg_name} must be YYYY-MM-DD. Received: {date_str}") from exc

def main():
    args = parse_args()
    start_date = parse_date(args.start_date, "start_date")
    end_date = parse_date(args.end_date, "end_date")

    if end_date <= start_date:
        raise ValueError("end_date must be later than start_date.")

    start_time = time.time()
    print("Start Time: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    print("Refreshing classification tables...")
    df_valid_decks, df_valid_event_types = parse_class_sheet()
    class_insert(df_valid_decks, df_valid_event_types)

    df_matches, df_events, df_standings, load_rep_list, event_skipped_rej, standings_skipped = parse_matchup_sheet(
        start_date=start_date, 
        end_date=end_date,
        export_debug_excels=args.debug_excels
    )
    load_rep_ins, event_rej, match_rej, standing_rej = match_insert(
        df_matches=df_matches, 
        df_events=df_events, 
        df_standings=df_standings, 
        standings_skipped=standings_skipped, 
        start_date=start_date, 
        end_date=end_date,
        export_debug_excels=args.debug_excels
    )
    load_report = [start_date,end_date - timedelta(days=1)] + load_rep_list + load_rep_ins
    insert_load_stats(
        load_report=load_report, 
        event_rej=event_skipped_rej + event_rej, 
        match_rej=match_rej, 
        standing_rej=standing_rej
    )

    print(time.time() - start_time)

if __name__ == "__main__":
    main()