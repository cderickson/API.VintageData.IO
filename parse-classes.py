from modules.classifications import parse_class_sheet, class_insert
import time
import sys

def main():
    start_time = time.time()
    print("Starting classification parse/load...")

    try:
        print("Fetching and parsing classification sheet...")
        df_valid_decks, df_valid_event_types = parse_class_sheet()
        print(
            f"Parsed {len(df_valid_decks)} deck rows and "
            f"{len(df_valid_event_types)} event type rows."
        )

        print("Loading classifications into database...")
        class_insert(df_valid_decks=df_valid_decks, df_valid_event_types=df_valid_event_types)
        print("Classification load completed successfully.")
    except Exception as exc:
        print(f"Classification load failed: {exc}")
        sys.exit(1)
    finally:
        print(f"Elapsed: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    main()