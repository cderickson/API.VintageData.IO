from modules import table_definitions as td
import argparse
import time
import sys

def main():
    parser = argparse.ArgumentParser(
        description="Create database tables for the project."
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop all managed tables before creating them.",
    )
    args = parser.parse_args()

    start_time = time.time()

    if args.reset:
        td.delete_all_tables()

    ok = td.create_new_tables()
    print(f"Elapsed: {time.time() - start_time:.2f}s")

    if not ok:
        print("Table creation failed.")
        sys.exit(1)

    print("Table creation completed successfully.")

if __name__ == "__main__":
    main()