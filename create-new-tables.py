from modules import table_definitions as td
import time

start_time = time.time()

td.delete_all_tables()
td.create_new_tables()

print(time.time() - start_time)