import pandas as pd

import duckdb
import argparse

database_path = ""
CHUNK_SIZE = 60 * 60 * 24 * 30

def receive_data_between_timestamps(ts1, ts2, con):
    pass


def process_singular_data(singular_data):
    pass


def process_data(data):
    pass


if __name__ == "__main__":
    con = duckdb.connect(database_path)

    min_ts, max_ts = con.execute("""
        SELECT MIN(timestamp), MAX(timestamp)
        FROM measurements
    """).fetchone()

    current_min_timestamp = min_ts
    current_max_timestamp = min_ts + CHUNK_SIZE

    while current_min_timestamp < max_ts:
        df = con.execute(f"""
                SELECT *
                FROM measurements
                WHERE timestamp >= {current_min_timestamp}
                  AND timestamp < {current_max_timestamp}
                ORDER BY timestamp
            """).fetch_df()

        data = receive_data_between_timestamps(current_min_timestamp, current_max_timestamp)
        process_data(data)

        current_min_timestamp = min(current_min_timestamp + CHUNK_SIZE, max_ts)
        current_max_timestamp = current_min_timestamp + CHUNK_SIZE
