import pandas as pd

import json
import duckdb
import threading

database_path = "/root/LOCAL/nsid_with_country.duckdb"
CHUNK_SIZE = 60 * 60 * 24 * 7
TABLE_NAME = "dns_data"

class NsidEncounter:

    def __init__(self):
        self.database = {}
        self.mx = threading.Lock()


    def insert(self, nsid, timestamp):
        with self.mx:
            if nsid in self.database:
                if timestamp < self.database[nsid]:
                    self.database[nsid] = timestamp

            else:
                self.database[nsid] = timestamp


    def record(self, path):
        with self.mx, open(path, "w") as file:
            json.dump(self.database, file, indent=4)



def receive_data_between_timestamps(ts1, ts2, con):
    return con.execute(f"""
                SELECT *
                FROM {TABLE_NAME}
                WHERE timestamp >= {ts1}
                AND timestamp < {ts2}
                ORDER BY timestamp
            """).fetch_df()


def process_singular_data(singular_data):
    pass


def process_data(min_ts, max_ts, con):
    data = receive_data_between_timestamps(min_ts, max_ts, con)

    print(data.head())


if __name__ == "__main__":
    con = duckdb.connect(database_path, read_only=True)
    first_nsid_encounters = NsidEncounter()

    min_ts, max_ts = con.execute(F"""
        SELECT MIN(timestamp), MAX(timestamp)
        FROM {TABLE_NAME}
    """).fetchone()

    current_min_timestamp = min_ts
    current_max_timestamp = min_ts + CHUNK_SIZE

    process_data(current_min_timestamp, current_max_timestamp, con)

    con.close()
