import pandas as pd

import json
import queue
import duckdb
import threading

DATABASE_PATH = "/root/LOCAL/nsid_with_country.duckdb"
CHUNK_SIZE = 60 * 60 * 24 * 7
TABLE_NAME = "dns_data"
THREAD_COUNT = 4

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


def worker(job_queue, tracker):
    con = duckdb.connect(DATABASE_PATH, read_only=True)
    while True:
        try:
            ts1, ts2 = job_queue.get_nowait()
        except queue.Empty:
            break

        df = receive_data_between_timestamps(ts1, ts2, con)
        for _, row in df.iterrows():
            tracker.insert(row["domain_name"], row["timestamp"])
        job_queue.task_done()
    con.close()

def split_time_range(min_ts, max_ts, chunk_size):
    return [(min_ts + i * chunk_size, min(min_ts + (i + 1) * chunk_size, max_ts)) for i in range(int((max_ts - min_ts) / chunk_size) + 1)]


if __name__ == "__main__":
    con = duckdb.connect(DATABASE_PATH, read_only=True)
    first_nsid_encounters = NsidEncounter()

    min_ts, max_ts = con.execute(F"""
        SELECT MIN(timestamp), MAX(timestamp)
        FROM {TABLE_NAME}
    """).fetchone()
    con.close()

    jobs = queue.Queue()
    for chunk in split_time_range(min_ts, max_ts, CHUNK_SIZE):
        jobs.put(chunk)

    tracker = NsidEncounter()
    threads = []

    for _ in range(THREAD_COUNT):
        t = threading.Thread(target=worker, args=(jobs, tracker))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    tracker.record("first_seen.json")
