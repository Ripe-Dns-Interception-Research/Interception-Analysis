import pandas as pd

import json
import queue
import duckdb
import threading

DATABASE_PATH = "/root/LOCAL/nsid_with_country.duckdb"
CHUNK_SIZE = 60 * 60 * 24 * 30
TABLE_NAME = "dns_data"
THREAD_COUNT = 12


class NsidEncounter:

    def __init__(self):
        self.database = {}
        self.mx = threading.Lock()

    def insert(self, nsid, date_obj):
        date_str = date_obj.date().isoformat() if hasattr(date_obj, "date") else str(date_obj)
        with self.mx:
            if nsid not in self.database or date_str < self.database[nsid]:
                self.database[nsid] = date_str

    def record(self, path):
        with self.mx, open(path, "w") as file:
            json.dump(self.database, file, indent=4)


def receive_data_between_timestamps(ts1, ts2, con):
    return con.execute(f"""
                SELECT nsid, MIN(DATE_TRUNC('day', TO_TIMESTAMP(timestamp))) AS first_day_seen
                FROM {TABLE_NAME}
                WHERE timestamp >= {ts1}
                AND timestamp < {ts2}
                GROUP BY nsid
            """).fetch_df()


def worker(job_queue, tracker):
    con = duckdb.connect(DATABASE_PATH, read_only=True)
    while True:
        ts1, ts2 = job_queue.get()
        if ts1 is None or ts2 is None:
            break

        df = receive_data_between_timestamps(ts1, ts2, con)
        for _, row in df.iterrows():
            nsid = row["nsid"]
            seen = row["first_day_seen"]

            if pd.isna(nsid) or pd.isna(seen):
                continue

            tracker.insert(str(nsid), seen.strftime("%Y-%m-%d"))

        job_queue.task_done()
        print(f"Thread {threading.current_thread().name} processed {ts1}–{ts2} with {len(df)} rows")
    con.close()


def split_time_range(min_ts, max_ts, chunk_size):
    return [(min_ts + i * chunk_size, min(min_ts + (i + 1) * chunk_size, max_ts)) for i in
            range(int((max_ts - min_ts) / chunk_size) + 1)]


if __name__ == "__main__":
    con = duckdb.connect(DATABASE_PATH, read_only=True)

    min_ts, max_ts = con.execute(F"""
        SELECT MIN(timestamp), MAX(timestamp)
        FROM {TABLE_NAME}
    """).fetchone()
    con.close()

    # We are putting none for each thread so they know job is done and quit.
    # might be implemented better?
    jobs = queue.Queue()
    for chunk in split_time_range(min_ts, max_ts, CHUNK_SIZE):
        print(chunk)
        jobs.put(chunk)
    for _ in range(THREAD_COUNT):
        jobs.put((None, None))

    tracker = NsidEncounter()
    threads = []

    for _ in range(THREAD_COUNT):
        t = threading.Thread(target=worker, args=(jobs, tracker))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    with open("first_seen.json.bck", "w") as file:
        file.write(str(tracker.database))

    tracker.record("first_seen.json")
