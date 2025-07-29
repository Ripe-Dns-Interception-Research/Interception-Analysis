import duckdb
import pandas as pd
from datetime import datetime, timedelta

con = duckdb.connect("/root/PROJECT/database_with_country_codes.db")

TABLE_NAME = "measurements"

# Step 1: Get min and max timestamps from table
min_max = con.execute(f"""
    SELECT 
        MIN(timestamp) as min_ts, 
        MAX(timestamp) as max_ts 
    FROM {TABLE_NAME}
""").fetchone()

# Convert UNIX timestamps to datetime
start_date = datetime.fromtimestamp(min_max[0])
end_date = datetime.fromtimestamp(min_max[1])
chunk_days = 30

print(f"Processing from {start_date} to {end_date}")

results = []

# Step 2: Loop through chunks
while start_date < end_date:
    chunk_end = start_date + timedelta(days=chunk_days)

    query = f"""
    SELECT 
        date_trunc('day', to_timestamp(timestamp)) AS day,
        probe_id,
        COUNT(*) AS total,
        SUM(intercepted::INT) AS intercepted_count,
        1.0 * SUM(intercepted::INT) / COUNT(*) AS rate
    FROM measurements
    WHERE timestamp BETWEEN {int(start_date.timestamp())} AND {int(chunk_end.timestamp())}
    GROUP BY day, probe_id
    ORDER BY day
    """

    chunk_df = con.execute(query).df()
    results.append(chunk_df)

    print(f"Processed chunk: {start_date.date()} → {chunk_end.date()}")
    start_date = chunk_end

# Step 3: Combine and save
df = pd.concat(results, ignore_index=True)
df.to_parquet("probe_daily_interception_rates.parquet")
