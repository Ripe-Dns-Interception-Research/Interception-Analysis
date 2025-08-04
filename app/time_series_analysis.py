import os
import pandas as pd
import ruptures as rpt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import json

# === File paths ===
input_file = os.path.join("/root", "interceptionInjection", "results", "country_weekly_interception_rates.parquet")
output_dir = os.path.join("results", "changepoints")
first_seen_json = os.path.join("results", "first_seen.json")
root_servers_csv = os.path.join("results", "first-servers.csv")
os.makedirs(output_dir, exist_ok=True)

# === Load weekly interception rate ===
df = pd.read_parquet(input_file)
df['week'] = pd.to_datetime(df['week'])
df['country_code'] = df['country_code'].str.upper().str.strip()

# === Load root server deployment data ===
root_df = pd.read_csv(root_servers_csv)
root_df['country_code'] = root_df['country_code'].str.upper().str.strip()
root_df['date'] = pd.to_datetime(root_df['date'], format="%m-%Y", errors='coerce')

# === Load first seen NSID data from JSON ===
with open(first_seen_json) as f:
    first_seen_raw = json.load(f)

first_seen_df = pd.DataFrame([
    {"nsid": nsid, "date": pd.to_datetime(date)}
    for nsid, date in first_seen_raw.items()
])
first_seen_df["country_code"] = first_seen_df["nsid"].str.extract(r"\.([a-z]{2})[-\.]", expand=False).str.upper()

# === Aggregate average rate per country-week ===
country_ts = (
    df.groupby(['week', 'country_code'])['rate']
    .mean()
    .reset_index()
    .sort_values(['country_code', 'week'])
)

# === Process each country ===
for country in country_ts['country_code'].unique():
    subset = country_ts[country_ts['country_code'] == country]
    signal = subset['rate'].fillna(0).values
    dates = subset['week'].values

    if len(signal) < 4 or signal.sum() == 0:
        print(f"Skipping {country}: too few points or all zero.")
        continue

    # === Detect change points ===
    model = rpt.Pelt(model="rbf").fit(signal)
    result = model.predict(pen=5)

    # === Probe count per week ===
    probe_counts = (
        df[df['country_code'] == country]
        .groupby('week')['probe_id']
        .nunique()
        .reindex(subset['week'])
        .fillna(0)
        .astype(int)
    )

    # === Filter deployment and first-seen dates within plot window ===
    plot_start, plot_end = dates.min(), dates.max()

    deploy_dates = root_df[root_df['country_code'] == country]['date'].dropna()
    seen_dates = first_seen_df[first_seen_df['country_code'] == country]['date'].dropna()
    deploy_dates = deploy_dates[(deploy_dates >= plot_start) & (deploy_dates <= plot_end)]
    seen_dates = seen_dates[(seen_dates >= plot_start) & (seen_dates <= plot_end)]

    # === Plot ===
    fig, ax1 = plt.subplots(figsize=(12, 4))
    ax1.plot(dates, signal, label='Interception Rate', color='blue')

    for cp in result[:-1]:
        ax1.axvline(dates[cp], color='red', linestyle='--', label='Change Point')

    # Deployment and first-seen markers
    already_labeled = set()
    for d in deploy_dates:
        label = 'Root Deployment' if 'Root Deployment' not in already_labeled else None
        ax1.axvline(d, color='blue', linestyle='--', alpha=0.7, linewidth=1.5, label=label)
        already_labeled.add('Root Deployment')

    for d in seen_dates:
        label = 'First Seen (Probe)' if 'First Seen (Probe)' not in already_labeled else None
        ax1.axvline(d, color='orange', linestyle=':', alpha=0.8, linewidth=1.5, label=label)
        already_labeled.add('First Seen (Probe)')

    ax1.set_ylabel("Interception Rate", color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.set_xlabel("Date")
    ax1.set_title(f"Change Points for {country}")
    ax1.grid(True)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # Plot active probes (green dashed, right y-axis)
    ax2 = ax1.twinx()
    ax2.plot(dates, probe_counts.values, label='Active Probes', color='green', linestyle='--')
    ax2.set_ylabel("Active Probes", color='green')
    ax2.tick_params(axis='y', labelcolor='green')

    # Combine and deduplicate legends
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    handles, labels = h1 + h2, l1 + l2
    unique = dict(zip(labels, handles))
    ax1.legend(unique.values(), unique.keys(), loc="upper left")

    fig.autofmt_xdate()
    fig.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{country}.png"))
    plt.close()

    print(f"✅ Saved: {country}.png")

print("✅ All plots complete.")
