import os
import pandas as pd
import ruptures as rpt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Paths
input_file = os.path.join("/root", "interceptionInjection", "results", "country_weekly_interception_rates.parquet")
output_dir = os.path.join("results", "changepoints")
os.makedirs(output_dir, exist_ok=True)

# Load and prep
df = pd.read_parquet(input_file)
df['week'] = pd.to_datetime(df['week'])

# Aggregate rate by country-week (mean rate across probes)
country_ts = (
    df.groupby(['week', 'country_code'])['rate']
    .mean()
    .reset_index()
    .sort_values(['country_code', 'week'])
)

# Run detection per country
for country in country_ts['country_code'].unique():
    subset = country_ts[country_ts['country_code'] == country]
    signal = subset['rate'].fillna(0).values
    dates = subset['week'].values

    if len(signal) < 4 or signal.sum() == 0:
        print(f"Skipping {country}: too few points or all zero.")
        continue

    # Change point detection
    model = rpt.Pelt(model="rbf").fit(signal)
    result = model.predict(pen=5)  # You can tune this penalty

    # Plot and save
    plt.figure(figsize=(12, 4))
    plt.plot(dates, signal, label='Interception Rate', color='blue')

    # Mark change points
    for cp in result[:-1]:  # exclude final end-of-series index
        plt.axvline(dates[cp], color='red', linestyle='--')

    plt.title(f"Change Points for {country}")
    plt.xlabel("Date")
    plt.ylabel("Interception Rate")
    plt.grid(True)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{country}.png"))
    plt.close()

    print(f"Saved: {country}.png")

print("✅ All plots saved.")
