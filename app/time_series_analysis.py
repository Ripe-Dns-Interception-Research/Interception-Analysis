import os
import pandas as pd
import ruptures as rpt
import matplotlib.pyplot as plt

# Paths
input_file = os.path.join("results", "country_weekly_interception_rates.parquet")
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

    if len(signal) < 4 or signal.sum() == 0:
        print(f"Skipping {country}: too few points or all zero.")
        continue

    # Change point detection
    model = rpt.Pelt(model="rbf").fit(signal)
    result = model.predict(pen=5)  # You can tune penalty

    # Plot and save
    plt.figure(figsize=(12, 4))
    rpt.display(signal, result)
    plt.title(f"Change Points for {country}")
    plt.xlabel("Weeks")
    plt.ylabel("Interception Rate")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{country}.png"))
    plt.close()

    print(f"Saved: {country}.png")

print("✅ All plots saved.")
