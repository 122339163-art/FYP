import pandas as pd
from datetime import datetime

# ---- CONFIG ----
input_file = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata.csv"
output_file = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"

# MUST include start date
master_start_str = "2026-02-17 13:32:33"
# ----------------

# Load CSV
df = pd.read_csv(input_file)

# Parse full starting timestamp
master_start = datetime.strptime(master_start_str, "%Y-%m-%d %H:%M:%S")

# Convert second column (index 1) from seconds to full datetime
absolute_dt = master_start + pd.to_timedelta(df.iloc[:, 1], unit="s")

# Insert new "date" column immediately before time column
df.insert(1, "date", absolute_dt.dt.strftime("%Y-%m-%d"))

# Replace original time column (now index 2 after insert)
df.iloc[:, 2] = absolute_dt.dt.strftime("%H:%M:%S.%f")

# Save updated CSV
df.to_csv(output_file, index=False)

print("Conversion complete.")
