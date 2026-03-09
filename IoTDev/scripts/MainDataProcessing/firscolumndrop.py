import pandas as pd

# Hardcoded file paths
input_file = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"
output_file = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime1.csv"

# Read CSV
df = pd.read_csv(input_file)

# Drop first column (index 0)
df = df.drop(df.columns[0], axis=1)

# Write updated CSV
df.to_csv(output_file, index=False)

print("First column removed successfully.")
