import pandas as pd

input_file = "~/FYP/NetData/run01/feb17normalrun.csv"
output_file = "~/FYP/NetData/run01/feb17normalrun_cleaned.csv"

df = pd.read_csv(input_file)

allowed_sources = ["10.0.0.1", "10.0.0.67"]
df = df[df.iloc[:, 2].isin(allowed_sources)]

df = df[df.iloc[:, 3].isin(allowed_sources)]

df.to_csv(output_file, index=False)

print(f"Cleaned data saved to {output_file}")
