import pandas as pd

input_file = "~/FYP/NetData/run01/feb17normalrun_cleaned.csv"
output_file = "~/FYP/NetData/run01/feb17normalrun_labels.csv"


df = pd.read_csv(input_file)

df = df[df.iloc[:, 6].astype(str).str.contains(">  9001")]

df.to_csv(output_file, index=False)

print(f"Rows with port 9001 saved to {output_file}")
