import pandas as pd

input_file = "~/FYP/NetData/run01/feb17normalrun_labels.csv"

df = pd.read_csv(input_file)

print(df.head(10))
