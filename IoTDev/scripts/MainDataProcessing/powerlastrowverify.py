import pandas as pd

def print_last_rows(file_path, n=5, skiprows=0):
    last_rows = None
    for chunk in pd.read_csv(file_path, skiprows=skiprows, chunksize=10_000_000):
        last_rows = chunk.tail(n)  # keep last n rows of current chunk
    print(f"Last {n} rows of {file_path}:")
    print(last_rows)
    print("-" * 50)

# Original file: skip first 3 garbage rows
print_last_rows("/media/iankenny/7BDD-A1FD/2026_02_17_13_32_30.csv", n=5, skiprows=3)

# Processed file: header already correct
print_last_rows("/home/iankenny/FYP/PowerData/2026_02_17_13_32_30_mastertime.csv", n=5)
