import pandas as pd
from datetime import datetime

# ---- CONFIG ----
original_file = "/media/iankenny/7BDD-A1FD/2026_02_17_13_32_30.csv"
processed_file = "/home/iankenny/FYP/PowerData/2026_02_17_13_32_30_mastertime.csv"
master_start_str = "2026-02-17 13:32:30"
chunk_size = 10_000_000
sample_start = 999950
sample_size = 100
# ----------------


print("=== CSV Verification Script ===")

master_start = datetime.strptime(master_start_str, "%Y-%m-%d %H:%M:%S")

# ---------- TEST 1: ROW COUNT ----------
try:
    orig_rows = sum(1 for _ in open(original_file)) - 4  # skip 3 garbage rows
    proc_rows = sum(1 for _ in open(processed_file)) - 1  # minus header
    test1_pass = orig_rows == proc_rows
except Exception as e:
    test1_pass = False
    orig_rows, proc_rows = str(e), str(e)

print(f"Test 1 (Row Count): {'PASS' if test1_pass else 'FAIL'} — Original: {orig_rows}, Processed: {proc_rows}")

# ---------- TEST 2: FIRST TIMESTAMP ----------
try:
    orig_first_row = pd.read_csv(original_file, skiprows=3, nrows=1)
    proc_first_row = pd.read_csv(processed_file, nrows=1)

    orig_first_seconds = orig_first_row.iloc[0, 0]  # original time in seconds
    proc_first_date = proc_first_row.iloc[0, 0]     # processed date
    proc_first_time = proc_first_row.iloc[0, 1]     # processed time

    proc_first_timestamp = datetime.strptime(f"{proc_first_date} {proc_first_time}", "%Y-%m-%d %H:%M:%S.%f")
    expected_first_timestamp = master_start + pd.to_timedelta(orig_first_seconds, unit='s')

    test2_pass = proc_first_timestamp == expected_first_timestamp  
except Exception as e:
    test2_pass = False
    proc_first_timestamp = str(e)

print(f"Test 2 (First Timestamp): {'PASS' if test2_pass else 'FAIL'} — Processed First Timestamp: {proc_first_timestamp}")

# ---------- TEST 3: LAST TIMESTAMP ----------
try:
    # Original last row
    orig_last_row = None
    for chunk in pd.read_csv(original_file, skiprows=3, chunksize=chunk_size):
        orig_last_row = chunk.iloc[-1]
    orig_last_seconds = orig_last_row.iloc[0]

    # Processed last row
    proc_last_row = None
    for chunk in pd.read_csv(processed_file, chunksize=chunk_size):
        proc_last_row = chunk.iloc[-1]
    proc_last_date = proc_last_row.iloc[0]
    proc_last_time = proc_last_row.iloc[1]
    proc_last_timestamp = datetime.strptime(f"{proc_last_date} {proc_last_time}", "%Y-%m-%d %H:%M:%S.%f")

    expected_last_timestamp = master_start + pd.to_timedelta(orig_last_seconds, unit='s')
    test3_pass = proc_last_timestamp == expected_last_timestamp  
except Exception as e:
    test3_pass = False
    proc_last_timestamp = str(e)

print(f"Test 3 (Last Timestamp): {'PASS' if test3_pass else 'FAIL'} — Processed Last Timestamp: {proc_last_timestamp}")

# ---------- TEST 4: SAMPLE ROWS 999950-1000049 ----------
try:
    # Determine chunk containing sample
    start_chunk = sample_start // chunk_size
    row_in_chunk = sample_start % chunk_size

    # Read sample from original
    orig_reader = pd.read_csv(original_file, skiprows=3, chunksize=chunk_size)
    chunk_idx = 0
    orig_sample = None
    for chunk in orig_reader:
        if chunk_idx == start_chunk:
            orig_sample = chunk.iloc[row_in_chunk:row_in_chunk + sample_size]
            break
        chunk_idx += 1

    # Read sample from processed
    proc_reader = pd.read_csv(processed_file, chunksize=chunk_size)
    chunk_idx = 0
    proc_sample = None
    for chunk in proc_reader:
        if chunk_idx == start_chunk:
            proc_sample = chunk.iloc[row_in_chunk:row_in_chunk + sample_size]
            break
        chunk_idx += 1

    # Compare the data column
    test4_pass = all(orig_sample.iloc[:, 1].values == proc_sample.iloc[:, 2].values)
except Exception as e:
    test4_pass = False
    e_msg = str(e)

print(f"Test 4 (Sample Rows {sample_start}-{sample_start+sample_size-1} Data Column): {'PASS' if test4_pass else 'FAIL'}")
if test4_pass:
    print(proc_sample)
