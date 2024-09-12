import pandas as pd
import time
import os


df = pd.read_csv("../data/AB_NYC_2019.csv")
step = 10000
sleep_sec = 10

for i, start in enumerate(range(0, df.shape[0], step)):
    filepath = f'../raw/df_{i+1}.csv'
    if not os.path.exists(filepath):
        df.iloc[start:start+step].to_csv(filepath, index=False)
        print(f"[INFO] File {filepath} saved")

    time.sleep(sleep_sec)