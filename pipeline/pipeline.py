import sys
import pandas as pd

df = pd.DataFrame({"day": [1, 2], "num_trips": [3, 4]})
month = sys.argv[2] if len(sys.argv) > 2 else "01"
df['month'] = month
print(df.head())

day = sys.argv[1] if len(sys.argv) > 1 else "01"
df.to_parquet(f"output_month_{month}.parquet")