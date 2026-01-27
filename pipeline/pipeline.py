import sys
import pandas as pd

# Read command-line args
day = sys.argv[1] if len(sys.argv) > 1 else "01"
month = sys.argv[2] if len(sys.argv) > 2 else "01"

# Create dataframe
df = pd.DataFrame({"day": [1, 2], "num_trips": [3, 4]})
df['month'] = month

print(df.head())

# Save parquet file
output_file = f"output_month_{month}.parquet"
df.to_parquet(output_file)
print(f"Parquet saved: {output_file}")
