# Original script created by https://github.com/jasina007

import pandas as pd
import os

# Construct input and output file paths
input_file = os.path.join(os.getcwd(), '..', 'Source', 'FullCsv', 'merged_data.csv')
output_dir = os.path.join(os.getcwd(), '..', 'Source', 'SpotifySplitted')

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Check if the input file exists
if not os.path.exists(input_file):
    print(f"Error: Input file '{input_file}' not found.")
    exit()

chunksize = 1000000
reader = pd.read_csv(input_file, chunksize=chunksize, low_memory=False)

# Divide the original CSV file into smaller files
for i, chunk in enumerate(reader):
    output_file = os.path.join(output_dir, f'SpotifyCharts_part_{i + 1}.csv')
    chunk.to_csv(output_file, index=False)
    print(f'Created file {output_file}')

print('Operation completed.')
