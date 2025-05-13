import os
import json
import pandas as pd
from pathlib import Path

# Obtain the ROOT_DIR for the parent directory where the script is running
ROOT_DIR = Path(__file__).parent

# Define the directory path
output_dir = ROOT_DIR / 'data/output'

# List all directories in the output directory
run_dirs = [d for d in output_dir.iterdir() if d.is_dir()]

# Sort directories by name (which includes the timestamp) in descending order
run_dirs.sort(reverse=True)

# Get the most recent directory
most_recent_dir = run_dirs[0]

# Path to the results.json file in the most recent directory
results_file = most_recent_dir / 'results.json'

# Load the JSON file
with open(results_file, 'r') as f:
    data = json.load(f)

# Extract relevant, irrelevant, and bypassed results
relevant_results = data.get('relevant_results', [])
irrelevant_results = data.get('irrelevant_results', [])
bypassed_results = data.get('bypassed_results', [])

# Convert results to pandas DataFrames
df_relevant = pd.DataFrame(relevant_results)
df_irrelevant = pd.DataFrame(irrelevant_results)
df_bypassed = pd.DataFrame(bypassed_results)

# Add 'TYPE_OF_RESULT' column to each DataFrame
df_relevant['TYPE_OF_RESULT'] = 'relevant_results'
df_irrelevant['TYPE_OF_RESULT'] = 'irrelevant_results'
df_bypassed['TYPE_OF_RESULT'] = 'bypassed_results'

# Combine all DataFrames
df_combined = pd.concat([df_relevant, df_irrelevant, df_bypassed], ignore_index=True)

# Define the CSV file path
csv_file = most_recent_dir / 'results.csv'

# Save combined DataFrame to CSV with escape character
df_combined.to_csv(csv_file, index=False, escapechar='\\')

print(f"CSV file saved at {csv_file}") 