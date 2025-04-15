import pandas as pd
import os

def add_record_ids():
    """Add record_id column to all Betfair odds CSV files, if not already present."""
    # Get the path to the spreadsheet_raw folder
    raw_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'spreadsheet_raw')
    
    # Process each year's CSV file
    for year in range(2016, 2026):
        filename = f'betfair_odds_{year}.csv'
        filepath = os.path.join(raw_folder, filename)
        
        if not os.path.exists(filepath):
            print(f"File not found: {filename}")
            continue
        
        print(f"Processing {filename}...")
        
        # Read the CSV file
        df = pd.read_csv(filepath)

        if 'record_id' in df.columns:
            print(f"Skipped {filename} (record_id already exists)")
            continue
        
        # Add record_id column with year prefix and sequential index
        df['record_id'] = [f"{year}_{i+1}" for i in range(len(df))]
        
        # Save the updated CSV file
        df.to_csv(filepath, index=False)
        print(f"Added record_id column to {filename}")

def main():
    print("Adding record_id column to Betfair odds CSV files (if missing)...")
    add_record_ids()
    print("Process completed!")

if __name__ == "__main__":
    main()
