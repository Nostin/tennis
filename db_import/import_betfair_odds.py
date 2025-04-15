import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from sqlalchemy import text
from datetime import datetime
from db_connect import get_engine

# Get the database engine
engine = get_engine()

def drop_betfair_odds_table():
    """Drop the betfair_odds table if it exists."""
    drop_table_query = """
    DROP TABLE IF EXISTS betfair_odds CASCADE;
    """
    
    with engine.connect() as conn:
        conn.execute(text(drop_table_query))
        conn.commit()

def create_betfair_odds_table():
    """Create the betfair_odds table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS betfair_odds (
        record_id VARCHAR(255) PRIMARY KEY,
        event_id VARCHAR(255),
        event_name VARCHAR(255),
        date DATE,
        time_utc TIME,
        player1_name VARCHAR(255),
        player1_name_normalised VARCHAR(255),
        player1_id VARCHAR(255),
        player2_name VARCHAR(255),
        player2_name_normalised VARCHAR(255),
        player2_id VARCHAR(255), 
        odds_player_name VARCHAR(255),
        odds_player_name_normalised VARCHAR(255),
        odds_player_id VARCHAR(255),
        opening_odds DECIMAL(10,2),
        odds_25pct DECIMAL(10,2),
        odds_50pct DECIMAL(10,2),
        odds_75pct DECIMAL(10,2),
        closing_odds DECIMAL(10,2),
        num_updates INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_query))
        conn.commit()

def import_betfair_odds(csv_file):
    """Import Betfair odds from CSV file, excluding doubles matches."""
    print(f"Importing Betfair odds from {csv_file}...")
    
    # Read CSV file
    df = pd.read_csv(csv_file)
    
    # Rename columns to match database schema
    column_mapping = {
        'record_id': 'record_id',
        'event_id': 'event_id',
        'event_name': 'event_name',
        'date': 'date',
        'time_utc': 'time_utc',
        'runnername1': 'player1_name',
        'runnerid1': 'player1_id',
        'runnername2': 'player2_name',
        'runnerid2': 'player2_id',
        'runner': 'odds_player_name',
        'runner_id': 'odds_player_id',
        'opening_odds': 'opening_odds',
        'odds_25pct': 'odds_25pct',
        'odds_50pct': 'odds_50pct',
        'odds_75pct': 'odds_75pct',
        'closing_odds': 'closing_odds',
        'num_updates': 'num_updates'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Drop is_runner1 column if it exists
    if 'is_runner1' in df.columns:
        df = df.drop(columns=['is_runner1'])
    
    # 🧼 Filter out doubles matches (where both names have a "/")
    initial_count = len(df)
    df = df[~(df['player1_name'].str.contains('/') & df['player2_name'].str.contains('/'))]
    filtered_count = len(df)
    print(f"Filtered out {initial_count - filtered_count} doubles matches")

    # Convert date and time columns
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d').dt.date
    df['time_utc'] = pd.to_datetime(df['time_utc'], format='%H:%M:%S').dt.time

    # Convert numeric columns
    numeric_columns = ['opening_odds', 'odds_25pct', 'odds_50pct', 'odds_75pct', 'closing_odds', 'num_updates']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert string columns
    string_columns = ['record_id', 'event_id', 'event_name', 'player1_name', 'player1_id', 
                     'player2_name', 'player2_id', 'odds_player_name', 'odds_player_id']
    for col in string_columns:
        df[col] = df[col].astype(str)

    # Insert data into database
    with engine.connect() as conn:
        df.to_sql('betfair_odds', conn, if_exists='append', index=False)
        conn.commit()
    
    print(f"Successfully imported {len(df)} records")


def import_betfair_odds_data():
    """Import Betfair odds data from spreadsheets."""
    years = range(2016, 2025)  # 2016 to 2024
    base_path = "spreadsheet_raw"
    
    for year in years:
        file_name = f"betfair_odds_{year}.csv"
        file_path = os.path.join(base_path, file_name)
        
        if not os.path.exists(file_path):
            print(f"Warning: File {file_path} not found. Skipping...")
            continue
            
        print(f"Processing {file_name}...")
        import_betfair_odds(file_path)

def main():
    # Always drop and recreate the table
    print("Dropping existing betfair_odds table...")
    drop_betfair_odds_table()
    
    print("Creating betfair_odds table...")
    create_betfair_odds_table()
    
    if len(sys.argv) == 1:
        # If no arguments provided, import all yearly files
        print("Importing Betfair odds data...")
        import_betfair_odds_data()
    elif len(sys.argv) == 2:
        # If a single file is provided, import just that file
        csv_file = sys.argv[1]
        import_betfair_odds(csv_file)
    else:
        print("Usage: python import_betfair_odds.py [csv_file]")
        print("  If no file is provided, imports all yearly files (2016-2024)")
        print("  If a file is provided, imports just that file")
        sys.exit(1)

if __name__ == "__main__":
    main() 