import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------
# CONFIGURATION
# -------------------------
CSV_FILE = r"../spreadsheet_raw/TA_ATP_2015_2024.csv"
DB_NAME = "tennis"
DB_USER = "seanthompson"  # Change this if you're using a different user
DB_PASS = ""  # If you have a PostgreSQL password, set it here
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "ta_atp_2015_2024"

# -------------------------
# READ CSV FILE
# -------------------------
print("Reading CSV file...")
required_columns = [
    "tourney_id", "tourney_name", "surface", "draw_size", "tourney_level", "tourney_date", "match_num", 
    "winner_id", "winner_seed", "winner_entry", "winner_name", "winner_hand", "winner_ht", "winner_ioc",
    "winner_age", "loser_id", "loser_seed", "loser_entry", "loser_name", "loser_hand", "loser_ht",
    "loser_ioc", "loser_age", "score", "best_of", "round", "minutes", "w_ace", "w_df", "w_svpt", "w_1stIn",
    "w_1stWon", "w_2ndWon", "w_SvGms", "w_bpSaved", "w_bpFaced", "l_ace", "l_df", "l_svpt", "l_1stIn",
    "l_1stWon", "l_2ndWon", "l_SvGms", "l_bpSaved", "l_bpFaced", "winner_rank", "winner_rank_points",
    "loser_rank", "loser_rank_points"
]
try:
    df = pd.read_csv(CSV_FILE, usecols=required_columns)
    print(f"Rows loaded from CSV: {len(df)}")
except FileNotFoundError:
    print(f"Error: {CSV_FILE} not found.")
    exit(1)
except ValueError as e:
    print(f"Error: CSV missing required columns or invalid data - {e}")
    exit(1)
except Exception as e:
    print(f"Error loading data: {e}")
    exit(1)

# Remove duplicates based on key match identifiers
print("Checking for duplicates...")
key_columns = ["tourney_name", "tourney_date", "winner_name", "loser_name"]
duplicates = df.duplicated(subset=key_columns, keep="first")
if duplicates.any():
    print(f"Found {duplicates.sum()} duplicate rows in CSV. Removing duplicates...")
    df = df.drop_duplicates(subset=key_columns, keep="first")
    print(f"Rows after deduplication: {len(df)}")

# Convert Date column to proper format with validation
print("Parsing dates...")
try:
    df["tourney_date"] = pd.to_datetime(df["tourney_date"], format="%Y%m%d", errors="raise")
except ValueError as e:
    invalid_dates = df[~df["tourney_date"].apply(lambda x: pd.to_datetime(x, format="%Y%m%d", errors="coerce")).notna()]
    print(f"Error: Found {len(invalid_dates)} rows with invalid dates. Sample issues:")
    print(invalid_dates[["tourney_date"]].head(10))
    print(f"Full error: {e}")
    exit(1)

# Add Gender column
df["Gender"] = "male"
print(f"Dates parsed and Gender column added. Final DataFrame rows: {len(df)}")

# -------------------------
# CONNECT TO POSTGRESQL
# -------------------------
print("Connecting to the database...")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -------------------------
# CREATE TABLE WITH PRIMARY KEY
# -------------------------
create_table_query = f"""
DROP TABLE IF EXISTS {TABLE_NAME};
CREATE TABLE {TABLE_NAME} (
    "MatchId" SERIAL PRIMARY KEY,
    "tourney_id" TEXT,
    "tourney_name" TEXT,
    "surface" TEXT,
    "draw_size" INT,
    "tourney_level" TEXT,
    "tourney_date" DATE,
    "match_num" INT,
    "winner_id" INT,
    "winner_seed" TEXT,
    "winner_entry" TEXT,
    "winner_name" TEXT,
    "winner_hand" TEXT,
    "winner_ht" INT,
    "winner_ioc" TEXT,
    "winner_age" FLOAT,
    "loser_id" INT,
    "loser_seed" TEXT,
    "loser_entry" TEXT,
    "loser_name" TEXT,
    "loser_hand" TEXT,
    "loser_ht" INT,
    "loser_ioc" TEXT,
    "loser_age" FLOAT,
    "score" TEXT,
    "best_of" INT,
    "round" TEXT,
    "minutes" INT,
    "w_ace" INT,
    "w_df" INT,
    "w_svpt" INT,
    "w_1stIn" INT,
    "w_1stWon" INT,
    "w_2ndWon" INT,
    "w_SvGms" INT,
    "w_bpSaved" INT,
    "w_bpFaced" INT,
    "l_ace" INT,
    "l_df" INT,
    "l_svpt" INT,
    "l_1stIn" INT,
    "l_1stWon" INT,
    "l_2ndWon" INT,
    "l_SvGms" INT,
    "l_bpSaved" INT,
    "l_bpFaced" INT,
    "winner_rank" INT,
    "winner_rank_points" INT,
    "loser_rank" INT,
    "loser_rank_points" INT,
    "Gender" TEXT
);
"""

print(f"Creating or replacing table '{TABLE_NAME}' with MatchId...")
with engine.connect() as connection:
    connection.execute(text(create_table_query))
    connection.commit()  # Ensure table creation is committed
    print(f"Table '{TABLE_NAME}' created successfully!")

# -------------------------
# INSERT DATA INTO DATABASE
# -------------------------
print(f"Inserting data into '{TABLE_NAME}'...")
# Insert data without MatchId (SERIAL will auto-increment)
df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
print(f"Data successfully inserted into '{TABLE_NAME}'!")

# -------------------------
# VERIFY DATA
# -------------------------
print("Verifying data...")
with engine.connect() as connection:
    result = connection.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
    count = result.scalar()
    print(f"Total rows in '{TABLE_NAME}' table: {count}")

    # Verify MatchId
    sample = connection.execute(text(f"SELECT \"MatchId\" FROM {TABLE_NAME} LIMIT 5")).fetchall()
    print("Sample MatchId values:", sample)