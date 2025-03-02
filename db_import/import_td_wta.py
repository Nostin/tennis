import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------
# CONFIGURATION
# -------------------------
CSV_FILE = r"../Raw Spreadsheet Data/TD_WTA_2015_2024.csv"
DB_NAME = "tennis"
DB_USER = "seanthompson"  # Change this if you're using a different user
DB_PASS = ""  # If you have a PostgreSQL password, set it here
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "td_wta_2015_2024"

# -------------------------
# READ CSV FILE
# -------------------------
print("Reading CSV file...")
required_columns = [
    "WTA", "Location", "Tournament", "Date", "Tier", "Court", "Surface", "Round",
    "Best of", "Winner", "Loser", "WRank", "LRank", "WPts", "LPts", "W1", "L1",
    "W2", "L2", "W3", "L3", "Wsets", "Lsets", "Comment",
    "MaxW", "MaxL", "AvgW", "AvgL"
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
key_columns = ["Tournament", "Date", "Winner", "Loser"]
duplicates = df.duplicated(subset=key_columns, keep="first")
if duplicates.any():
    print(f"Found {duplicates.sum()} duplicate rows in CSV. Removing duplicates...")
    df = df.drop_duplicates(subset=key_columns, keep="first")
    print(f"Rows after deduplication: {len(df)}")

# Convert Date column to proper format with validation
print("Parsing dates...")
try:
    df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d", errors="raise")
except ValueError as e:
    invalid_dates = df[~df["Date"].apply(lambda x: pd.to_datetime(x, format="%Y-%m-%d", errors="coerce")).notna()]
    print(f"Error: Found {len(invalid_dates)} rows with invalid dates. Sample issues:")
    print(invalid_dates[["Date"]].head(10))
    print(f"Full error: {e}")
    exit(1)

# Add Gender column
df["Gender"] = "female"
print(f"Dates parsed and Gender column added. Final DataFrame rows: {len(df)}")

# -------------------------
# CONNECT TO POSTGRESQL
# -------------------------
print("Connecting to the database...")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -------------------------
# INSERT DATA INTO DATABASE (REPLACE TABLE)
# -------------------------
print(f"Inserting data into '{TABLE_NAME}' (replacing existing table)...")
df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
print(f"Data successfully inserted into '{TABLE_NAME}'!")

# -------------------------
# VERIFY DATA
# -------------------------
print("Verifying data...")
with engine.connect() as connection:
    result = connection.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
    count = result.scalar()
    print(f"Total rows in '{TABLE_NAME}' table: {count}")