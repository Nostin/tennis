import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------
# CONFIGURATION
# -------------------------
CSV_FILE = r"../Raw Spreadsheet Data/TD_ATP_2015_2024.csv"
DB_NAME = "tennis"
DB_USER = "seanthompson"  # Change this if you're using a different user
DB_PASS = ""  # If you have a PostgreSQL password, set it here
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "td_atp_2015_2024"

# -------------------------
# READ CSV FILE
# -------------------------
print("Reading CSV file...")
required_columns = [
    "ATP", "Location", "Tournament", "Date", "Series", "Court", "Surface", "Round",
    "Best of", "Winner", "Loser", "WRank", "LRank", "WPts", "LPts", "W1", "L1",
    "W2", "L2", "W3", "L3", "W4", "L4", "W5", "L5", "Wsets", "Lsets", "Comment",
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
df["Gender"] = "male"
print(f"Dates parsed and Gender column added. Final DataFrame rows: {len(df)}")

# -------------------------
# CONNECT TO POSTGRESQL
# -------------------------
print("Connecting to the database...")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -------------------------
# CREATE TABLE WITH PRIMARY KEY AND FOREIGN KEY
# -------------------------
create_table_query = f"""
DROP TABLE IF EXISTS {TABLE_NAME};
CREATE TABLE {TABLE_NAME} (
    "MatchId" SERIAL PRIMARY KEY,
    "ATP" INT,
    "Location" TEXT,
    "Tournament" TEXT,
    "Date" DATE,
    "Series" TEXT,
    "Court" TEXT,
    "Surface" TEXT,
    "Round" TEXT,
    "Best of" INT,
    "Winner" TEXT,
    "Loser" TEXT,
    "WRank" INT,
    "LRank" INT,
    "WPts" INT,
    "LPts" INT,
    "W1" INT,
    "L1" INT,
    "W2" INT,
    "L2" INT,
    "W3" INT,
    "L3" INT,
    "W4" INT,
    "L4" INT,
    "W5" INT,
    "L5" INT,
    "Wsets" INT,
    "Lsets" INT,
    "Comment" TEXT,
    "MaxW" FLOAT,
    "MaxL" FLOAT,
    "AvgW" FLOAT,
    "AvgL" FLOAT,
    "Gender" TEXT,
    "TA_Match_Id" INTEGER DEFAULT NULL
);
"""

print(f"Creating or replacing table '{TABLE_NAME}' with MatchId and TA_Match_Id...")
with engine.connect() as connection:
    connection.execute(text(create_table_query))
    connection.commit()  # Ensure table creation is committed
    print(f"Table '{TABLE_NAME}' created successfully!")

# -------------------------
# INSERT DATA INTO DATABASE
# -------------------------
print(f"Inserting data into '{TABLE_NAME}'...")
# Insert data without MatchId ( SERIAL will auto-increment )
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

    # Verify MatchId and TA_Match_Id
    sample = connection.execute(text(f"SELECT \"MatchId\", \"TA_Match_Id\" FROM {TABLE_NAME} LIMIT 5")).fetchall()
    print("Sample MatchId and TA_Match_Id values:", sample)