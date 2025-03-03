import pandas as pd
from sqlalchemy import create_engine, text
from datetime import timedelta

# -------------------------
# CONFIGURATION
# -------------------------
DB_NAME = "tennis"
DB_USER = "seanthompson"
DB_PASS = ""  # Add password if necessary
DB_HOST = "localhost"
DB_PORT = "5432"

# -------------------------
# CONNECT TO POSTGRESQL
# -------------------------
print("Connecting to the database...")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -------------------------
# LOAD DATA FROM DATABASE
# -------------------------
print("Loading data from database...")
with engine.connect() as connection:
    df_td = pd.read_sql('SELECT * FROM td_atp_2015_2024 WHERE "TA_Match_Id" IS NULL', connection)
    df_ta = pd.read_sql('SELECT * FROM ta_atp_2015_2024', connection)

print(f"Loaded {len(df_td)} unmatched records from tennis-data.")
print(f"Loaded {len(df_ta)} records from tennis-abstract.")

# -------------------------
# PREPARE DATA FOR MATCHING
# -------------------------
print("Preparing data for matching...")

# Convert dates
df_td["Date"] = pd.to_datetime(df_td["Date"], errors="coerce")
df_ta["tourney_date"] = pd.to_datetime(df_ta["tourney_date"], errors="coerce")

# Normalize tournament names
df_td["Tournament"] = df_td["Tournament"].str.strip()
df_ta["tourney_name"] = df_ta["tourney_name"].str.strip()

# Standardize tournament names to match between datasets
tournament_name_mapping = {
    "French Open": "Roland Garros",
    "BNP Paribas Open": "Indian Wells Masters",
    "Western & Southern Financial Group Masters": "Cincinnati Masters",
    "Mutua Madrid Open": "Madrid Masters",
    "Geneva Open": "Geneva",
    "Lyon Open": "Lyon",
    "Sony Ericsson Open": "Miami Masters",
    "Heineken Open": "Auckland",
    "Masters Cup": "Tour Finals",
    "Winston-Salem Open at Wake Forest University": "Winston-Salem",
    "AEGON Championships": "Queen's Club",
    "AnyTech365 Andalucia Open": "Marbella"
}

# Apply mapping to standardize tournament names
df_td["Tournament"] = df_td["Tournament"].replace(tournament_name_mapping)
df_ta["tourney_name"] = df_ta["tourney_name"].replace(tournament_name_mapping)

# Extract last names from winner and loser names
def extract_last_name(name):
    if pd.isna(name):
        return None
    parts = name.split()
    return parts[-1] if parts else None

df_ta["winner_last"] = df_ta["winner_name"].apply(extract_last_name)
df_ta["loser_last"] = df_ta["loser_name"].apply(extract_last_name)

def extract_td_last_name(name):
    if pd.isna(name):
        return None
    parts = name.split()
    if len(parts) > 1:
        return parts[0]
    return name

df_td["Winner_last"] = df_td["Winner"].apply(extract_td_last_name)
df_td["Loser_last"] = df_td["Loser"].apply(extract_td_last_name)

# -------------------------
# PERFORM MATCHING
# -------------------------
print("Performing matching based on Tournament names, date, and last names...")
df_matched = df_ta.merge(
    df_td,
    left_on=["tourney_name", "winner_last", "loser_last"],
    right_on=["Tournament", "Winner_last", "Loser_last"],
    how="inner"
)
df_matched["date_diff"] = (df_matched["Date"] - df_matched["tourney_date"]).dt.days
df_matched = df_matched[(df_matched["date_diff"] >= 0) & (df_matched["date_diff"] <= 16)]

print(f"✅ Matched {len(df_matched)} records based on tournament name, date, and last names.")

# -------------------------
# UPDATE TA_Match_Id FOR MATCHES
# -------------------------
print("Updating TA_Match_Id in td_atp_2015_2024...")
with engine.connect() as connection:
    df_matched[["MatchId_y", "MatchId_x"]].rename(columns={"MatchId_y": "td_match_id", "MatchId_x": "ta_match_id"}).to_sql(
        "temp_match_ids", connection, if_exists="replace", index=False
    )
    update_query = """
    UPDATE td_atp_2015_2024
    SET "TA_Match_Id" = temp_match_ids.ta_match_id
    FROM temp_match_ids
    WHERE td_atp_2015_2024."MatchId" = temp_match_ids.td_match_id;
    """
    connection.execute(text(update_query))
    connection.commit()
print("✅ TA_Match_Id updated successfully!")

# -------------------------
# VERIFY UPDATES
# -------------------------
print("Verifying updates...")
with engine.connect() as connection:
    matched_count = connection.execute(text('SELECT COUNT(*) FROM td_atp_2015_2024 WHERE "TA_Match_Id" IS NOT NULL')).scalar()
    unmatched_count = connection.execute(text('SELECT COUNT(*) FROM td_atp_2015_2024 WHERE "TA_Match_Id" IS NULL')).scalar()
    print(f"Total Matched td records (TA_Match_Id populated): {matched_count}")
    print(f"Total Unmatched td records (TA_Match_Id NULL): {unmatched_count}")

print("✅ Data processing complete!")
