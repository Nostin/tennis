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
    df_td = pd.read_sql("SELECT * FROM td_atp_2015_2024", connection)
    df_ta = pd.read_sql("SELECT * FROM ta_atp_2015_2024", connection)

print(f"Loaded {len(df_td)} records from tennis-data.")
print(f"Loaded {len(df_ta)} records from tennis-abstract.")

# Reset TA_Match_Id to NULL
print("Resetting TA_Match_Id to NULL in td_atp_2015_2024...")
with engine.connect() as connection:
    connection.execute(text('UPDATE td_atp_2015_2024 SET "TA_Match_Id" = NULL;'))
    connection.commit()
print("✅ TA_Match_Id reset successfully!")

# -------------------------
# PREPARE DATA FOR MATCHING
# -------------------------
def extract_score_char(score, position):
    if pd.isna(score) or not isinstance(score, str) or score.strip().upper() in ["W/O", "RET", "DEF"]:
        return None
    try:
        return int(score[0]) if position == 0 else int(score[2])
    except (IndexError, ValueError):
        return None

df_ta["score_w1"] = df_ta["score"].apply(lambda x: extract_score_char(x, 0))
df_ta["score_l1"] = df_ta["score"].apply(lambda x: extract_score_char(x, 1))

# Ensure consistent case for surface
df_td["Surface"] = df_td["Surface"].str.title()
df_ta["surface"] = df_ta["surface"].str.title()

# Ensure numeric types match
df_td["W1"] = pd.to_numeric(df_td["W1"], errors="coerce")
df_td["L1"] = pd.to_numeric(df_td["L1"], errors="coerce")
df_td["WRank"] = pd.to_numeric(df_td["WRank"], errors="coerce")
df_td["LRank"] = pd.to_numeric(df_td["LRank"], errors="coerce")
df_td["WPts"] = pd.to_numeric(df_td["WPts"], errors="coerce")
df_td["LPts"] = pd.to_numeric(df_td["LPts"], errors="coerce")
df_ta["winner_rank"] = pd.to_numeric(df_ta["winner_rank"], errors="coerce")
df_ta["loser_rank"] = pd.to_numeric(df_ta["loser_rank"], errors="coerce")
df_ta["winner_rank_points"] = pd.to_numeric(df_ta["winner_rank_points"], errors="coerce")
df_ta["loser_rank_points"] = pd.to_numeric(df_ta["loser_rank_points"], errors="coerce")

# Convert dates
df_td["Date"] = pd.to_datetime(df_td["Date"], errors="coerce")
df_ta["tourney_date"] = pd.to_datetime(df_ta["tourney_date"], errors="coerce")

# -------------------------
# EXACT MATCHING
# -------------------------
print("Performing exact matching in DataFrames...")

# Standard exact match
df_exact_match = df_ta.merge(
    df_td,
    left_on=["winner_rank", "loser_rank", "winner_rank_points", "loser_rank_points", "surface", "score_w1", "score_l1"],
    right_on=["WRank", "LRank", "WPts", "LPts", "Surface", "W1", "L1"],
    how="inner"
)
df_exact_match["date_diff"] = (df_exact_match["Date"] - df_exact_match["tourney_date"]).dt.days
df_exact_match = df_exact_match[(df_exact_match["date_diff"] >= 0) & (df_exact_match["date_diff"] <= 16)]

# Exclude matched records
matched_td_ids = df_exact_match["MatchId_y"].unique()
matched_ta_ids = df_exact_match["MatchId_x"].unique()
df_td_unmatched = df_td[~df_td["MatchId"].isin(matched_td_ids)]
df_ta_unmatched = df_ta[~df_ta["MatchId"].isin(matched_ta_ids)]

# Walkover match
df_walkover_match = df_ta_unmatched[df_ta_unmatched["score"].str.strip().str.upper() == "W/O"].merge(
    df_td_unmatched[df_td_unmatched["Comment"].str.lower() == "walkover"],
    left_on=["winner_rank", "loser_rank", "winner_rank_points", "loser_rank_points", "surface"],
    right_on=["WRank", "LRank", "WPts", "LPts", "Surface"],
    how="inner"
)
df_walkover_match["date_diff"] = (df_walkover_match["Date"] - df_walkover_match["tourney_date"]).dt.days
df_walkover_match = df_walkover_match[(df_walkover_match["date_diff"] >= 0) & (df_walkover_match["date_diff"] <= 16)]

# Combine matches
df_matched = pd.concat([df_exact_match, df_walkover_match])

# Check duplicates
duplicate_td_matches = df_matched["MatchId_y"].duplicated(keep=False)
duplicate_details = []
if duplicate_td_matches.any():
    duplicate_count = df_matched["MatchId_y"].duplicated(keep="first").sum()
    print(f"⚠️ Warning: Found {duplicate_count} td_atp_2015_2024 records with multiple matches!")
    print("Sample duplicate td MatchIds:", df_matched[duplicate_td_matches]["MatchId_y"].tolist()[:5])
    duplicate_details = df_matched[duplicate_td_matches][["MatchId_y", "MatchId_x", "tourney_date", "Date", "score", "Comment"]].to_string()
    print("Duplicate matches details:", duplicate_details)

# Filter to 1-1 matches only
df_matched_1to1 = df_matched[~df_matched["MatchId_y"].isin(df_matched[duplicate_td_matches]["MatchId_y"])]
print(f"✅ Exact 1-1 matches found: {len(df_matched_1to1)} records.")

# -------------------------
# UPDATE TA_Match_Id FOR 1-1 MATCHES ONLY
# -------------------------
print("Updating TA_Match_Id in td_atp_2015_2024 for 1-1 matches only...")
with engine.connect() as connection:
    df_matched_1to1[["MatchId_y", "MatchId_x"]].rename(columns={"MatchId_y": "td_match_id", "MatchId_x": "ta_match_id"}).to_sql(
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
print("✅ TA_Match_Id updated successfully for 1-1 matches!")

# -------------------------
# VERIFY UPDATES
# -------------------------
print("Verifying updates...")
with engine.connect() as connection:
    matched_count = connection.execute(text('SELECT COUNT(*) FROM td_atp_2015_2024 WHERE "TA_Match_Id" IS NOT NULL')).scalar()
    unmatched_count = connection.execute(text('SELECT COUNT(*) FROM td_atp_2015_2024 WHERE "TA_Match_Id" IS NULL')).scalar()
    print(f"Matched td records (TA_Match_Id populated): {matched_count}")
    print(f"Unmatched td records (TA_Match_Id NULL): {unmatched_count}")

print("✅ Data processing complete!")