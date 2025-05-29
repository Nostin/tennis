import sys
import os
import pandas as pd
import numpy as np
from sqlalchemy import text

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_connect import get_engine

engine = get_engine()

# -------------------------
# Reset the feature columns
# -------------------------
with engine.begin() as conn:
    conn.execute(text("""
        ALTER TABLE betfair_odds
        DROP COLUMN IF EXISTS shape,
        DROP COLUMN IF EXISTS shape_strength,
        DROP COLUMN IF EXISTS shortened_fav,
        DROP COLUMN IF EXISTS late_money,
        DROP COLUMN IF EXISTS became_favourite,
        DROP COLUMN IF EXISTS lost_favourite,
        DROP COLUMN IF EXISTS stayed_favourite,
        DROP COLUMN IF EXISTS stayed_not_favourite;
    """))

    conn.execute(text("""
        ALTER TABLE betfair_odds
        ADD COLUMN IF NOT EXISTS shape TEXT,
        ADD COLUMN IF NOT EXISTS shape_strength DOUBLE PRECISION,
        ADD COLUMN IF NOT EXISTS shortened_fav BOOLEAN,
        ADD COLUMN IF NOT EXISTS late_money BOOLEAN,
        ADD COLUMN IF NOT EXISTS became_favourite BOOLEAN,
        ADD COLUMN IF NOT EXISTS lost_favourite BOOLEAN,
        ADD COLUMN IF NOT EXISTS stayed_favourite BOOLEAN,
        ADD COLUMN IF NOT EXISTS stayed_not_favourite BOOLEAN;
    """))

# -------------------------
# Load and process the data
# -------------------------
query = "SELECT * FROM betfair_odds WHERE odds_00pct IS NOT NULL AND odds_100pct IS NOT NULL"
df = pd.read_sql(query, engine)

# Classify shape
def classify_shape(row):
    odds = [row["odds_00pct"], row["odds_20pct"], row["odds_40pct"], row["odds_60pct"], row["odds_80pct"], row["odds_100pct"]]
    if any(pd.isnull(odds)):
        return "Unknown"
    
    diffs = np.diff(odds)
    if all(x < 0 for x in diffs):
        return "Shortener"
    elif all(x > 0 for x in diffs):
        return "Drifter"
    elif abs(np.std(diffs)) < 0.02:
        return "Flat"
    elif diffs[0] < 0 and diffs[-1] > 0:
        return "U-shape"
    elif diffs[0] > 0 and diffs[-1] < 0:
        return "Inverse U"
    else:
        return "Mixed"

df["shape"] = df.apply(classify_shape, axis=1)
df["shape_strength"] = df[["odds_00pct", "odds_20pct", "odds_40pct", "odds_60pct", "odds_80pct", "odds_100pct"]].std(axis=1)

df["shortened_fav"] = (
    (df["odds_00pct"] < 2.0) &
    (df["odds_20pct"] < 2.0) &
    (df["odds_40pct"] < 2.0) &
    (df["odds_60pct"] < 2.0) &
    (df["odds_80pct"] < 2.0) &
    (df["odds_100pct"] < 2.0) &
    (df["odds_100pct"] < df["odds_00pct"])
)

df["late_money"] = df["odds_80pct"] > df["odds_100pct"]

df["was_fav_at_open"] = df["odds_00pct"] < 2.0
df["was_fav_at_close"] = df["odds_100pct"] < 2.0

df["became_favourite"] = ~df["was_fav_at_open"] & df["was_fav_at_close"]
df["lost_favourite"] = df["was_fav_at_open"] & ~df["was_fav_at_close"]
df["stayed_favourite"] = df["was_fav_at_open"] & df["was_fav_at_close"]
df["stayed_not_favourite"] = ~df["was_fav_at_open"] & ~df["was_fav_at_close"]

# -------------------------
# Write to temporary table
# -------------------------
print("📤 Writing updated features to temporary table...")
df_updates = df[[
    "record_id", "shape", "shape_strength", "shortened_fav", "late_money",
    "became_favourite", "lost_favourite", "stayed_favourite", "stayed_not_favourite"
]]
df_updates.to_sql("betfair_odds_temp_updates", engine, if_exists="replace", index=False)

# -------------------------
# Bulk update using JOIN
# -------------------------
print("🔄 Bulk updating main table from temp table...")
with engine.begin() as conn:
    conn.execute(text("""
        UPDATE betfair_odds AS b
        SET
            shape = u.shape,
            shape_strength = u.shape_strength,
            shortened_fav = u.shortened_fav,
            late_money = u.late_money,
            became_favourite = u.became_favourite,
            lost_favourite = u.lost_favourite,
            stayed_favourite = u.stayed_favourite,
            stayed_not_favourite = u.stayed_not_favourite
        FROM betfair_odds_temp_updates AS u
        WHERE b.record_id = u.record_id;
    """))

    conn.execute(text("DROP TABLE IF EXISTS betfair_odds_temp_updates;"))

print("✅ Database updated with new odds movement features.")
