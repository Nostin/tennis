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
        DROP COLUMN IF EXISTS late_money;
    """))

    conn.execute(text("""
        ALTER TABLE betfair_odds
        ADD COLUMN IF NOT EXISTS shape TEXT,
        ADD COLUMN IF NOT EXISTS shape_strength DOUBLE PRECISION,
        ADD COLUMN IF NOT EXISTS shortened_fav BOOLEAN,
        ADD COLUMN IF NOT EXISTS late_money BOOLEAN;
    """))

# Load the data
query = "SELECT * FROM betfair_odds WHERE opening_odds IS NOT NULL AND closing_odds IS NOT NULL"
df = pd.read_sql(query, engine)

# Classify shape
def classify_shape(row):
    odds = [row["opening_odds"], row["odds_25pct"], row["odds_50pct"], row["odds_75pct"], row["closing_odds"]]
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

# Add derived columns
df["shape"] = df.apply(classify_shape, axis=1)
df["shape_strength"] = df[["opening_odds", "odds_25pct", "odds_50pct", "odds_75pct", "closing_odds"]].std(axis=1)

df["shortened_fav"] = (
    (df["opening_odds"] < 2.0) &
    (df["odds_25pct"] < 2.0) &
    (df["odds_50pct"] < 2.0) &
    (df["odds_75pct"] < 2.0) &
    (df["closing_odds"] < 2.0) &
    (df["closing_odds"] < df["opening_odds"])
)

df["late_money"] = df["odds_75pct"] > df["closing_odds"]

# Prepare to update the DB
print("Updating records in the database...")
with engine.begin() as conn:
    for _, row in df.iterrows():
        conn.execute(
            text("""
                UPDATE betfair_odds
                SET shape = :shape,
                    shape_strength = :shape_strength,
                    shortened_fav = :shortened_fav,
                    late_money = :late_money
                WHERE record_id = :record_id
            """),
            {
                "shape": row["shape"],
                "shape_strength": row["shape_strength"],
                "shortened_fav": row["shortened_fav"],
                "late_money": row["late_money"],
                "record_id": row["record_id"],
            }
        )

print("✅ Database updated with new odds movement features.")
