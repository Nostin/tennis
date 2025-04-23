import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from sqlalchemy import text
from db_connect import get_engine

# Config
engine = get_engine()
TABLE_NAME = "matched_atp_records"

# -------------------------
# Schema update: Add columns
# -------------------------
print("🛠️ Updating schema...")

with engine.begin() as conn:
    conn.execute(text(f"""
        ALTER TABLE {TABLE_NAME}
        DROP COLUMN IF EXISTS f_winner_home_adv,
        DROP COLUMN IF EXISTS f_loser_home_adv;
    """))

    conn.execute(text(f"""
        ALTER TABLE {TABLE_NAME}
        ADD COLUMN IF NOT EXISTS f_winner_home_adv INT,
        ADD COLUMN IF NOT EXISTS f_loser_home_adv INT;
    """))

# -------------------------
# Load necessary columns
# -------------------------
print("🔍 Loading match data...")

with engine.connect() as conn:
    df = pd.read_sql(text(f"""
        SELECT matchid, winner_ioc, loser_ioc, tournament_ioc
        FROM {TABLE_NAME}
    """), conn)

# -------------------------
# Compute home advantage
# -------------------------
print("⚙️ Calculating home advantage...")

df["f_winner_home_adv"] = (df["winner_ioc"] == df["tournament_ioc"]).astype(int)
df["f_loser_home_adv"] = (df["loser_ioc"] == df["tournament_ioc"]).astype(int)

# -------------------------
# Update DB
# -------------------------
print("📝 Writing results to DB...")

with engine.begin() as conn:
    for _, row in df.iterrows():
        conn.execute(text(f"""
            UPDATE {TABLE_NAME}
            SET f_winner_home_adv = :w_adv,
                f_loser_home_adv = :l_adv
            WHERE matchid = :id
        """), {
            "id": row["matchid"],
            "w_adv": int(row["f_winner_home_adv"]),
            "l_adv": int(row["f_loser_home_adv"]),
        })

print("✅ Done. Home advantage flags added.")
