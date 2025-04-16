import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from sqlalchemy import text
from db_connect import get_engine

engine = get_engine()
TABLE_NAME = "matched_atp_records"

# -------------------------
# Load match data
# -------------------------
with engine.connect() as conn:
    df = pd.read_sql(f"""
        SELECT matchid, tourney_id, date, tournament, winner_name, loser_name, minutes
        FROM {TABLE_NAME}
        ORDER BY date ASC
    """, conn)

df["date"] = pd.to_datetime(df["date"])
df["minutes"] = df["minutes"].fillna(0)

# -------------------------
# Create long format for player tracking
# -------------------------
players_df = pd.concat([
    df[["matchid", "date", "tourney_id", "winner_name", "minutes"]].rename(columns={"winner_name": "player"}),
    df[["matchid", "date", "tourney_id", "loser_name", "minutes"]].rename(columns={"loser_name": "player"})
])

players_df = players_df.sort_values(["player", "tourney_id", "date", "matchid"]).copy()

# Assign match order per player per tournament
players_df["match_order"] = players_df.groupby(["player", "tourney_id"]).cumcount()

# Filter valid-minute matches for cumsum
valid_minutes = players_df[players_df["minutes"] > 0].copy()
valid_minutes["prior_minutes"] = valid_minutes.groupby(["player", "tourney_id"])["minutes"].cumsum()
valid_minutes["match_order"] = valid_minutes.groupby(["player", "tourney_id"]).cumcount() + 1  # offset by 1

# Merge back to full data
players_df = players_df.merge(
    valid_minutes[["player", "tourney_id", "match_order", "prior_minutes"]],
    on=["player", "tourney_id", "match_order"],
    how="left"
)

players_df["cumulative_minutes"] = players_df["prior_minutes"].fillna(0)
players_df.drop(columns=["prior_minutes", "match_order"], inplace=True)

# -------------------------
# Merge into main match DataFrame
# -------------------------
df = df.merge(
    players_df[["matchid", "player", "cumulative_minutes"]],
    left_on=["matchid", "winner_name"],
    right_on=["matchid", "player"],
    how="left"
).rename(columns={"cumulative_minutes": "f_winner_tourney_minutes"}).drop(columns=["player"])

df = df.merge(
    players_df[["matchid", "player", "cumulative_minutes"]],
    left_on=["matchid", "loser_name"],
    right_on=["matchid", "player"],
    how="left"
).rename(columns={"cumulative_minutes": "f_loser_tourney_minutes"}).drop(columns=["player"])

# Fill remaining NaNs
df["f_winner_tourney_minutes"] = df["f_winner_tourney_minutes"].fillna(0)
df["f_loser_tourney_minutes"] = df["f_loser_tourney_minutes"].fillna(0)

# -------------------------
# Write to database
# -------------------------
with engine.begin() as conn:
    conn.execute(text("""
        ALTER TABLE matched_atp_records
        DROP COLUMN IF EXISTS f_winner_tourney_minutes,
        DROP COLUMN IF EXISTS f_loser_tourney_minutes;
    """))

    conn.execute(text("""
        ALTER TABLE matched_atp_records
        ADD COLUMN IF NOT EXISTS f_winner_tourney_minutes INT,
        ADD COLUMN IF NOT EXISTS f_loser_tourney_minutes INT;
    """))

    conn.execute(text("""
        CREATE TEMP TABLE temp_fatigue_update (
            matchid INT,
            f_winner_tourney_minutes INT,
            f_loser_tourney_minutes INT
        ) ON COMMIT DROP;
    """))

    fatigue_data = df[["matchid", "f_winner_tourney_minutes", "f_loser_tourney_minutes"]].values.tolist()
    conn.execute(
        text("""
            INSERT INTO temp_fatigue_update (matchid, f_winner_tourney_minutes, f_loser_tourney_minutes)
            VALUES (:matchid, :w_minutes, :l_minutes)
        """),
        [{"matchid": row[0], "w_minutes": int(row[1]), "l_minutes": int(row[2])} for row in fatigue_data]
    )

    conn.execute(text(f"""
        UPDATE {TABLE_NAME}
        SET
            f_winner_tourney_minutes = temp.f_winner_tourney_minutes,
            f_loser_tourney_minutes = temp.f_loser_tourney_minutes
        FROM temp_fatigue_update temp
        WHERE {TABLE_NAME}.matchid = temp.matchid
    """))

print("✅ Tournament fatigue minutes correctly updated — all Round 1 entries now start at 0.")
