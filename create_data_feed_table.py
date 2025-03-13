import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.preprocessing import StandardScaler

# -------------------------
# CONFIGURATION
# -------------------------
DB_NAME = "tennis"
DB_USER = "seanthompson"
DB_PASS = ""
DB_HOST = "localhost"
DB_PORT = "5432"
SOURCE_TABLE = "matched_atp_records"
TARGET_TABLE = "model_data_feed"

# -------------------------
# CONNECT TO POSTGRESQL
# -------------------------
print("Connecting to the database...")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Create table
create_table_query = f"""
DROP TABLE IF EXISTS {TARGET_TABLE};
CREATE TABLE {TARGET_TABLE} (
    matchid INT PRIMARY KEY,
    date DATE,
    surface TEXT,
    player1_name TEXT,
    player2_name TEXT,
    target INTEGER,
    elo_diff NUMERIC(8,2),
    surface_elo_diff NUMERIC(8,2),
    fatigue_diff NUMERIC(8,2),
    odds_diff NUMERIC(8,2),
    h2h_wins_diff INTEGER,
    win_pct_3m_diff NUMERIC(5,2),
    dominance_roll_diff NUMERIC(5,2),
    recent_matches_30d_diff INTEGER,
    days_since_last_diff INTEGER,
    tournament_strength INTEGER
);
"""
with engine.connect() as conn:
    conn.execute(text(create_table_query))
    conn.commit()
print(f"✅ Created table {TARGET_TABLE}")

# Load data
with engine.connect() as conn:
    df = pd.read_sql(f"""
        SELECT 
            matchid, date, surface, 
            winner_name, loser_name, avgw, avgl,
            winner_overall_elo, loser_overall_elo, winner_surface_elo, loser_surface_elo,
            winner_fatigue, loser_fatigue, w_h2h_wins, l_h2h_wins,
            winner_win_pct_3m, loser_win_pct_3m, winner_hold_pct_total, loser_hold_pct_total,
            winner_break_pct_total, loser_break_pct_total, winner_recent_matches_30d, loser_recent_matches_30d,
            winner_days_since_last, loser_days_since_last, series
        FROM {SOURCE_TABLE}
        WHERE ("comment" != 'Walkover' OR "comment" IS NULL)
        AND winner_overall_elo IS NOT NULL 
        AND loser_overall_elo IS NOT NULL
        ORDER BY date
    """, conn)

# Initial assignment
df["player1_name"] = df["winner_name"]
df["player2_name"] = df["loser_name"]
df["target"] = 1

# Feature Engineering
df["elo_diff"] = df["winner_overall_elo"] - df["loser_overall_elo"]
df["surface_elo_diff"] = df["winner_surface_elo"] - df["loser_surface_elo"]
df["fatigue_diff"] = df["winner_fatigue"].fillna(df["winner_fatigue"].median()) - df["loser_fatigue"].fillna(df["loser_fatigue"].median())
df["odds_diff"] = df["avgw"] - df["avgl"]
df["h2h_wins_diff"] = df["w_h2h_wins"].fillna(0) - df["l_h2h_wins"].fillna(0)
df["win_pct_3m_diff"] = df["winner_win_pct_3m"].fillna(0) - df["loser_win_pct_3m"].fillna(0)
df["dominance_roll_diff"] = (df["winner_hold_pct_total"].fillna(0) - df["winner_break_pct_total"].fillna(0)) - \
                            (df["loser_hold_pct_total"].fillna(0) - df["loser_break_pct_total"].fillna(0))
df["recent_matches_30d_diff"] = df["winner_recent_matches_30d"].fillna(0) - df["loser_recent_matches_30d"].fillna(0)
df["days_since_last_diff"] = df["winner_days_since_last"].fillna(0) - df["loser_days_since_last"].fillna(0)

# Tournament Strength
tournament_strength_map = {"Grand Slam": 5, "Masters 1000": 4, "ATP500": 3, "ATP250": 2, "Masters Cup": 1}
df["tournament_strength"] = df["series"].map(tournament_strength_map).fillna(0).astype(int)

# Randomize Player 1 and Player 2
mask = np.random.rand(len(df)) < 0.5
df.loc[mask, ["player1_name", "player2_name"]] = df.loc[mask, ["player2_name", "player1_name"]].values
df.loc[mask, "target"] = 0
diff_columns = [
    "elo_diff", "surface_elo_diff", "fatigue_diff", "odds_diff", "h2h_wins_diff",
    "win_pct_3m_diff", "dominance_roll_diff", "recent_matches_30d_diff", "days_since_last_diff"
]
df.loc[mask, diff_columns] *= -1

# Scale features
scaler = StandardScaler()
numeric_cols = diff_columns + ["tournament_strength"]
df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

# Select final columns
model_data_feed = df[[
    "matchid", "date", "surface", "player1_name", "player2_name", "target",
    "elo_diff", "surface_elo_diff", "fatigue_diff", "odds_diff", "h2h_wins_diff",
    "win_pct_3m_diff", "dominance_roll_diff", "recent_matches_30d_diff", "days_since_last_diff",
    "tournament_strength"
]]

# Insert data
with engine.connect() as conn:
    model_data_feed.to_sql(TARGET_TABLE, conn, if_exists="append", index=False, method="multi")
print(f"✅ Populated {TARGET_TABLE} with {len(model_data_feed)} rows!")