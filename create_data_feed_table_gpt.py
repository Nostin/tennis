import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# -------------------------
# CONFIGURATION
# -------------------------
DB_NAME = "tennis"
DB_USER = "seanthompson"
DB_PASS = ""  # Add password if necessary
DB_HOST = "localhost"
DB_PORT = "5432"
SOURCE_TABLE = "matched_atp_records"
TARGET_TABLE = "model_data_feed"

# -------------------------
# CONNECT TO POSTGRESQL
# -------------------------
print("Connecting to the database...")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Create model_data_feed table
create_table_query = f"""
DROP TABLE IF EXISTS {TARGET_TABLE};
CREATE TABLE {TARGET_TABLE} (
    matchid TEXT PRIMARY KEY,
    date DATE,
    surface TEXT,
    player1_name TEXT,
    player2_name TEXT,
    target INTEGER,  -- 1 if Player 1 wins, 0 if Player 2 wins
    elo_diff NUMERIC(8,2),
    surface_elo_diff NUMERIC(8,2),
    fatigue_diff NUMERIC(8,2),
    odds_diff NUMERIC(8,2),
    h2h_wins_diff INTEGER,
    win_pct_3m_diff NUMERIC(5,2),
    dominance_roll_diff NUMERIC(5,2),
    recent_matches_30d_diff INTEGER,
    days_since_last_diff INTEGER,
    tournament_strength INTEGER,
    ace_pct_diff NUMERIC(5,2),
    df_pct_diff NUMERIC(5,2),
    first_serve_pct_diff NUMERIC(5,2),
    first_serve_win_pct_diff NUMERIC(5,2),
    second_serve_win_pct_diff NUMERIC(5,2),
    bp_saved_pct_diff NUMERIC(5,2)
);
"""
with engine.connect() as conn:
    conn.execute(text(create_table_query))
    conn.commit()
print(f"✅ Created table {TARGET_TABLE}")

# Load data (updated to use Series instead of flag columns)
with engine.connect() as conn:
    df = pd.read_sql(f"""
        SELECT 
            matchid, 
            date, 
            surface, 
            winner_name, 
            loser_name,
            winner_overall_elo, 
            loser_overall_elo,
            winner_surface_elo, 
            loser_surface_elo,
            winner_fatigue, 
            loser_fatigue, 
            w_h2h_wins, 
            l_h2h_wins,
            winner_win_pct_3m, 
            loser_win_pct_3m,
            winner_hold_pct_total, 
            winner_break_pct_total,
            loser_hold_pct_total, 
            loser_break_pct_total,
            winner_recent_matches_30d, 
            loser_recent_matches_30d,
            avgw, 
            avgl, 
            winner_days_since_last, 
            loser_days_since_last,
            series,
            w_ace, 
            w_df, 
            w_svpt, 
            w_1stin, 
            w_1stwon, 
            w_2ndwon, 
            w_bpsaved, 
            w_bpfaced,
            l_ace, 
            l_df, 
            l_svpt, 
            l_1stin, 
            l_1stwon, 
            l_2ndwon, 
            l_bpsaved, 
            l_bpfaced
        FROM {SOURCE_TABLE}
        WHERE (comment != 'Walkover' OR comment IS NULL)
        AND winner_overall_elo IS NOT NULL 
        AND loser_overall_elo IS NOT NULL
    """, conn)

# Initial assignment: Player 1 = winner, Player 2 = loser
df["player1_name"] = df["winner_name"]
df["player2_name"] = df["loser_name"]
df["target"] = 1  # Player 1 wins initially

# Feature Engineering: Differences (Player 1 - Player 2)
df["elo_diff"] = df["winner_overall_elo"] - df["loser_overall_elo"]
df["surface_elo_diff"] = df["winner_surface_elo"] - df["loser_surface_elo"]
df["fatigue_diff"] = df["winner_fatigue"] - df["loser_fatigue"]
df["odds_diff"] = df["avgw"] - df["avgl"]
df["h2h_wins_diff"] = df["w_h2h_wins"] - df["l_h2h_wins"]
df["win_pct_3m_diff"] = df["winner_win_pct_3m"] - df["loser_win_pct_3m"]
df["dominance_roll_diff"] = (df["winner_hold_pct_total"] - df["winner_break_pct_total"]) - \
                            (df["loser_hold_pct_total"] - df["loser_break_pct_total"])
df["recent_matches_30d_diff"] = df["winner_recent_matches_30d"] - df["loser_recent_matches_30d"]
df["days_since_last_diff"] = df["winner_days_since_last"] - df["loser_days_since_last"]

# Tournament Strength (mapped from Series)
tournament_strength_map = {
    "Grand Slam": 5,
    "Masters 1000": 4,
    "ATP500": 3,
    "ATP250": 2,
    "Masters Cup": 1  # Assuming Masters Cup is lower tier, adjust if needed
}
df["tournament_strength"] = df["series"].map(tournament_strength_map).fillna(0).astype(int)

# Serve/Return Stats
df["w_ace_pct"] = df["w_ace"] / df["w_svpt"].replace(0, 1) * 100
df["l_ace_pct"] = df["l_ace"] / df["l_svpt"].replace(0, 1) * 100
df["ace_pct_diff"] = df["w_ace_pct"] - df["l_ace_pct"]

df["w_df_pct"] = df["w_df"] / df["w_svpt"].replace(0, 1) * 100
df["l_df_pct"] = df["l_df"] / df["l_svpt"].replace(0, 1) * 100
df["df_pct_diff"] = df["w_df_pct"] - df["l_df_pct"]

df["w_first_serve_pct"] = df["w_1stin"] / df["w_svpt"].replace(0, 1) * 100
df["l_first_serve_pct"] = df["l_1stin"] / df["l_svpt"].replace(0, 1) * 100
df["first_serve_pct_diff"] = df["w_first_serve_pct"] - df["l_first_serve_pct"]

df["w_first_serve_win_pct"] = df["w_1stwon"] / df["w_1stin"].replace(0, 1) * 100
df["l_first_serve_win_pct"] = df["l_1stwon"] / df["l_1stin"].replace(0, 1) * 100
df["first_serve_win_pct_diff"] = df["w_first_serve_win_pct"] - df["l_first_serve_win_pct"]

df["w_second_serve_win_pct"] = df["w_2ndwon"] / (df["w_svpt"] - df["w_1stin"]).replace(0, 1) * 100
df["l_second_serve_win_pct"] = df["l_2ndwon"] / (df["l_svpt"] - df["l_1stin"]).replace(0, 1) * 100
df["second_serve_win_pct_diff"] = df["w_second_serve_win_pct"] - df["l_second_serve_win_pct"]

df["w_bp_saved_pct"] = df["w_bpsaved"] / df["w_bpfaced"].replace(0, 1) * 100
df["l_bp_saved_pct"] = df["l_bpsaved"] / df["l_bpfaced"].replace(0, 1) * 100
df["bp_saved_pct_diff"] = df["w_bp_saved_pct"] - df["l_bp_saved_pct"]

# Randomize Player 1 and Player 2
mask = np.random.rand(len(df)) < 0.5  # Randomly select ~50% of rows to swap
df.loc[mask, ["player1_name", "player2_name"]] = df.loc[mask, ["player2_name", "player1_name"]].values
df.loc[mask, "target"] = 0  # Player 2 wins in swapped rows
diff_columns = [
    "elo_diff", "surface_elo_diff", "fatigue_diff", "odds_diff", "h2h_wins_diff",
    "win_pct_3m_diff", "dominance_roll_diff", "recent_matches_30d_diff", "days_since_last_diff",
    "ace_pct_diff", "df_pct_diff", "first_serve_pct_diff", "first_serve_win_pct_diff",
    "second_serve_win_pct_diff", "bp_saved_pct_diff"
]
df.loc[mask, diff_columns] *= -1  # Flip the sign of differences for swapped rows

# Handle NaNs
df = df.fillna(0)

# Select final columns
model_data_feed = df[[
    "matchid", "date", "surface", "player1_name", "player2_name", "target",
    "elo_diff", "surface_elo_diff", "fatigue_diff", "odds_diff", "h2h_wins_diff",
    "win_pct_3m_diff", "dominance_roll_diff", "recent_matches_30d_diff", "days_since_last_diff",
    "tournament_strength", "ace_pct_diff", "df_pct_diff", "first_serve_pct_diff",
    "first_serve_win_pct_diff", "second_serve_win_pct_diff", "bp_saved_pct_diff"
]]

# Insert data
with engine.connect() as conn:
    model_data_feed.to_sql(TARGET_TABLE, conn, if_exists="append", index=False, method="multi")
print(f"✅ Populated {TARGET_TABLE} with {len(model_data_feed)} rows!")