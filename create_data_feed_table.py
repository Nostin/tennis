import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# Configuration
DB_NAME = "tennis"
DB_USER = "seanthompson"
DB_PASS = ""
DB_HOST = "localhost"
DB_PORT = "5432"
SOURCE_TABLE = "matched_atp_records"
TARGET_TABLE = "model_data_feed"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Create table (unchanged)
create_table_query = f"""
DROP TABLE IF EXISTS {TARGET_TABLE};
CREATE TABLE {TARGET_TABLE} (
    matchid TEXT PRIMARY KEY,
    date DATE,
    surface TEXT,
    player1_name TEXT,
    player2_name TEXT,
    target INTEGER,
    elo_diff NUMERIC(8,2),
    surface_elo_diff NUMERIC(8,2),
    tournament_fatigue_diff INTEGER,
    odds_diff NUMERIC(8,2),
    h2h_wins_diff INTEGER,
    win_pct_3m_diff NUMERIC(5,2),
    win_pct_6m_diff NUMERIC(5,2),
    dominance_roll_diff NUMERIC(5,2),
    recent_matches_30d_diff INTEGER,
    days_since_last_diff INTEGER,
    tournament_strength INTEGER,
    ace_rate_3m_diff NUMERIC(5,2),
    ace_rate_6m_diff NUMERIC(5,2),
    df_rate_3m_diff NUMERIC(5,2),
    df_rate_6m_diff NUMERIC(5,2),
    bpsaved_rate_3m_diff NUMERIC(5,2),
    bpsaved_rate_6m_diff NUMERIC(5,2),
    bpfaced_rate_3m_diff NUMERIC(5,2),
    bpfaced_rate_6m_diff NUMERIC(5,2),
    first_serve_pct_3m_diff NUMERIC(5,2),
    first_serve_win_pct_3m_diff NUMERIC(5,2),
    second_serve_win_pct_3m_diff NUMERIC(5,2),
    first_serve_win_pct_3m_surface_diff NUMERIC(5,2),
    second_serve_win_pct_3m_surface_diff NUMERIC(5,2),
    recent_form_6matches_diff NUMERIC(5,2),
    avg_elo_faced_diff NUMERIC(8,2),
    elo_first_serve_interaction NUMERIC(8,2),
    avgw NUMERIC(8,2),
    avgl NUMERIC(8,2)
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
            p1_name AS player1_name, p2_name AS player2_name, 
            winner_name, avgw, avgl,
            winner_overall_elo, loser_overall_elo,
            winner_surface_elo, loser_surface_elo,
            w_h2h_wins, l_h2h_wins,
            winner_win_pct_3m, loser_win_pct_3m,
            winner_win_pct_6m, loser_win_pct_6m,
            winner_hold_pct_total, loser_hold_pct_total,
            winner_break_pct_total, loser_break_pct_total,
            winner_recent_matches_30d, loser_recent_matches_30d,
            winner_days_since_last, loser_days_since_last,
            winner_avg_elo_faced, loser_avg_elo_faced,
            series,
            p1_ace_rate_3m, p1_ace_rate_6m, p2_ace_rate_3m, p2_ace_rate_6m,
            p1_df_rate_3m, p1_df_rate_6m, p2_df_rate_3m, p2_df_rate_6m,
            p1_bpsaved_rate_3m, p1_bpsaved_rate_6m, p2_bpsaved_rate_3m, p2_bpsaved_rate_6m,
            p1_bpfaced_rate_3m, p1_bpfaced_rate_6m, p2_bpfaced_rate_3m, p2_bpfaced_rate_6m,
            p1_first_serve_pct_3m, p2_first_serve_pct_3m,
            p1_first_serve_win_pct_3m, p2_first_serve_win_pct_3m,
            p1_second_serve_win_pct_3m, p2_second_serve_win_pct_3m,
            p1_first_serve_win_pct_3m_hard, p2_first_serve_win_pct_3m_hard,
            p1_second_serve_win_pct_3m_hard, p2_second_serve_win_pct_3m_hard,
            p1_first_serve_win_pct_3m_clay, p2_first_serve_win_pct_3m_clay,
            p1_second_serve_win_pct_3m_clay, p2_second_serve_win_pct_3m_clay,
            p1_first_serve_win_pct_3m_grass, p2_first_serve_win_pct_3m_grass,
            p1_second_serve_win_pct_3m_grass, p2_second_serve_win_pct_3m_grass,
            p1_recent_form_6matches, p2_recent_form_6matches,
            p1_tournament_minutes, p2_tournament_minutes
        FROM {SOURCE_TABLE}
        WHERE ("comment" != 'Walkover' OR "comment" IS NULL)
        AND winner_overall_elo IS NOT NULL 
        AND loser_overall_elo IS NOT NULL
        ORDER BY date ASC
    """, conn)

# Assign p1/p2 stats based on winner/loser
df["p1_elo"] = df.apply(lambda row: row["winner_overall_elo"] if row["player1_name"] == row["winner_name"] else row["loser_overall_elo"], axis=1)
df["p2_elo"] = df.apply(lambda row: row["loser_overall_elo"] if row["player1_name"] == row["winner_name"] else row["winner_overall_elo"], axis=1)
df["p1_surface_elo"] = df.apply(lambda row: row["winner_surface_elo"] if row["player1_name"] == row["winner_name"] else row["loser_surface_elo"], axis=1)
df["p2_surface_elo"] = df.apply(lambda row: row["loser_surface_elo"] if row["player1_name"] == row["winner_name"] else row["winner_surface_elo"], axis=1)
df["p1_h2h_wins"] = df.apply(lambda row: row["w_h2h_wins"] if row["player1_name"] == row["winner_name"] else row["l_h2h_wins"], axis=1)
df["p2_h2h_wins"] = df.apply(lambda row: row["l_h2h_wins"] if row["player1_name"] == row["winner_name"] else row["w_h2h_wins"], axis=1)
df["p1_win_pct_3m"] = df.apply(lambda row: row["winner_win_pct_3m"] if row["player1_name"] == row["winner_name"] else row["loser_win_pct_3m"], axis=1)
df["p2_win_pct_3m"] = df.apply(lambda row: row["loser_win_pct_3m"] if row["player1_name"] == row["winner_name"] else row["winner_win_pct_3m"], axis=1)
df["p1_win_pct_6m"] = df.apply(lambda row: row["winner_win_pct_6m"] if row["player1_name"] == row["winner_name"] else row["loser_win_pct_6m"], axis=1)
df["p2_win_pct_6m"] = df.apply(lambda row: row["loser_win_pct_6m"] if row["player1_name"] == row["winner_name"] else row["winner_win_pct_6m"], axis=1)
df["p1_hold_pct_total"] = df.apply(lambda row: row["winner_hold_pct_total"] if row["player1_name"] == row["winner_name"] else row["loser_hold_pct_total"], axis=1)
df["p2_hold_pct_total"] = df.apply(lambda row: row["loser_hold_pct_total"] if row["player1_name"] == row["winner_name"] else row["winner_hold_pct_total"], axis=1)
df["p1_break_pct_total"] = df.apply(lambda row: row["winner_break_pct_total"] if row["player1_name"] == row["winner_name"] else row["loser_break_pct_total"], axis=1)
df["p2_break_pct_total"] = df.apply(lambda row: row["loser_break_pct_total"] if row["player1_name"] == row["winner_name"] else row["winner_break_pct_total"], axis=1)
df["p1_recent_matches_30d"] = df.apply(lambda row: row["winner_recent_matches_30d"] if row["player1_name"] == row["winner_name"] else row["loser_recent_matches_30d"], axis=1)
df["p2_recent_matches_30d"] = df.apply(lambda row: row["loser_recent_matches_30d"] if row["player1_name"] == row["winner_name"] else row["winner_recent_matches_30d"], axis=1)
df["p1_days_since_last"] = df.apply(lambda row: row["winner_days_since_last"] if row["player1_name"] == row["winner_name"] else row["loser_days_since_last"], axis=1)
df["p2_days_since_last"] = df.apply(lambda row: row["loser_days_since_last"] if row["player1_name"] == row["winner_name"] else row["winner_days_since_last"], axis=1)
df["p1_avg_elo_faced"] = df.apply(lambda row: row["winner_avg_elo_faced"] if row["player1_name"] == row["winner_name"] else row["loser_avg_elo_faced"], axis=1)
df["p2_avg_elo_faced"] = df.apply(lambda row: row["loser_avg_elo_faced"] if row["player1_name"] == row["winner_name"] else row["winner_avg_elo_faced"], axis=1)

# Define target (1 if p1 is winner, 0 if p2 is winner)
df["target"] = (df["player1_name"] == df["winner_name"]).astype(int)

# Feature Engineering (p1 - p2)
df["elo_diff"] = df["p1_elo"] - df["p2_elo"]
df["surface_elo_diff"] = df["p1_surface_elo"] - df["p2_surface_elo"]
df["tournament_fatigue_diff"] = df["p1_tournament_minutes"].fillna(0) - df["p2_tournament_minutes"].fillna(0)
df["odds_diff"] = df["avgw"] - df["avgl"]
df["h2h_wins_diff"] = df["p1_h2h_wins"].fillna(0) - df["p2_h2h_wins"].fillna(0)
df["win_pct_3m_diff"] = df["p1_win_pct_3m"].fillna(0) - df["p2_win_pct_3m"].fillna(0)
df["win_pct_6m_diff"] = df["p1_win_pct_6m"].fillna(0) - df["p2_win_pct_6m"].fillna(0)
df["dominance_roll_diff"] = (df["p1_hold_pct_total"].fillna(0) - df["p1_break_pct_total"].fillna(0)) - \
                            (df["p2_hold_pct_total"].fillna(0) - df["p2_break_pct_total"].fillna(0))
df["recent_matches_30d_diff"] = df["p1_recent_matches_30d"].fillna(0) - df["p2_recent_matches_30d"].fillna(0)
df["days_since_last_diff"] = df["p1_days_since_last"].fillna(0) - df["p2_days_since_last"].fillna(0)
df["ace_rate_3m_diff"] = df["p1_ace_rate_3m"].fillna(0) - df["p2_ace_rate_3m"].fillna(0)
df["ace_rate_6m_diff"] = df["p1_ace_rate_6m"].fillna(0) - df["p2_ace_rate_6m"].fillna(0)
df["df_rate_3m_diff"] = df["p1_df_rate_3m"].fillna(0) - df["p2_df_rate_3m"].fillna(0)
df["df_rate_6m_diff"] = df["p1_df_rate_6m"].fillna(0) - df["p2_df_rate_6m"].fillna(0)
df["bpsaved_rate_3m_diff"] = df["p1_bpsaved_rate_3m"].fillna(0) - df["p2_bpsaved_rate_3m"].fillna(0)
df["bpsaved_rate_6m_diff"] = df["p1_bpsaved_rate_6m"].fillna(0) - df["p2_bpsaved_rate_6m"].fillna(0)
df["bpfaced_rate_3m_diff"] = df["p1_bpfaced_rate_3m"].fillna(0) - df["p2_bpfaced_rate_3m"].fillna(0)
df["bpfaced_rate_6m_diff"] = df["p1_bpfaced_rate_6m"].fillna(0) - df["p2_bpfaced_rate_6m"].fillna(0)
df["first_serve_pct_3m_diff"] = df["p1_first_serve_pct_3m"].fillna(0) - df["p2_first_serve_pct_3m"].fillna(0)
df["first_serve_win_pct_3m_diff"] = df["p1_first_serve_win_pct_3m"].fillna(0) - df["p2_first_serve_win_pct_3m"].fillna(0)
df["second_serve_win_pct_3m_diff"] = df["p1_second_serve_win_pct_3m"].fillna(0) - df["p2_second_serve_win_pct_3m"].fillna(0)

# Surface-specific serve stats
for surface in ["Hard", "Clay", "Grass"]:
    mask = df["surface"] == surface
    df.loc[mask, "first_serve_win_pct_3m_surface_diff"] = (
        df.loc[mask, f"p1_first_serve_win_pct_3m_{surface.lower()}"].fillna(0) -
        df.loc[mask, f"p2_first_serve_win_pct_3m_{surface.lower()}"].fillna(0)
    )
    df.loc[mask, "second_serve_win_pct_3m_surface_diff"] = (
        df.loc[mask, f"p1_second_serve_win_pct_3m_{surface.lower()}"].fillna(0) -
        df.loc[mask, f"p2_second_serve_win_pct_3m_{surface.lower()}"].fillna(0)
    )

df["recent_form_6matches_diff"] = df["p1_recent_form_6matches"].fillna(0) - df["p2_recent_form_6matches"].fillna(0)
df["avg_elo_faced_diff"] = df["p1_avg_elo_faced"].fillna(0) - df["p2_avg_elo_faced"].fillna(0)
df["elo_first_serve_interaction"] = df["elo_diff"] * df["first_serve_win_pct_3m_diff"]

# Tournament Strength
tournament_strength_map = {"Grand Slam": 5, "Masters 1000": 4, "ATP500": 3, "ATP250": 2, "Masters Cup": 1}
df["tournament_strength"] = df["series"].map(tournament_strength_map).fillna(0).astype(int)

# Randomize Player 1 and Player 2
mask = np.random.rand(len(df)) < 0.5
df.loc[mask, ["player1_name", "player2_name"]] = df.loc[mask, ["player2_name", "player1_name"]].values
df.loc[mask, "target"] = 1 - df.loc[mask, "target"]
diff_columns = [
    "elo_diff", "surface_elo_diff", "tournament_fatigue_diff", "odds_diff", "h2h_wins_diff",
    "win_pct_3m_diff", "win_pct_6m_diff", "dominance_roll_diff", "recent_matches_30d_diff", "days_since_last_diff",
    "ace_rate_3m_diff", "ace_rate_6m_diff", "df_rate_3m_diff", "df_rate_6m_diff",
    "bpsaved_rate_3m_diff", "bpsaved_rate_6m_diff", "bpfaced_rate_3m_diff", "bpfaced_rate_6m_diff",
    "first_serve_pct_3m_diff", "first_serve_win_pct_3m_diff", "second_serve_win_pct_3m_diff",
    "first_serve_win_pct_3m_surface_diff", "second_serve_win_pct_3m_surface_diff",
    "recent_form_6matches_diff", "avg_elo_faced_diff", "elo_first_serve_interaction"
]
df.loc[mask, diff_columns] *= -1

# Handle NaNs
numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
df[numeric_cols] = df[numeric_cols].fillna(0)

# Select final columns
model_data_feed = df[[
    "matchid", "date", "surface", "player1_name", "player2_name", "target",
    "elo_diff", "surface_elo_diff", "tournament_fatigue_diff", "odds_diff", "h2h_wins_diff",
    "win_pct_3m_diff", "win_pct_6m_diff", "dominance_roll_diff", "recent_matches_30d_diff", "days_since_last_diff",
    "tournament_strength", "ace_rate_3m_diff", "ace_rate_6m_diff", "df_rate_3m_diff", "df_rate_6m_diff",
    "bpsaved_rate_3m_diff", "bpsaved_rate_6m_diff", "bpfaced_rate_3m_diff", "bpfaced_rate_6m_diff",
    "first_serve_pct_3m_diff", "first_serve_win_pct_3m_diff", "second_serve_win_pct_3m_diff",
    "first_serve_win_pct_3m_surface_diff", "second_serve_win_pct_3m_surface_diff",
    "recent_form_6matches_diff", "avg_elo_faced_diff", "elo_first_serve_interaction",
    "avgw", "avgl"
]]

# Insert data
with engine.connect() as conn:
    model_data_feed.to_sql(TARGET_TABLE, conn, if_exists="append", index=False, method="multi")
print(f"✅ Populated {TARGET_TABLE} with {len(model_data_feed)} rows!")