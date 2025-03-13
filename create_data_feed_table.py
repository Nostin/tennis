import pandas as pd
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

# Create model_data_feed table (unchanged)
create_table_query = f"""
DROP TABLE IF EXISTS {TARGET_TABLE};
CREATE TABLE {TARGET_TABLE} (
    matchid TEXT PRIMARY KEY,
    elo_diff NUMERIC(8,2),
    surface_elo_diff NUMERIC(8,2),
    winner_fatigue NUMERIC(8,2),
    loser_fatigue NUMERIC(8,2),
    w_h2h_wins INTEGER,
    l_h2h_wins INTEGER,
    w_h2h_wins_surface INTEGER,
    l_h2h_wins_surface INTEGER,
    winner_win_pct_3m NUMERIC(5,2),
    loser_win_pct_3m NUMERIC(5,2),
    winner_win_pct_6m NUMERIC(5,2),
    loser_win_pct_6m NUMERIC(5,2),
    winner_hold_pct_total NUMERIC(5,2),
    winner_hold_pct_surface NUMERIC(5,2),
    winner_break_pct_total NUMERIC(5,2),
    winner_break_pct_surface NUMERIC(5,2),
    loser_hold_pct_total NUMERIC(5,2),
    loser_hold_pct_surface NUMERIC(5,2),
    loser_break_pct_total NUMERIC(5,2),
    loser_break_pct_surface NUMERIC(5,2),
    winner_dominance_roll NUMERIC(5,2),
    loser_dominance_roll NUMERIC(5,2),
    winner_recent_matches_30d INTEGER,
    loser_recent_matches_30d INTEGER,
    winner_age NUMERIC(5,2),
    loser_age NUMERIC(5,2),
    winner_tb_win_pct NUMERIC(5,2),
    loser_tb_win_pct NUMERIC(5,2),
    winner_home_advantage INTEGER,
    loser_home_advantage INTEGER,
    winner_odds NUMERIC(5,2),
    loser_odds NUMERIC(5,2),
    is_grand_slam INTEGER,
    is_atp250 INTEGER,
    is_masters_cup INTEGER,
    is_atp500 INTEGER,
    is_masters_1000 INTEGER,
    is_hard INTEGER,
    is_clay INTEGER,
    is_grass INTEGER,
    year INTEGER,
    target INTEGER
);
"""
with engine.connect() as conn:
    conn.execute(text(create_table_query))
    conn.commit()
print(f"✅ Created table {TARGET_TABLE}")

# Load data, excluding walkovers
with engine.connect() as conn:
    df = pd.read_sql(f"""
        SELECT matchid, date, surface, winner_overall_elo, loser_overall_elo,
               winner_surface_elo_hard, winner_surface_elo_clay, winner_surface_elo_grass,
               loser_surface_elo_hard, loser_surface_elo_clay, loser_surface_elo_grass,
               winner_fatigue, loser_fatigue, w_h2h_wins, l_h2h_wins,
               w_h2h_wins_hard, l_h2h_wins_hard, w_h2h_wins_clay, l_h2h_wins_clay,
               w_h2h_wins_grass, l_h2h_wins_grass,
               winner_win_pct_3m, loser_win_pct_3m, winner_win_pct_6m, loser_win_pct_6m,
               winner_hold_pct_total, winner_break_pct_total,
               winner_hold_pct_roll_hard, winner_break_pct_roll_hard,
               winner_hold_pct_roll_clay, winner_break_pct_roll_clay,
               winner_hold_pct_roll_grass, winner_break_pct_roll_grass,
               loser_hold_pct_total, loser_break_pct_total,
               loser_hold_pct_roll_hard, loser_break_pct_roll_hard,
               loser_hold_pct_roll_clay, loser_break_pct_roll_clay,
               loser_hold_pct_roll_grass, loser_break_pct_roll_grass,
               winner_home_advantage, loser_home_advantage,
               w_odds AS winner_odds, l_odds AS loser_odds,
               is_grand_slam, is_atp250, is_masters_cup, is_atp500, is_masters_1000,
               winner_age, loser_age, winner_recent_matches_30d, loser_recent_matches_30d,
               winner_tb_win_pct, loser_tb_win_pct
        FROM {SOURCE_TABLE}
        WHERE comment != 'Walkover' OR comment IS NULL
    """, conn)

# Process features (unchanged from previous)
df["elo_diff"] = df["winner_overall_elo"] - df["loser_overall_elo"]
df["surface_elo_diff"] = df.apply(
    lambda row: (row["winner_surface_elo_hard"] - row["loser_surface_elo_hard"]) if row["surface"] == "Hard" else
                (row["winner_surface_elo_clay"] - row["loser_surface_elo_clay"]) if row["surface"] == "Clay" else
                (row["winner_surface_elo_grass"] - row["loser_surface_elo_grass"]) if row["surface"] == "Grass" else 0,
    axis=1
)
df["w_h2h_wins_surface"] = df.apply(
    lambda row: row["w_h2h_wins_hard"] if row["surface"] == "Hard" else
                row["w_h2h_wins_clay"] if row["surface"] == "Clay" else
                row["w_h2h_wins_grass"] if row["surface"] == "Grass" else 0,
    axis=1
)
df["l_h2h_wins_surface"] = df.apply(
    lambda row: row["l_h2h_wins_hard"] if row["surface"] == "Hard" else
                row["l_h2h_wins_clay"] if row["surface"] == "Clay" else
                row["l_h2h_wins_grass"] if row["surface"] == "Grass" else 0,
    axis=1
)
df["winner_hold_pct_surface"] = df.apply(
    lambda row: row["winner_hold_pct_roll_hard"] if row["surface"] == "Hard" else
                row["winner_hold_pct_roll_clay"] if row["surface"] == "Clay" else
                row["winner_hold_pct_roll_grass"] if row["surface"] == "Grass" else 0,
    axis=1
)
df["winner_break_pct_surface"] = df.apply(
    lambda row: row["winner_break_pct_roll_hard"] if row["surface"] == "Hard" else
                row["winner_break_pct_roll_clay"] if row["surface"] == "Clay" else
                row["winner_break_pct_roll_grass"] if row["surface"] == "Grass" else 0,
    axis=1
)
df["loser_hold_pct_surface"] = df.apply(
    lambda row: row["loser_hold_pct_roll_hard"] if row["surface"] == "Hard" else
                row["loser_hold_pct_roll_clay"] if row["surface"] == "Clay" else
                row["loser_hold_pct_roll_grass"] if row["surface"] == "Grass" else 0,
    axis=1
)
df["loser_break_pct_surface"] = df.apply(
    lambda row: row["loser_break_pct_roll_hard"] if row["surface"] == "Hard" else
                row["loser_break_pct_roll_clay"] if row["surface"] == "Clay" else
                row["loser_break_pct_roll_grass"] if row["surface"] == "Grass" else 0,
    axis=1
)
df["winner_dominance_roll"] = df["winner_hold_pct_surface"] - df["winner_break_pct_surface"]
df["loser_dominance_roll"] = df["loser_hold_pct_surface"] - df["loser_break_pct_surface"]
df["is_hard"] = (df["surface"] == "Hard").astype(int)
df["is_clay"] = (df["surface"] == "Clay").astype(int)
df["is_grass"] = (df["surface"] == "Grass").astype(int)
df["year"] = pd.to_datetime(df["date"]).dt.year
df["target"] = 1

# -------------------------
# POPULATE TABLE
# -------------------------
print(f"Populating {TARGET_TABLE}...")
try:
    with engine.connect() as conn:
        for index, row in df.iterrows():
            insert_query = text(f"""
                INSERT INTO {TARGET_TABLE} (
                    matchid, elo_diff, surface_elo_diff, winner_fatigue, loser_fatigue,
                    w_h2h_wins, l_h2h_wins, w_h2h_wins_surface, l_h2h_wins_surface,
                    winner_win_pct_3m, loser_win_pct_3m, winner_win_pct_6m, loser_win_pct_6m,
                    winner_hold_pct_total, winner_hold_pct_surface, winner_break_pct_total,
                    winner_break_pct_surface, loser_hold_pct_total, loser_hold_pct_surface,
                    loser_break_pct_total, loser_break_pct_surface, winner_dominance_roll,
                    loser_dominance_roll, winner_recent_matches_30d, loser_recent_matches_30d,
                    winner_age, loser_age, winner_tb_win_pct, loser_tb_win_pct,
                    winner_home_advantage, loser_home_advantage, winner_odds, loser_odds,
                    is_grand_slam, is_atp250, is_masters_cup, is_atp500, is_masters_1000,
                    is_hard, is_clay, is_grass, year, target
                ) VALUES (
                    :matchid, :elo_diff, :surface_elo_diff, :winner_fatigue, :loser_fatigue,
                    :w_h2h_wins, :l_h2h_wins, :w_h2h_wins_surface, :l_h2h_wins_surface,
                    :winner_win_pct_3m, :loser_win_pct_3m, :winner_win_pct_6m, :loser_win_pct_6m,
                    :winner_hold_pct_total, :winner_hold_pct_surface, :winner_break_pct_total,
                    :winner_break_pct_surface, :loser_hold_pct_total, :loser_hold_pct_surface,
                    :loser_break_pct_total, :loser_break_pct_surface, :winner_dominance_roll,
                    :loser_dominance_roll, :winner_recent_matches_30d, :loser_recent_matches_30d,
                    :winner_age, :loser_age, :winner_tb_win_pct, :loser_tb_win_pct,
                    :winner_home_advantage, :loser_home_advantage, :winner_odds, :loser_odds,
                    :is_grand_slam, :is_atp250, :is_masters_cup, :is_atp500, :is_masters_1000,
                    :is_hard, :is_clay, :is_grass, :year, :target
                ) ON CONFLICT (matchid) DO NOTHING
            """)
            conn.execute(insert_query, {
                "matchid": row["matchid"],
                "elo_diff": row["elo_diff"],
                "surface_elo_diff": row["surface_elo_diff"],
                "winner_fatigue": row["winner_fatigue"],
                "loser_fatigue": row["loser_fatigue"],
                "w_h2h_wins": row["w_h2h_wins"],
                "l_h2h_wins": row["l_h2h_wins"],
                "w_h2h_wins_surface": row["w_h2h_wins_surface"],
                "l_h2h_wins_surface": row["l_h2h_wins_surface"],
                "winner_win_pct_3m": row["winner_win_pct_3m"],
                "loser_win_pct_3m": row["loser_win_pct_3m"],
                "winner_win_pct_6m": row["winner_win_pct_6m"],
                "loser_win_pct_6m": row["loser_win_pct_6m"],
                "winner_hold_pct_total": row["winner_hold_pct_total"],
                "winner_hold_pct_surface": row["winner_hold_pct_surface"],
                "winner_break_pct_total": row["winner_break_pct_total"],
                "winner_break_pct_surface": row["winner_break_pct_surface"],
                "loser_hold_pct_total": row["loser_hold_pct_total"],
                "loser_hold_pct_surface": row["loser_hold_pct_surface"],
                "loser_break_pct_total": row["loser_break_pct_total"],
                "loser_break_pct_surface": row["loser_break_pct_surface"],
                "winner_dominance_roll": row["winner_dominance_roll"],
                "loser_dominance_roll": row["loser_dominance_roll"],
                "winner_recent_matches_30d": row["winner_recent_matches_30d"],
                "loser_recent_matches_30d": row["loser_recent_matches_30d"],
                "winner_age": row["winner_age"],
                "loser_age": row["loser_age"],
                "winner_tb_win_pct": row["winner_tb_win_pct"],
                "loser_tb_win_pct": row["loser_tb_win_pct"],
                "winner_home_advantage": row["winner_home_advantage"],
                "loser_home_advantage": row["loser_home_advantage"],
                "winner_odds": row["winner_odds"],
                "loser_odds": row["loser_odds"],
                "is_grand_slam": row["is_grand_slam"],
                "is_atp250": row["is_atp250"],
                "is_masters_cup": row["is_masters_cup"],
                "is_atp500": row["is_atp500"],
                "is_masters_1000": row["is_masters_1000"],
                "is_hard": row["is_hard"],
                "is_clay": row["is_clay"],
                "is_grass": row["is_grass"],
                "year": row["year"],
                "target": row["target"]
            })
        conn.commit()
    print(f"✅ Populated {TARGET_TABLE} with {len(df)} rows (walkovers excluded)!")
except Exception as e:
    print(f"Error populating table: {e}")
    raise