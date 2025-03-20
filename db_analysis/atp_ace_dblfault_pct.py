import sys
import os
import pandas as pd
from sqlalchemy import text
from datetime import timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Adjust path for db_connect
try:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from db_connect import get_engine
except NameError:
    from db_connect import get_engine

engine = get_engine()

# Configuration
TABLE_NAME = "matched_atp_records"
WINDOW_3M = 90  # 3 months in days
WINDOW_6M = 180  # 6 months in days

# Add new columns (adjusted to p1/p2 instead of w/l)
with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE matched_atp_records
        ADD COLUMN IF NOT EXISTS p1_name TEXT,
        ADD COLUMN IF NOT EXISTS p2_name TEXT,
        ADD COLUMN IF NOT EXISTS p1_ace_rate_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_ace_rate_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_ace_rate_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_ace_rate_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_df_rate_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_df_rate_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_df_rate_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_df_rate_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_bpsaved_rate_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_bpsaved_rate_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_bpsaved_rate_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_bpsaved_rate_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_bpfaced_rate_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_bpfaced_rate_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_bpfaced_rate_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_bpfaced_rate_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_first_serve_pct_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_first_serve_pct_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_first_serve_pct_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_first_serve_pct_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_first_serve_win_pct_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_first_serve_win_pct_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_first_serve_win_pct_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_first_serve_win_pct_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_second_serve_win_pct_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_second_serve_win_pct_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_second_serve_win_pct_3m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_second_serve_win_pct_6m NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_first_serve_win_pct_3m_hard NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_first_serve_win_pct_3m_hard NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_second_serve_win_pct_3m_hard NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_second_serve_win_pct_3m_hard NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_first_serve_win_pct_3m_clay NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_first_serve_win_pct_3m_clay NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_second_serve_win_pct_3m_clay NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_second_serve_win_pct_3m_clay NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_first_serve_win_pct_3m_grass NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_first_serve_win_pct_3m_grass NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_second_serve_win_pct_3m_grass NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_second_serve_win_pct_3m_grass NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_recent_form_6matches NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p2_recent_form_6matches NUMERIC(5,2),
        ADD COLUMN IF NOT EXISTS p1_tournament_minutes INTEGER,
        ADD COLUMN IF NOT EXISTS p2_tournament_minutes INTEGER;
    """))
    conn.commit()

# Load match data with ORDER BY
print("Loading data from database...")
with engine.connect() as conn:
    df = pd.read_sql(f'''
        SELECT matchid, date, winner_name, loser_name, surface, tournament, minutes,
               w_ace, l_ace, w_df, l_df, w_bpsaved, l_bpsaved, w_bpfaced, l_bpfaced,
               w_svpt, l_svpt, w_1stin, l_1stin, w_1stwon, l_1stwon, w_2ndwon, l_2ndwon
        FROM {TABLE_NAME}
        ORDER BY date ASC
    ''', conn)

df['date'] = pd.to_datetime(df['date'], dayfirst=True)

# Assign p1 and p2 alphabetically to avoid outcome bias
df['p1_name'] = df[['winner_name', 'loser_name']].min(axis=1)
df['p2_name'] = df[['winner_name', 'loser_name']].max(axis=1)

# Track player stats
player_stats = {}
tournament_minutes = {}

def calculate_stat(player, current_date, current_matchid, stat_key, history, window_days, surface=None):
    if player not in history or stat_key not in history[player]:
        return 0.0

    # Strictly use only past matches, ignoring future matches and the current match
    valid_matches = [
        (d, val) for d, val in history[player][stat_key]
        if d < current_date and (current_date - d).days <= window_days
        and val["matchid"] != current_matchid  # Ensure we ignore the current match
        and (surface is None or val.get("surface") == surface)
    ]
    
    if not valid_matches:
        return 0.0

    if stat_key in ["ace", "df", "bpsaved", "bpfaced"]:
        total_stat = sum(val["value"] for _, val in valid_matches)
        count = len(valid_matches)
        return round(total_stat / count, 2) if count > 0 else 0.0
    
    elif stat_key in ["first_serve_pct", "first_serve_win_pct", "second_serve_win_pct"]:
        total_svpt = sum(val["svpt"] for _, val in valid_matches)
        total_1stIn = sum(val["1stin"] for _, val in valid_matches)
        total_1stWon = sum(val["1stwon"] for _, val in valid_matches)
        total_2ndWon = sum(val["2ndwon"] for _, val in valid_matches)

        if stat_key == "first_serve_pct":
            return round(total_1stIn / total_svpt * 100, 2) if total_svpt > 0 else 0.0
        elif stat_key == "first_serve_win_pct":
            return round(total_1stWon / total_1stIn * 100, 2) if total_1stIn > 0 else 0.0
        elif stat_key == "second_serve_win_pct":
            total_2ndSvpt = total_svpt - total_1stIn
            return round(total_2ndWon / total_2ndSvpt * 100, 2) if total_2ndSvpt > 0 else 0.0

    return 0.0

updated_rows = []

print("Calculating rolling stats...")
for index, row in df.iterrows():
    match_date = row["date"]
    p1 = row["p1_name"]
    p2 = row["p2_name"]
    winner = row["winner_name"]
    loser = row["loser_name"]
    matchid = row["matchid"]
    surface = row["surface"]
    tournament = row["tournament"]
    minutes = row["minutes"] if pd.notna(row["minutes"]) else 0

    if pd.isna(p1) or pd.isna(p2) or pd.isna(matchid):
        continue

    # Initialize player history
    for player in [p1, p2]:
        if player not in player_stats:
            player_stats[player] = {
                "ace": [], "df": [], "bpsaved": [], "bpfaced": [],
                "first_serve_pct": [], "first_serve_win_pct": [], "second_serve_win_pct": [],
                "recent_form_6matches": []
            }
        if player not in tournament_minutes:
            tournament_minutes[player] = {}

    # Calculate rolling stats for p1 and p2
    stats_to_calc = ["ace", "df", "bpsaved", "bpfaced", "first_serve_pct", "first_serve_win_pct", "second_serve_win_pct"]
    rolling_values = {}
    for stat in stats_to_calc:
        for prefix, player in [("p1", p1), ("p2", p2)]:
            rolling_values[f'{prefix}_{stat}_rate_3m'] = calculate_stat(player, match_date, matchid, stat, player_stats, WINDOW_3M)
            rolling_values[f'{prefix}_{stat}_rate_6m'] = calculate_stat(player, match_date, matchid, stat, player_stats, WINDOW_6M)
            if stat in ["first_serve_win_pct", "second_serve_win_pct"]:
                for surf in ["Hard", "Clay", "Grass"]:
                    rolling_values[f'{prefix}_{stat}_3m_{surf.lower()}'] = calculate_stat(player, match_date, matchid, stat, player_stats, WINDOW_3M, surf)

    # Recent form
    rolling_values["p1_recent_form_6matches"] = calculate_stat(p1, match_date, matchid, "recent_form_6matches", player_stats, WINDOW_6M)
    rolling_values["p2_recent_form_6matches"] = calculate_stat(p2, match_date, matchid, "recent_form_6matches", player_stats, WINDOW_6M)

    # Tournament-specific fatigue
    p1_minutes = tournament_minutes[p1].get(tournament, 0)
    p2_minutes = tournament_minutes[p2].get(tournament, 0)
    rolling_values["p1_tournament_minutes"] = p1_minutes
    rolling_values["p2_tournament_minutes"] = p2_minutes

    # Store for update
    updated_rows.append((matchid, *rolling_values.values()))

    # ❗ UPDATE PLAYER HISTORY ONLY AFTER CALCULATING STATS
    winner_serve_dict = {"svpt": row["w_svpt"], "1stin": row["w_1stin"], "1stwon": row["w_1stwon"], "2ndwon": row["w_2ndwon"], "surface": surface, "matchid": matchid}
    loser_serve_dict = {"svpt": row["l_svpt"], "1stin": row["l_1stin"], "1stwon": row["l_1stwon"], "2ndwon": row["l_2ndwon"], "surface": surface, "matchid": matchid}

    for player, is_winner in [(p1, p1 == winner), (p2, p2 == winner)]:
        serve_dict = winner_serve_dict if is_winner else loser_serve_dict
        for stat, col in [("ace", "w_ace" if is_winner else "l_ace"), ("df", "w_df" if is_winner else "l_df"),
                          ("bpsaved", "w_bpsaved" if is_winner else "l_bpsaved"), ("bpfaced", "w_bpfaced" if is_winner else "l_bpfaced")]:
            player_stats[player][stat].append((match_date, {"value": row[col] if pd.notna(row[col]) else 0, "surface": surface, "matchid": matchid}))
        for stat in ["first_serve_pct", "first_serve_win_pct", "second_serve_win_pct"]:
            player_stats[player][stat].append((match_date, serve_dict))
        player_stats[player]["recent_form_6matches"].append((match_date, {"won": 1 if is_winner else 0, "matchid": matchid}))

    # Update tournament minutes
    tournament_minutes[p1][tournament] = p1_minutes + minutes
    tournament_minutes[p2][tournament] = p2_minutes + minutes

# Define parameter names
param_names = [
    "p1_ace_rate_3m", "p1_ace_rate_6m", "p2_ace_rate_3m", "p2_ace_rate_6m",
    "p1_df_rate_3m", "p1_df_rate_6m", "p2_df_rate_3m", "p2_df_rate_6m",
    "p1_bpsaved_rate_3m", "p1_bpsaved_rate_6m", "p2_bpsaved_rate_3m", "p2_bpsaved_rate_6m",
    "p1_bpfaced_rate_3m", "p1_bpfaced_rate_6m", "p2_bpfaced_rate_3m", "p2_bpfaced_rate_6m",
    "p1_first_serve_pct_3m", "p1_first_serve_pct_6m", "p2_first_serve_pct_3m", "p2_first_serve_pct_6m",
    "p1_first_serve_win_pct_3m", "p1_first_serve_win_pct_6m", "p2_first_serve_win_pct_3m", "p2_first_serve_win_pct_6m",
    "p1_second_serve_win_pct_3m", "p1_second_serve_win_pct_6m", "p2_second_serve_win_pct_3m", "p2_second_serve_win_pct_6m",
    "p1_first_serve_win_pct_3m_hard", "p2_first_serve_win_pct_3m_hard",
    "p1_second_serve_win_pct_3m_hard", "p2_second_serve_win_pct_3m_hard",
    "p1_first_serve_win_pct_3m_clay", "p2_first_serve_win_pct_3m_clay",
    "p1_second_serve_win_pct_3m_clay", "p2_second_serve_win_pct_3m_clay",
    "p1_first_serve_win_pct_3m_grass", "p2_first_serve_win_pct_3m_grass",
    "p1_second_serve_win_pct_3m_grass", "p2_second_serve_win_pct_3m_grass",
    "p1_recent_form_6matches", "p2_recent_form_6matches",
    "p1_tournament_minutes", "p2_tournament_minutes",
    "matchid"
]

# Update query
update_query = text(f"""
    UPDATE {TABLE_NAME}
    SET p1_ace_rate_3m = :p1_ace_rate_3m, p1_ace_rate_6m = :p1_ace_rate_6m,
        p2_ace_rate_3m = :p2_ace_rate_3m, p2_ace_rate_6m = :p2_ace_rate_6m,
        p1_df_rate_3m = :p1_df_rate_3m, p1_df_rate_6m = :p1_df_rate_6m,
        p2_df_rate_3m = :p2_df_rate_3m, p2_df_rate_6m = :p2_df_rate_6m,
        p1_bpsaved_rate_3m = :p1_bpsaved_rate_3m, p1_bpsaved_rate_6m = :p1_bpsaved_rate_6m,
        p2_bpsaved_rate_3m = :p2_bpsaved_rate_3m, p2_bpsaved_rate_6m = :p2_bpsaved_rate_6m,
        p1_bpfaced_rate_3m = :p1_bpfaced_rate_3m, p1_bpfaced_rate_6m = :p1_bpfaced_rate_6m,
        p2_bpfaced_rate_3m = :p2_bpfaced_rate_3m, p2_bpfaced_rate_6m = :p2_bpfaced_rate_6m,
        p1_first_serve_pct_3m = :p1_first_serve_pct_3m, p1_first_serve_pct_6m = :p1_first_serve_pct_6m,
        p2_first_serve_pct_3m = :p2_first_serve_pct_3m, p2_first_serve_pct_6m = :p2_first_serve_pct_6m,
        p1_first_serve_win_pct_3m = :p1_first_serve_win_pct_3m, p1_first_serve_win_pct_6m = :p1_first_serve_win_pct_6m,
        p2_first_serve_win_pct_3m = :p2_first_serve_win_pct_3m, p2_first_serve_win_pct_6m = :p2_first_serve_win_pct_6m,
        p1_second_serve_win_pct_3m = :p1_second_serve_win_pct_3m, p1_second_serve_win_pct_6m = :p1_second_serve_win_pct_6m,
        p2_second_serve_win_pct_3m = :p2_second_serve_win_pct_3m, p2_second_serve_win_pct_6m = :p2_second_serve_win_pct_6m,
        p1_first_serve_win_pct_3m_hard = :p1_first_serve_win_pct_3m_hard,
        p2_first_serve_win_pct_3m_hard = :p2_first_serve_win_pct_3m_hard,
        p1_second_serve_win_pct_3m_hard = :p1_second_serve_win_pct_3m_hard,
        p2_second_serve_win_pct_3m_hard = :p2_second_serve_win_pct_3m_hard,
        p1_first_serve_win_pct_3m_clay = :p1_first_serve_win_pct_3m_clay,
        p2_first_serve_win_pct_3m_clay = :p2_first_serve_win_pct_3m_clay,
        p1_second_serve_win_pct_3m_clay = :p1_second_serve_win_pct_3m_clay,
        p2_second_serve_win_pct_3m_clay = :p2_second_serve_win_pct_3m_clay,
        p1_first_serve_win_pct_3m_grass = :p1_first_serve_win_pct_3m_grass,
        p2_first_serve_win_pct_3m_grass = :p2_first_serve_win_pct_3m_grass,
        p1_second_serve_win_pct_3m_grass = :p1_second_serve_win_pct_3m_grass,
        p2_second_serve_win_pct_3m_grass = :p2_second_serve_win_pct_3m_grass,
        p1_recent_form_6matches = :p1_recent_form_6matches,
        p2_recent_form_6matches = :p2_recent_form_6matches,
        p1_tournament_minutes = :p1_tournament_minutes,
        p2_tournament_minutes = :p2_tournament_minutes
    WHERE matchid = :matchid
""")

# Ensure only the newest match is updated
latest_match_date = df["date"].max()
df_latest = df[df["date"] == latest_match_date]

# Filter updated_rows to only contain the latest match
filtered_updated_rows = [row for row in updated_rows if row[0] in df_latest["matchid"].values]

# Update database
print("Updating database...")
with engine.connect() as conn:
    conn.execute(
        update_query,
        [dict(zip(param_names, list(values) + [matchid])) for matchid, *values in filtered_updated_rows]
    )
    conn.commit()