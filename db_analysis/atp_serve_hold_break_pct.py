import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from sqlalchemy import text
from db_connect import get_engine

# Get the database engine
engine = get_engine()

# -------------------------
# CONFIGURATION
# -------------------------
TABLE_NAME = "matched_atp_records"

# Rolling Average Window
ROLLING_WINDOW = 10
SURFACES = ["Hard", "Clay", "Grass"]

# Load match data
with engine.connect() as conn:
    df = pd.read_sql(f"""
        SELECT matchid, date, winner_name, loser_name, surface,
               w_svgms, w_bpfaced, w_bpsaved, 
               l_svgms, l_bpfaced, l_bpsaved 
        FROM {TABLE_NAME}
        ORDER BY date ASC, matchid ASC
    """, conn)

# Convert Date column to datetime and sort matches chronologically
df['date'] = pd.to_datetime(df['date'], dayfirst=True)
df = df.sort_values(by="date")

# Dictionaries to track player history
player_history = {}  # {player: [(hold, break, surface)]} for rolling
player_totals = {}   # {player: [games_served, breaks_lost, opp_games, breaks_won]} for aggregated
player_surface_totals = {}  # {player: {surface: [games_served, breaks_lost, opp_games, breaks_won]}} for surface-specific

# -------------------------
# CALCULATE HOLD % AND BREAK %
# -------------------------
print("Calculating total and rolling hold and break percentages...")

def calculate_match_hold_break(w_svgms, w_bpfaced, w_bpsaved, l_svgms, l_bpfaced, l_bpsaved):
    w_hold_pct = ((w_svgms - (w_bpfaced - w_bpsaved)) / w_svgms * 100) if w_svgms > 0 else 0.0
    w_break_pct = ((l_bpfaced - l_bpsaved) / l_svgms * 100) if l_svgms > 0 else 0.0
    l_hold_pct = ((l_svgms - (l_bpfaced - l_bpsaved)) / l_svgms * 100) if l_svgms > 0 else 0.0
    l_break_pct = ((w_bpfaced - w_bpsaved) / w_svgms * 100) if w_svgms > 0 else 0.0
    return w_hold_pct, w_break_pct, l_hold_pct, l_break_pct

def calculate_rolling_avg(player, history, surface=None, window=ROLLING_WINDOW):
    if player not in history or not history[player]:
        return 0.0, 0.0
    if surface:
        recent_matches = [m for m in history[player] if m[2] == surface][-window:]
    else:
        recent_matches = history[player][-window:]
    if not recent_matches:
        return 0.0, 0.0
    avg_hold = sum(h for h, _, _ in recent_matches) / len(recent_matches)
    avg_break = sum(b for _, b, _ in recent_matches) / len(recent_matches)
    return round(avg_hold, 2), round(avg_break, 2)

def calculate_total_pct(player, totals, surface=None):
    if player not in totals or (surface and surface not in totals[player]):
        return 0.0, 0.0
    if surface:
        games_served, breaks_lost, opp_games, breaks_won = totals[player][surface]
    else:
        games_served, breaks_lost, opp_games, breaks_won = totals[player]
    total_hold = ((games_served - breaks_lost) / games_served * 100) if games_served > 0 else 0.0
    total_break = (breaks_won / opp_games * 100) if opp_games > 0 else 0.0
    return round(total_hold, 2), round(total_break, 2)

updated_rows = []

for index, row in df.iterrows():
    match_date = row["date"]
    winner = row["winner_name"]
    loser = row["loser_name"]
    matchid = row["matchid"]
    surface = row["surface"] if pd.notna(row["surface"]) and row["surface"] in SURFACES else "Unknown"
    w_svgms = row["w_svgms"] if pd.notna(row["w_svgms"]) else 0
    w_bpfaced = row["w_bpfaced"] if pd.notna(row["w_bpfaced"]) else 0
    w_bpsaved = row["w_bpsaved"] if pd.notna(row["w_bpsaved"]) else 0
    l_svgms = row["l_svgms"] if pd.notna(row["l_svgms"]) else 0
    l_bpfaced = row["l_bpfaced"] if pd.notna(row["l_bpfaced"]) else 0
    l_bpsaved = row["l_bpsaved"] if pd.notna(row["l_bpsaved"]) else 0

    # Validate data
    if pd.isna(winner) or pd.isna(loser) or pd.isna(matchid):
        print(f"Skipping row {index}: Missing winner, loser, or matchid")
        continue

    # Initialize player history and totals
    if winner not in player_history:
        player_history[winner] = []
        player_totals[winner] = [0, 0, 0, 0]
        player_surface_totals[winner] = {s: [0, 0, 0, 0] for s in SURFACES}
    if loser not in player_history:
        player_history[loser] = []
        player_totals[loser] = [0, 0, 0, 0]
        player_surface_totals[loser] = {s: [0, 0, 0, 0] for s in SURFACES}

    # Calculate pre-match stats (aggregated)
    w_hold_roll, w_break_roll = calculate_rolling_avg(winner, player_history)
    w_hold_total, w_break_total = calculate_total_pct(winner, player_totals)
    l_hold_roll, l_break_roll = calculate_rolling_avg(loser, player_history)
    l_hold_total, l_break_total = calculate_total_pct(loser, player_totals)

    # Calculate pre-match stats (surface-specific)
    w_hold_roll_hard, w_break_roll_hard = calculate_rolling_avg(winner, player_history, "Hard")
    w_hold_total_hard, w_break_total_hard = calculate_total_pct(winner, player_surface_totals, "Hard")
    w_hold_roll_clay, w_break_roll_clay = calculate_rolling_avg(winner, player_history, "Clay")
    w_hold_total_clay, w_break_total_clay = calculate_total_pct(winner, player_surface_totals, "Clay")
    w_hold_roll_grass, w_break_roll_grass = calculate_rolling_avg(winner, player_history, "Grass")
    w_hold_total_grass, w_break_total_grass = calculate_total_pct(winner, player_surface_totals, "Grass")

    l_hold_roll_hard, l_break_roll_hard = calculate_rolling_avg(loser, player_history, "Hard")
    l_hold_total_hard, l_break_total_hard = calculate_total_pct(loser, player_surface_totals, "Hard")
    l_hold_roll_clay, l_break_roll_clay = calculate_rolling_avg(loser, player_history, "Clay")
    l_hold_total_clay, l_break_total_clay = calculate_total_pct(loser, player_surface_totals, "Clay")
    l_hold_roll_grass, l_break_roll_grass = calculate_rolling_avg(loser, player_history, "Grass")
    l_hold_total_grass, l_break_total_grass = calculate_total_pct(loser, player_surface_totals, "Grass")

    # Calculate per-match stats for history update
    w_hold_pct, w_break_pct, l_hold_pct, l_break_pct = calculate_match_hold_break(
        w_svgms, w_bpfaced, w_bpsaved, l_svgms, l_bpfaced, l_bpsaved
    )

    # Store for database update (aggregated + all surfaces)
    updated_rows.append((
        matchid,
        w_hold_total, w_break_total, w_hold_roll, w_break_roll,
        w_hold_total_hard, w_break_total_hard, w_hold_roll_hard, w_break_roll_hard,
        w_hold_total_clay, w_break_total_clay, w_hold_roll_clay, w_break_roll_clay,
        w_hold_total_grass, w_break_total_grass, w_hold_roll_grass, w_break_roll_grass,
        l_hold_total, l_break_total, l_hold_roll, l_break_roll,
        l_hold_total_hard, l_break_total_hard, l_hold_roll_hard, l_break_roll_hard,
        l_hold_total_clay, l_break_total_clay, l_hold_roll_clay, l_break_roll_clay,
        l_hold_total_grass, l_break_total_grass, l_hold_roll_grass, l_break_roll_grass
    ))

    # Update player history and totals
    player_history[winner].append((w_hold_pct, w_break_pct, surface))
    player_history[loser].append((l_hold_pct, l_break_pct, surface))
    # Aggregated totals
    player_totals[winner][0] += w_svgms
    player_totals[winner][1] += (w_bpfaced - w_bpsaved)
    player_totals[winner][2] += l_svgms
    player_totals[winner][3] += (l_bpfaced - l_bpsaved)
    player_totals[loser][0] += l_svgms
    player_totals[loser][1] += (l_bpfaced - l_bpsaved)
    player_totals[loser][2] += w_svgms
    player_totals[loser][3] += (w_bpfaced - w_bpsaved)
    # Surface-specific totals
    if surface in SURFACES:
        player_surface_totals[winner][surface][0] += w_svgms
        player_surface_totals[winner][surface][1] += (w_bpfaced - w_bpsaved)
        player_surface_totals[winner][surface][2] += l_svgms
        player_surface_totals[winner][surface][3] += (l_bpfaced - l_bpsaved)
        player_surface_totals[loser][surface][0] += l_svgms
        player_surface_totals[loser][surface][1] += (l_bpfaced - l_bpsaved)
        player_surface_totals[loser][surface][2] += w_svgms
        player_surface_totals[loser][surface][3] += (w_bpfaced - w_bpsaved)

# -------------------------
# UPDATE DATABASE
# -------------------------
print("Updating database with hold and break percentages...")
try:
    with engine.connect() as conn:
        for row in updated_rows:
            (matchid,
             w_hold_t, w_break_t, w_hold_r, w_break_r,
             w_hold_t_h, w_break_t_h, w_hold_r_h, w_break_r_h,
             w_hold_t_c, w_break_t_c, w_hold_r_c, w_break_r_c,
             w_hold_t_g, w_break_t_g, w_hold_r_g, w_break_r_g,
             l_hold_t, l_break_t, l_hold_r, l_break_r,
             l_hold_t_h, l_break_t_h, l_hold_r_h, l_break_r_h,
             l_hold_t_c, l_break_t_c, l_hold_r_c, l_break_r_c,
             l_hold_t_g, l_break_t_g, l_hold_r_g, l_break_r_g) = row
            
            update_query = text(f"""
                UPDATE {TABLE_NAME}
                SET winner_hold_pct_total = :w_hold_t, winner_break_pct_total = :w_break_t,
                    winner_hold_pct_roll = :w_hold_r, winner_break_pct_roll = :w_break_r,
                    winner_hold_pct_total_hard = :w_hold_t_h, winner_break_pct_total_hard = :w_break_t_h,
                    winner_hold_pct_roll_hard = :w_hold_r_h, winner_break_pct_roll_hard = :w_break_r_h,
                    winner_hold_pct_total_clay = :w_hold_t_c, winner_break_pct_total_clay = :w_break_t_c,
                    winner_hold_pct_roll_clay = :w_hold_r_c, winner_break_pct_roll_clay = :w_break_r_c,
                    winner_hold_pct_total_grass = :w_hold_t_g, winner_break_pct_total_grass = :w_break_t_g,
                    winner_hold_pct_roll_grass = :w_hold_r_g, winner_break_pct_roll_grass = :w_break_r_g,
                    loser_hold_pct_total = :l_hold_t, loser_break_pct_total = :l_break_t,
                    loser_hold_pct_roll = :l_hold_r, loser_break_pct_roll = :l_break_r,
                    loser_hold_pct_total_hard = :l_hold_t_h, loser_break_pct_total_hard = :l_break_t_h,
                    loser_hold_pct_roll_hard = :l_hold_r_h, loser_break_pct_roll_hard = :l_break_r_h,
                    loser_hold_pct_total_clay = :l_hold_t_c, loser_break_pct_total_clay = :l_break_t_c,
                    loser_hold_pct_roll_clay = :l_hold_r_c, loser_break_pct_roll_clay = :l_break_r_c,
                    loser_hold_pct_total_grass = :l_hold_t_g, loser_break_pct_total_grass = :l_break_t_g,
                    loser_hold_pct_roll_grass = :l_hold_r_g, loser_break_pct_roll_grass = :l_break_r_g
                WHERE matchid = :matchid
            """)
            conn.execute(update_query, {
                "matchid": matchid,
                "w_hold_t": w_hold_t, "w_break_t": w_break_t, "w_hold_r": w_hold_r, "w_break_r": w_break_r,
                "w_hold_t_h": w_hold_t_h, "w_break_t_h": w_break_t_h, "w_hold_r_h": w_hold_r_h, "w_break_r_h": w_break_r_h,
                "w_hold_t_c": w_hold_t_c, "w_break_t_c": w_break_t_c, "w_hold_r_c": w_hold_r_c, "w_break_r_c": w_break_r_c,
                "w_hold_t_g": w_hold_t_g, "w_break_t_g": w_break_t_g, "w_hold_r_g": w_hold_r_g, "w_break_r_g": w_break_r_g,
                "l_hold_t": l_hold_t, "l_break_t": l_break_t, "l_hold_r": l_hold_r, "l_break_r": l_break_r,
                "l_hold_t_h": l_hold_t_h, "l_break_t_h": l_break_t_h, "l_hold_r_h": l_hold_r_h, "l_break_r_h": l_break_r_h,
                "l_hold_t_c": l_hold_t_c, "l_break_t_c": l_break_t_c, "l_hold_r_c": l_hold_r_c, "l_break_r_c": l_break_r_c,
                "l_hold_t_g": l_hold_t_g, "l_break_t_g": l_break_t_g, "l_hold_r_g": l_hold_r_g, "l_break_r_g": l_break_r_g
            })
        conn.commit()
    print("✅ Total and rolling hold and break percentages (aggregated and surface-specific) successfully updated in the database!")
except Exception as e:
    print(f"Error updating database: {e}")
    raise