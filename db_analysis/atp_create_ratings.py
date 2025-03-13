import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
import math
from datetime import datetime, timedelta
from sqlalchemy import text
from db_connect import get_engine

# Get the database engine
engine = get_engine()

# -------------------------
# CONFIGURATION
# -------------------------
INITIAL_RATING = 1500
DECAY_THRESHOLD_DAYS = 180
DECAY_RATE = 0.995
REMOVAL_THRESHOLD_DAYS = 730
SURFACE_TYPES = ["Hard", "Clay", "Grass"]
K_BASE = 32
K_MIN = 12
K_MAX = 50
MATCH_HISTORY_LIMIT = 1000

# -------------------------
# LOAD MATCHES IN CHRONOLOGICAL ORDER
# -------------------------
print("Loading matches from database...")
with engine.connect() as connection:
    df_matches = pd.read_sql(
        text('SELECT * FROM matched_atp_records ORDER BY date ASC'),
        connection
    )

print(f"Loaded {len(df_matches)} matches from database.")

# -------------------------
# INITIALIZE ELO DATA
# -------------------------
player_ratings = {}
player_surface_ratings = {surface: {} for surface in SURFACE_TYPES}
player_last_match = {}
player_match_history = {}

def get_or_create_player(player_name, surface, match_date):
    """Retrieve or initialize player's Elo ratings."""
    if player_name not in player_ratings:
        player_ratings[player_name] = INITIAL_RATING
        player_last_match[player_name] = match_date
        player_match_history[player_name] = []
    if player_name not in player_surface_ratings[surface]:
        player_surface_ratings[surface][player_name] = INITIAL_RATING
    return player_ratings[player_name], player_surface_ratings[surface][player_name]

def expected_score(rating_a, rating_b):
    """Calculate expected probability of Player A beating Player B."""
    return 1 / (1 + math.pow(10, (rating_b - rating_a) / 400))

def apply_rating_decay(player, match_date):
    """Apply Elo decay for inactivity."""
    if player in player_last_match:
        days_inactive = (match_date - player_last_match[player]).days
        if days_inactive > DECAY_THRESHOLD_DAYS:
            months_inactive = days_inactive / 30
            decay_factor = DECAY_RATE ** months_inactive
            player_ratings[player] *= decay_factor
            for surface in SURFACE_TYPES:
                if player in player_surface_ratings[surface]:
                    player_surface_ratings[surface][player] *= decay_factor

def calculate_dynamic_k(player, surface):
    """Calculate K-factor based on both total and surface-specific matches played."""
    total_matches = len(player_match_history.get(player, []))
    surface_matches = sum(1 for match in player_match_history.get(player, []) if match[2] == surface)
    base_k = K_BASE * (1 / (1 + 0.1 * total_matches))  # Standard K-Factor Calculation
    surface_adjustment = max(0.5, 1 - math.exp(-surface_matches / 5))  # Surface Learning Curve
    return max(K_MIN, min(K_MAX, base_k * surface_adjustment))

def calculate_log_surface_weighting(overall_elo, surface_elo, total_matches, surface_matches):
    """Blend overall Elo and surface Elo using a smooth weighting function."""
    if total_matches == 0:  # Prevent division by zero
        return overall_elo
    surface_weight = 1 - math.exp(-surface_matches / 10)  # Gradually increases toward 1
    return round(surface_weight * surface_elo + (1 - surface_weight) * overall_elo, 2)

def calculate_weighted_avg_elo_faced(player):
    """Compute exponentially weighted average Elo faced."""
    if not player_match_history.get(player, []):
        return 0
    decay_factor = 0.9
    weighted_sum = 0
    total_weight = 0
    weight = 1
    recent_matches = list(reversed(player_match_history[player]))[:50]  # Last 50 matches
    for match in recent_matches:
        weighted_sum += match[1] * weight  # match[1] is opponent's Elo
        total_weight += weight
        weight *= decay_factor
    return round(weighted_sum / total_weight, 2) if total_weight > 0 else 0

def adjust_for_tie_breaks(winner_blended_elo, loser_blended_elo, tie_breaks):
    """Reduce Elo impact based on tie-break count and expected probability."""
    if tie_breaks == 0:
        return 1  # No adjustment
    expected_winner = expected_score(winner_blended_elo, loser_blended_elo)
    tb_adjustment = 1 - (0.02 * tie_breaks * (1 - expected_winner))  # Scale by upset probability
    return max(0.9, tb_adjustment)  # Cap at 90% reduction

def adjust_k_for_retirement(K_factor, comment, set_count):
    """Reduce K-factor based on how deep the match went before retirement."""
    if "Retired" in str(comment):
        if set_count == 1:  # Early retirement
            return K_factor * 0.3
        elif set_count == 2:  # Mid-match
            return K_factor * 0.6
        else:  # Deep into match
            return K_factor * 0.9
    return K_factor

def calculate_hold_break_percent(w_SvGms, w_bpSaved, w_bpFaced, l_bpSaved, l_bpFaced, l_SvGms):
    """Compute Hold % and Break %."""
    hold_pct = (w_SvGms - w_bpFaced + w_bpSaved) / w_SvGms if w_SvGms > 0 else 0
    break_pct = (l_bpFaced - l_bpSaved) / l_SvGms if l_SvGms > 0 else 0
    return hold_pct, break_pct

# -------------------------
# PROCESS MATCHES AND UPDATE ELO
# -------------------------
updated_rows = []

for _, row in df_matches.iterrows():
    match_date = row["date"]
    surface = row["surface"]
    winner = row["winner_name"]
    loser = row["loser_name"]
    match_id = row["matchid"]
    comment = row["comment"] if pd.notna(row["comment"]) else ""
    set_count = row["sets"] if "sets" in row and pd.notna(row["sets"]) else 3  # Default to 3 sets
    score = row["score"] if pd.notna(row["score"]) else ""

    if surface not in SURFACE_TYPES:
        continue
    if comment == "Walkover":
        continue  # Skip walkovers

    # Ensure players exist
    winner_rating, winner_surface_rating = get_or_create_player(winner, surface, match_date)
    loser_rating, loser_surface_rating = get_or_create_player(loser, surface, match_date)

    # Apply decay
    apply_rating_decay(winner, match_date)
    apply_rating_decay(loser, match_date)

    # Pre-match ratings
    winner_overall_elo = player_ratings[winner]
    loser_overall_elo = player_ratings[loser]
    winner_surface_elo = player_surface_ratings[surface][winner]
    loser_surface_elo = player_surface_ratings[surface][loser]

    # Compute pre-match statistics
    total_matches_winner = len(player_match_history.get(winner, []))
    total_matches_loser = len(player_match_history.get(loser, []))
    surface_matches_winner = sum(1 for match in player_match_history.get(winner, []) if match[2] == surface)
    surface_matches_loser = sum(1 for match in player_match_history.get(loser, []) if match[2] == surface)

    winner_blended_elo = calculate_log_surface_weighting(
        winner_overall_elo, winner_surface_elo, total_matches_winner, surface_matches_winner
    )
    loser_blended_elo = calculate_log_surface_weighting(
        loser_overall_elo, loser_surface_elo, total_matches_loser, surface_matches_loser
    )

    winner_avg_elo_faced = calculate_weighted_avg_elo_faced(winner)
    loser_avg_elo_faced = calculate_weighted_avg_elo_faced(loser)

    # Compute Serve/Return Strength
    winner_hold_pct, winner_break_pct = calculate_hold_break_percent(
        row["w_svgms"] if pd.notna(row["w_svgms"]) else 0,
        row["w_bpsaved"] if pd.notna(row["w_bpsaved"]) else 0,
        row["w_bpfaced"] if pd.notna(row["w_bpfaced"]) else 0,
        row["l_bpsaved"] if pd.notna(row["l_bpsaved"]) else 0,
        row["l_bpfaced"] if pd.notna(row["l_bpfaced"]) else 0,
        row["l_svgms"] if pd.notna(row["l_svgms"]) else 0
    )
    loser_hold_pct, loser_break_pct = calculate_hold_break_percent(
        row["l_svgms"] if pd.notna(row["l_svgms"]) else 0,
        row["l_bpsaved"] if pd.notna(row["l_bpsaved"]) else 0,
        row["l_bpfaced"] if pd.notna(row["l_bpfaced"]) else 0,
        row["w_bpsaved"] if pd.notna(row["w_bpsaved"]) else 0,
        row["w_bpfaced"] if pd.notna(row["w_bpfaced"]) else 0,
        row["w_svgms"] if pd.notna(row["w_svgms"]) else 0
    )

    # Compute Dominance Factor
    dominance_factor = ((winner_hold_pct - loser_break_pct) + (loser_hold_pct - winner_break_pct)) / 2
    dominance_factor = max(0.5, min(1.5, dominance_factor))  # Cap between 0.5 and 1.5

    # Tie-breaks
    tie_breaks = sum(1 for s in str(score).split() if "7-6" in s or "6-7" in s)
    tb_adjustment = adjust_for_tie_breaks(winner_blended_elo, loser_blended_elo, tie_breaks)
    dominance_factor *= tb_adjustment

    # Dynamic K-factors
    K_factor_winner = adjust_k_for_retirement(calculate_dynamic_k(winner, surface), comment, set_count)
    K_factor_loser = adjust_k_for_retirement(calculate_dynamic_k(loser, surface), comment, set_count)

    # Expected scores
    expected_winner = expected_score(winner_blended_elo, loser_blended_elo)
    expected_loser = 1 - expected_winner

    # Store pre-match results
    updated_rows.append((
        match_id,
        round(winner_overall_elo, 2),
        round(winner_surface_elo, 2),
        total_matches_winner,
        winner_avg_elo_faced,
        round(loser_overall_elo, 2),
        round(loser_surface_elo, 2),
        total_matches_loser,
        loser_avg_elo_faced
    ))

    # Update ratings post-match
    player_ratings[winner] += K_factor_winner * (1 - expected_winner) * dominance_factor
    player_ratings[loser] += K_factor_loser * (0 - expected_loser) * dominance_factor
    player_surface_ratings[surface][winner] += K_factor_winner * (1 - expected_winner) * dominance_factor
    player_surface_ratings[surface][loser] += K_factor_loser * (0 - expected_loser) * dominance_factor

    # Update match history and last match date
    player_last_match[winner] = match_date
    player_last_match[loser] = match_date
    player_match_history[winner].append((loser, loser_overall_elo, surface, 1))
    player_match_history[loser].append((winner, winner_overall_elo, surface, 0))
    player_match_history[winner] = player_match_history[winner][-MATCH_HISTORY_LIMIT:]
    player_match_history[loser] = player_match_history[loser][-MATCH_HISTORY_LIMIT:]

# -------------------------
# UPDATE DATABASE WITH PRE-MATCH ELO AND FEATURES
# -------------------------
print("Updating database with pre-match Elo ratings and features...")
with engine.connect() as connection:
    for match in updated_rows:
        query = text("""
            UPDATE matched_atp_records
            SET
                winner_overall_elo = :w_elo,
                winner_surface_elo = :w_s_elo,
                winner_total_matches = :w_matches,
                winner_avg_elo_faced = :w_avg_elo,
                loser_overall_elo = :l_elo,
                loser_surface_elo = :l_s_elo,
                loser_total_matches = :l_matches,
                loser_avg_elo_faced = :l_avg_elo
            WHERE matchid = :match_id
        """)
        connection.execute(query, {
            "match_id": match[0],
            "w_elo": match[1],
            "w_s_elo": match[2],
            "w_matches": match[3],
            "w_avg_elo": match[4],
            "l_elo": match[5],
            "l_s_elo": match[6],
            "l_matches": match[7],
            "l_avg_elo": match[8]
        })
    connection.commit()

print("✅ Database updated with refined Elo ratings and additional features!")