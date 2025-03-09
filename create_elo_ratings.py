# This script reads in a CSV of tennis matches and creates an ELO rating for each player
import pandas as pd
import math
from datetime import datetime, timedelta

# Elo Constants
INITIAL_RATING = 1500
DECAY_THRESHOLD_DAYS = 180  # Days before decay starts
DECAY_RATE = 0.995  # Rating retains 99.5% of value per month (0.5% decay)
REMOVAL_THRESHOLD_DAYS = 730  # Remove players inactive for 2 years
SURFACE_TYPES = ["Hard", "Clay", "Grass"]  # Track separate Elo for each surface
K_BASE = 32  # Base K-factor
K_MIN = 12  # Minimum K-factor for experienced players
K_MAX = 50  # Maximum K-factor for new players
MATCH_HISTORY_LIMIT = 1000  # Limit for tracking career match history

# Load match data
file_path = "tennis_all.csv"
try:
    df = pd.read_csv(file_path)
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
    df = df.sort_values(by=["Date"])
except FileNotFoundError:
    print(f"Error: {file_path} not found.")
    exit(1)
except Exception as e:
    print(f"Error loading data: {e}")
    exit(1)

# Initialize player data
player_ratings = {}
player_surface_ratings = {surface: {} for surface in SURFACE_TYPES}
player_last_match = {}
player_match_history = {}

def get_or_create_player(player_name, surface, match_date):
    if player_name not in player_ratings:
        player_ratings[player_name] = INITIAL_RATING
        player_last_match[player_name] = match_date
        player_match_history[player_name] = []
    if player_name not in player_surface_ratings[surface]:
        player_surface_ratings[surface][player_name] = INITIAL_RATING
    return player_ratings[player_name], player_surface_ratings[surface][player_name]

def expected_score(rating_a, rating_b):
    return 1 / (1 + math.pow(10, (rating_b - rating_a) / 400))

def apply_rating_decay(player, match_date):
    if player in player_last_match:
        days_inactive = (match_date - player_last_match[player]).days
        if days_inactive > DECAY_THRESHOLD_DAYS:
            months_inactive = days_inactive / 30
            decay_factor = DECAY_RATE ** months_inactive
            player_ratings[player] *= decay_factor
            for surface in SURFACE_TYPES:
                if player in player_surface_ratings[surface]:
                    player_surface_ratings[surface][player] *= decay_factor

def calculate_dynamic_k(player):
    matches_played = len(player_match_history.get(player, []))
    return max(K_MIN, min(K_MAX, K_BASE * (1 / (1 + 0.1 * matches_played))))

def calculate_weighted_avg_elo_faced(player):
    """Calculate exponentially weighted average Elo of opponents faced."""
    if not player_match_history[player]:
        return 0
    decay_factor = 0.9
    weighted_sum = 0
    total_weight = 0
    weight = 1
    recent_matches = list(reversed(player_match_history[player]))[:50]  # Last 50 matches
    for match in recent_matches:
        weighted_sum += match[1] * weight
        total_weight += weight
        weight *= decay_factor
    return weighted_sum / total_weight if total_weight > 0 else 0

def calculate_log_surface_weighting(overall_elo, surface_elo, total_matches, surface_matches):
    """Blend overall Elo and surface Elo using logarithmic weighting."""
    if total_matches == 0:  # Prevent division by zero
        return overall_elo
    surface_weight = math.log(1 + surface_matches) / math.log(1 + total_matches)
    return surface_weight * surface_elo + (1 - surface_weight) * overall_elo

def update_elo(winner, loser, match_date, surface, comment):
    global player_ratings, player_last_match, player_surface_ratings, player_match_history
    if "Walkover" in str(comment):
        return
    
    # Get or create player ratings
    winner_rating, winner_surface_rating = get_or_create_player(winner, surface, match_date)
    loser_rating, loser_surface_rating = get_or_create_player(loser, surface, match_date)
    
    # Apply decay
    apply_rating_decay(winner, match_date)
    apply_rating_decay(loser, match_date)
    
    # Get current ratings
    winner_rating = player_ratings[winner]
    loser_rating = player_ratings[loser]
    winner_surface_rating = player_surface_ratings[surface][winner]
    loser_surface_rating = player_surface_ratings[surface][loser]
    
    # Calculate match counts for weighting
    total_matches_winner = len(player_match_history[winner])
    total_matches_loser = len(player_match_history[loser])
    surface_matches_winner = sum(1 for match in player_match_history[winner] if match[2] == surface)
    surface_matches_loser = sum(1 for match in player_match_history[loser] if match[2] == surface)
    
    # Calculate blended Elo ratings
    winner_blended_elo = calculate_log_surface_weighting(
        winner_rating, winner_surface_rating, total_matches_winner, surface_matches_winner
    )
    loser_blended_elo = calculate_log_surface_weighting(
        loser_rating, loser_surface_rating, total_matches_loser, surface_matches_loser
    )
    
    # Calculate K-factors
    K_factor_winner = calculate_dynamic_k(winner)
    K_factor_loser = calculate_dynamic_k(loser)
    if "Retired" in str(comment):
        K_factor_winner *= 0.5
        K_factor_loser *= 0.5
    
    # Calculate expected scores using blended ratings
    expected_winner = expected_score(winner_blended_elo, loser_blended_elo)
    expected_loser = 1 - expected_winner
    
    # Update both overall and surface ratings
    player_ratings[winner] += K_factor_winner * (1 - expected_winner)
    player_ratings[loser] += K_factor_loser * (0 - expected_loser)
    player_surface_ratings[surface][winner] += K_factor_winner * (1 - expected_winner)
    player_surface_ratings[surface][loser] += K_factor_loser * (0 - expected_loser)
    
    # Update match history
    player_last_match[winner] = match_date
    player_last_match[loser] = match_date
    player_match_history[winner].append((loser, player_ratings[loser], surface, 1))
    player_match_history[loser].append((winner, player_ratings[winner], surface, 0))
    player_match_history[winner] = player_match_history[winner][-MATCH_HISTORY_LIMIT:]
    player_match_history[loser] = player_match_history[loser][-MATCH_HISTORY_LIMIT:]

for _, row in df.iterrows():
    surface = row["Surface"]
    comment = row.get("Comment", "")
    if surface not in SURFACE_TYPES:
        continue
    update_elo(row["Winner"], row["Loser"], row["Date"], surface, comment)

today = datetime.today()
for player in player_ratings:
    apply_rating_decay(player, today)

six_months_ago = today - timedelta(days=180)
final_ratings = {}
for player, rating in player_ratings.items():
    last_played = player_last_match[player]
    days_inactive = (today - last_played).days
    if days_inactive > REMOVAL_THRESHOLD_DAYS:
        continue
    matches_last_6m = sum(1 for match in player_match_history[player] if isinstance(match[2], datetime) and match[2] > six_months_ago)
    career_matches = len(player_match_history[player])
    avg_elo_faced = calculate_weighted_avg_elo_faced(player)
    wins_vs_top20 = sum(1 for match in player_match_history[player] if match[1] >= 1800 and match[3] == 1)
    matches_vs_top20 = sum(1 for match in player_match_history[player] if match[1] >= 1800)
    wins_vs_top50 = sum(1 for match in player_match_history[player] if match[1] >= 1600 and match[3] == 1)
    matches_vs_top50 = sum(1 for match in player_match_history[player] if match[1] >= 1600)
    winrate_vs_top20 = wins_vs_top20 / matches_vs_top20 if matches_vs_top20 else 0
    winrate_vs_top50 = wins_vs_top50 / matches_vs_top50 if matches_vs_top50 else 0
    final_ratings[player] = {
        "Overall Elo": round(rating, 2),
        "Hard Elo": round(player_surface_ratings["Hard"].get(player, rating), 2),
        "Clay Elo": round(player_surface_ratings["Clay"].get(player, rating), 2),
        "Grass Elo": round(player_surface_ratings["Grass"].get(player, rating), 2),
        "Last Match": last_played.strftime("%Y-%m-%d"),
        "Matches Last 6M": matches_last_6m,
        "Career Matches": career_matches,
        "Avg Elo Faced": round(avg_elo_faced, 2),
        "Matches vs Top 20": matches_vs_top20,
        "Winrate vs Top 20": round(winrate_vs_top20, 2),
        "Matches vs Top 50": matches_vs_top50,
        "Winrate vs Top 50": round(winrate_vs_top50, 2),
    }

ratings_df = pd.DataFrame.from_dict(final_ratings, orient="index").reset_index()
ratings_df.rename(columns={"index": "Player"}, inplace=True)
ratings_df.to_csv("player_elo_ratings_updated.csv", index=False)
print("✅ Elo ratings updated correctly.")