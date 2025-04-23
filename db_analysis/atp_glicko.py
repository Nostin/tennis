import sys
import os
import math
import pandas as pd
import logging
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import text

# -------------------------
# CONFIGURATION
# -------------------------
INITIAL_RATING = 1500
INITIAL_RD = 200
INITIAL_VOLATILITY = 0.06
TAU = 0.5
Q = math.log(10) / 300
EPSILON = 0.000001
ROLLING_WINDOW_DAYS = 365
SURFACE_TYPES = ["Hard", "Clay", "Grass"]
RATING_CAP = 2500
MIN_RATING = 1100
BATCH_SIZE = 500

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("glicko_update.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db_connect import get_engine

# Connect to database
engine = get_engine()

# Ensure Glicko columns
with engine.begin() as conn:
    conn.execute(text("""
        ALTER TABLE matched_atp_records
        DROP COLUMN IF EXISTS f_winner_overall_glicko,
        DROP COLUMN IF EXISTS f_winner_overall_rd,
        DROP COLUMN IF EXISTS f_winner_overall_volatility,
        DROP COLUMN IF EXISTS f_loser_overall_glicko,
        DROP COLUMN IF EXISTS f_loser_overall_rd,
        DROP COLUMN IF EXISTS f_loser_overall_volatility,
        DROP COLUMN IF EXISTS f_winner_surface_glicko,
        DROP COLUMN IF EXISTS f_winner_surface_rd,
        DROP COLUMN IF EXISTS f_winner_surface_volatility,
        DROP COLUMN IF EXISTS f_loser_surface_glicko,
        DROP COLUMN IF EXISTS f_loser_surface_rd,
        DROP COLUMN IF EXISTS f_loser_surface_volatility,
        DROP COLUMN IF EXISTS f_winner_blend_glicko,
        DROP COLUMN IF EXISTS f_loser_blend_glicko;
    """))
    
    conn.execute(text("""
        ALTER TABLE matched_atp_records
        ADD COLUMN IF NOT EXISTS f_winner_overall_glicko FLOAT,
        ADD COLUMN IF NOT EXISTS f_winner_overall_rd FLOAT,
        ADD COLUMN IF NOT EXISTS f_winner_overall_volatility FLOAT,
        ADD COLUMN IF NOT EXISTS f_loser_overall_glicko FLOAT,
        ADD COLUMN IF NOT EXISTS f_loser_overall_rd FLOAT,
        ADD COLUMN IF NOT EXISTS f_loser_overall_volatility FLOAT,
        ADD COLUMN IF NOT EXISTS f_winner_surface_glicko FLOAT,
        ADD COLUMN IF NOT EXISTS f_winner_surface_rd FLOAT,
        ADD COLUMN IF NOT EXISTS f_winner_surface_volatility FLOAT,
        ADD COLUMN IF NOT EXISTS f_loser_surface_glicko FLOAT,
        ADD COLUMN IF NOT EXISTS f_loser_surface_rd FLOAT,
        ADD COLUMN IF NOT EXISTS f_loser_surface_volatility FLOAT,
        ADD COLUMN IF NOT EXISTS f_winner_blend_glicko FLOAT,
        ADD COLUMN IF NOT EXISTS f_loser_blend_glicko FLOAT;
    """))

# Load match data
with engine.connect() as conn:
    df = pd.read_sql(text("""
        SELECT matchid, date, surface, winner_name, loser_name, score,
               w_svgms, l_svgms, w_bpfaced, w_bpsaved, l_bpfaced, l_bpsaved, comment,
               f_winner_overall_elo, f_loser_overall_elo
        FROM matched_atp_records
        ORDER BY date ASC, matchid ASC
    """), conn)

# Parse date and validate data
df["date"] = pd.to_datetime(df["date"])
df = df.dropna(subset=["date", "surface", "winner_name", "loser_name"])
df = df[df["surface"].isin(SURFACE_TYPES)]

# Initialize ratings and match counts
players = {}
player_surface_ratings = {surface: {} for surface in SURFACE_TYPES}
player_last_match = {}
player_match_count = {}
same_day_matches = {}

# Glicko-2 helpers
def glicko2_rating(player_rating, player_rd, player_volatility, opponent_rating, opponent_rd, match_result):
    player_rating = INITIAL_RATING if player_rating == 0 else player_rating
    opponent_rating = INITIAL_RATING if opponent_rating == 0 else opponent_rating
    player_rd = INITIAL_RD if player_rd == 0 else player_rd
    opponent_rd = INITIAL_RD if opponent_rd == 0 else opponent_rd
    player_volatility = INITIAL_VOLATILITY if player_volatility == 0 else max(player_volatility, INITIAL_VOLATILITY)

    mu = (player_rating - 1500) / 173.7178
    phi = player_rd / 173.7178
    opponent_mu = (opponent_rating - 1500) / 173.7178
    opponent_phi = opponent_rd / 173.7178

    g = 1 / math.sqrt(1 + 3 * Q * Q * opponent_phi * opponent_phi / (math.pi * math.pi))
    e = 1 / (1 + math.exp(-g * (mu - opponent_mu)))
    v = 1 / (g * g * e * (1 - e) + EPSILON)
    delta = v * g * (match_result - e)

    a = math.log(player_volatility * player_volatility)
    def f(x, delta=delta, phi=phi, v=v, a=a, q=Q, tau=TAU):
        ex = math.exp(x)
        term1 = ex * (delta * delta - phi * phi - v - ex) / (2 * (phi * phi + v + ex) ** 2)
        term2 = (x - a) / (tau * tau)
        return term1 - term2

    A = a
    B = 0
    if delta * delta > phi * phi + v:
        B = math.log(delta * delta - phi * phi - v)
    else:
        k = 1
        while f(a - k * TAU) < 0:
            k += 1
        B = a - k * TAU

    fA = f(A)
    fB = f(B)
    while abs(B - A) > 0.000001:
        C = A + (A - B) * fA / (fB - fA)
        fC = f(C)
        if fC * fB < 0:
            A = B
            fA = fB
        else:
            fA /= 2
        B = C
        fB = fC
    new_volatility = math.exp(A / 2)

    phi_star = math.sqrt(phi * phi + new_volatility * new_volatility)
    new_phi = 1 / math.sqrt(1 / (phi_star * phi_star) + 1 / v)
    new_mu = mu + Q / (new_phi * new_phi) * delta

    new_rating = 1500 + 173.7178 * new_mu
    new_rd = 173.7178 * new_phi

    new_rating = max(MIN_RATING, min(RATING_CAP, new_rating))
    new_rd = max(50, min(new_rd, INITIAL_RD * 2))

    return new_rating, new_rd, new_volatility

def elo_expected(r1, r2):
    return 1 / (1 + 10 ** ((r2 - r1) / 400))

def apply_rolling_window(match_date):
    cutoff = match_date - timedelta(days=ROLLING_WINDOW_DAYS)
    for player in list(players.keys()):
        if player in player_last_match and player_last_match[player] < cutoff:
            players[player]["rating"] = INITIAL_RATING
            players[player]["rd"] = INITIAL_RD
            players[player]["volatility"] = INITIAL_VOLATILITY
            for surface in SURFACE_TYPES:
                if player in player_surface_ratings[surface]:
                    player_surface_ratings[surface][player]["rating"] = INITIAL_RATING
                    player_surface_ratings[surface][player]["rd"] = INITIAL_RD
                    player_surface_ratings[surface][player]["volatility"] = INITIAL_VOLATILITY

logger.info("Updating ratings...")
updates = []
prediction_log = []

for idx, row in df.iterrows():
    winner = row["winner_name"]
    loser = row["loser_name"]
    match_date = row["date"]
    match_id = row["matchid"]
    surface = row["surface"]
    w_elo = row["f_winner_overall_elo"]
    l_elo = row["f_loser_overall_elo"]

    if row["comment"] == "Walkover" or surface not in SURFACE_TYPES:
        continue

    if pd.isna(w_elo) or pd.isna(l_elo) or not winner or not loser:
        logger.warning(f"Skipping match {match_id}: Missing data (winner={winner}, loser={loser}, w_elo={w_elo}, l_elo={l_elo})")
        continue

    # Track same-day matches
    match_date_str = match_date.strftime("%Y-%m-%d")
    for player in [winner, loser]:
        if player not in same_day_matches:
            same_day_matches[player] = {}
        if match_date_str not in same_day_matches[player]:
            same_day_matches[player][match_date_str] = []
        same_day_matches[player][match_date_str].append(match_id)

    apply_rolling_window(match_date)

    # Initialize players
    for player in [winner, loser]:
        if player not in players:
            players[player] = {
                "rating": INITIAL_RATING,
                "rd": INITIAL_RD,
                "volatility": INITIAL_VOLATILITY
            }
            player_match_count[player] = 0
        if player not in player_surface_ratings[surface]:
            player_surface_ratings[surface][player] = {
                "rating": INITIAL_RATING,
                "rd": INITIAL_RD,
                "volatility": INITIAL_VOLATILITY
            }

    # Check first match
    w_is_first = player_match_count.get(winner, 0) == 0
    l_is_first = player_match_count.get(loser, 0) == 0

    # Handle same-day matches: first match is lowest matchid
    if len(same_day_matches[winner].get(match_date_str, [])) > 1:
        w_is_first = w_is_first and match_id == min(same_day_matches[winner][match_date_str])
        logger.warning(f"Multiple matches for {winner} on {match_date_str}: {same_day_matches[winner][match_date_str]}, first={w_is_first}")
    if len(same_day_matches[loser].get(match_date_str, [])) > 1:
        l_is_first = l_is_first and match_id == min(same_day_matches[loser][match_date_str])
        logger.warning(f"Multiple matches for {loser} on {match_date_str}: {same_day_matches[loser][match_date_str]}, first={l_is_first}")

    # Get pre-match ratings
    w_rating = INITIAL_RATING if w_is_first else players[winner]["rating"]
    w_rd = INITIAL_RD if w_is_first else players[winner]["rd"]
    w_volatility = INITIAL_VOLATILITY if w_is_first else players[winner]["volatility"]
    l_rating = INITIAL_RATING if l_is_first else players[loser]["rating"]
    l_rd = INITIAL_RD if l_is_first else players[loser]["rd"]
    l_volatility = INITIAL_VOLATILITY if l_is_first else players[loser]["volatility"]
    w_s_rating = INITIAL_RATING if w_is_first else player_surface_ratings[surface][winner]["rating"]
    w_s_rd = INITIAL_RD if w_is_first else player_surface_ratings[surface][winner]["rd"]
    w_s_volatility = INITIAL_VOLATILITY if w_is_first else player_surface_ratings[surface][winner]["volatility"]
    l_s_rating = INITIAL_RATING if l_is_first else player_surface_ratings[surface][loser]["rating"]
    l_s_rd = INITIAL_RD if l_is_first else player_surface_ratings[surface][loser]["rd"]
    l_s_volatility = INITIAL_VOLATILITY if l_is_first else player_surface_ratings[surface][loser]["volatility"]

    if w_is_first:
        logger.info(f"First match for {winner}: match_id={match_id}, date={match_date_str}, count={player_match_count.get(winner, 0)}, rating={w_rating}, rd={w_rd}, volatility={w_volatility}")
    if l_is_first:
        logger.info(f"First match for {loser}: match_id={match_id}, date={match_date_str}, count={player_match_count.get(loser, 0)}, rating={l_rating}, rd={l_rd}, volatility={l_volatility}")

    # Compute expected win probability
    mu_w = (w_rating - 1500) / 173.7178
    mu_l = (l_rating - 1500) / 173.7178
    phi_l = l_rd / 173.7178
    g = 1 / math.sqrt(1 + 3 * Q * Q * phi_l * phi_l / (math.pi * math.pi))
    glicko_exp = 1 / (1 + math.exp(-g * (mu_w - mu_l)))
    elo_exp = elo_expected(w_elo, l_elo)

    prediction_log.append({
        "match_id": match_id,
        "winner": winner,
        "loser": loser,
        "glicko_expected": glicko_exp,
        "elo_expected": elo_exp,
        "actual": 1,
        "w_rating_pre": w_rating,
        "l_rating_pre": l_rating,
        "w_rd_pre": w_rd,
        "l_rd_pre": l_rd,
        "w_match_count": player_match_count.get(winner, 0),
        "l_match_count": player_match_count.get(loser, 0),
        "w_is_first": w_is_first,
        "l_is_first": l_is_first
    })

    # Calculate post-match ratings
    new_w_rating, new_w_rd, new_w_volatility = glicko2_rating(
        w_rating, w_rd, w_volatility, l_rating, l_rd, 1
    )
    new_l_rating, new_l_rd, new_l_volatility = glicko2_rating(
        l_rating, l_rd, l_volatility, w_rating, w_rd, 0
    )
    new_w_s_rating, new_w_s_rd, new_w_s_volatility = glicko2_rating(
        w_s_rating, w_s_rd, w_s_volatility, l_s_rating, l_s_rd, 1
    )
    new_l_s_rating, new_l_s_rd, new_l_s_volatility = glicko2_rating(
        l_s_rating, l_s_rd, l_s_volatility, w_s_rating, w_s_rd, 0
    )

    # Compute blended ratings (pre-match)
    w_blend_rating = (w_rating + w_s_rating) / 2
    l_blend_rating = (l_rating + l_s_rating) / 2

    # Store post-match ratings in players dictionary
    players[winner]["rating"] = new_w_rating
    players[winner]["rd"] = new_w_rd
    players[winner]["volatility"] = new_w_volatility
    players[loser]["rating"] = new_l_rating
    players[loser]["rd"] = new_l_rd
    players[loser]["volatility"] = new_l_volatility
    player_surface_ratings[surface][winner]["rating"] = new_w_s_rating
    player_surface_ratings[surface][winner]["rd"] = new_w_s_rd
    player_surface_ratings[surface][winner]["volatility"] = new_w_s_volatility
    player_surface_ratings[surface][loser]["rating"] = new_l_s_rating
    player_surface_ratings[surface][loser]["rd"] = new_l_s_rd
    player_surface_ratings[surface][loser]["volatility"] = new_l_s_volatility

    player_last_match[winner] = match_date
    player_last_match[loser] = match_date

    # Store pre-match ratings in updates
    updates.append({
        "matchid": int(row["matchid"]),
        "f_winner_overall_glicko": float(round(w_rating, 2)),
        "f_winner_overall_rd": float(round(w_rd, 2)),
        "f_winner_overall_volatility": float(round(w_volatility, 4)),
        "f_loser_overall_glicko": float(round(l_rating, 2)),
        "f_loser_overall_rd": float(round(l_rd, 2)),
        "f_loser_overall_volatility": float(round(l_volatility, 4)),
        "f_winner_surface_glicko": float(round(w_s_rating, 2)),
        "f_winner_surface_rd": float(round(w_s_rd, 2)),
        "f_winner_surface_volatility": float(round(w_s_volatility, 4)),
        "f_loser_surface_glicko": float(round(l_s_rating, 2)),
        "f_loser_surface_rd": float(round(l_s_rd, 2)),
        "f_loser_surface_volatility": float(round(l_s_volatility, 4)),
        "f_winner_blend_glicko": float(round(w_blend_rating, 2)),
        "f_loser_blend_glicko": float(round(l_blend_rating, 2))
    })

    # Increment match count after processing
    player_match_count[winner] = player_match_count.get(winner, 0) + 1
    player_match_count[loser] = player_match_count.get(loser, 0) + 1

    logger.info(f"Match {match_id}: {winner} (pre: {w_rating:.2f}, rd {w_rd:.2f}, post: {new_w_rating:.2f}, rd {new_w_rd:.2f}, count={player_match_count[winner]}, first={w_is_first}) vs "
                f"{loser} (pre: {l_rating:.2f}, rd {l_rd:.2f}, post: {new_l_rating:.2f}, rd {new_l_rd:.2f}, count={player_match_count[loser]}, first={l_is_first})")

# Evaluate predictive accuracy
logger.info("Evaluating predictive accuracy...")
pred_df = pd.DataFrame(prediction_log)
if not pred_df.empty and "elo_expected" in pred_df:
    glicko_log_loss = -sum(math.log(p) if p > 0 else 0 for p in pred_df["glicko_expected"]) / len(pred_df)
    elo_log_loss = -sum(math.log(p) if p > 0 else 0 for p in pred_df["elo_expected"] if pd.notna(p)) / len(pred_df[pred_df["elo_expected"].notna()])
    logger.info(f"Glicko Log Loss: {glicko_log_loss:.4f}")
    logger.info(f"Elo Log Loss: {elo_log_loss:.4f}")
    logger.info(f"Glicko {'better' if glicko_log_loss < elo_log_loss else 'worse'} than Elo")
    
    logger.info("Sample predictions:")
    for _, row in pred_df.tail(10).iterrows():
        logger.info(f"Match {row['match_id']}: {row['winner']} vs {row['loser']}, "
                    f"Glicko Expected: {row['glicko_expected']:.3f}, Elo Expected: {row['elo_expected']:.3f}, "
                    f"Pre-match: {row['winner']}={row['w_rating_pre']:.2f}, {row['loser']}={row['l_rating_pre']:.2f}, "
                    f"Counts: {row['winner']}={row['w_match_count']}, {row['loser']}={row['l_match_count']}, "
                    f"First: {row['winner']}={row['w_is_first']}, {row['loser']}={row['l_is_first']}")

# Validate ratings against Elo
logger.info("Validating ratings...")
for player in players:
    glicko_rating = players[player]["rating"]
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT elo, date FROM (
                SELECT f_winner_overall_elo AS elo, date
                FROM matched_atp_records
                WHERE winner_name = :player
                AND f_winner_overall_elo IS NOT NULL
                UNION
                SELECT f_loser_overall_elo AS elo, date
                FROM matched_atp_records
                WHERE loser_name = :player
                AND f_loser_overall_elo IS NOT NULL
            ) AS combined
            ORDER BY date DESC
            LIMIT 1
        """), {"player": player}).fetchone()
        elo_rating = result[0] if result else None
    if elo_rating and (glicko_rating > elo_rating + 200 or glicko_rating < elo_rating - 200):
        logger.warning(f"Rating anomaly for {player}: Glicko={glicko_rating:.2f}, Elo={elo_rating:.2f}")

# Write updates to database
logger.info("Writing updates to database...")
write_success = True
updates_df = pd.DataFrame(updates)
updates_df = updates_df.astype({
    "matchid": int,
    "f_winner_overall_glicko": float,
    "f_winner_overall_rd": float,
    "f_winner_overall_volatility": float,
    "f_loser_overall_glicko": float,
    "f_loser_overall_rd": float,
    "f_loser_overall_volatility": float,
    "f_winner_surface_glicko": float,
    "f_winner_surface_rd": float,
    "f_winner_surface_volatility": float,
    "f_loser_surface_glicko": float,
    "f_loser_surface_rd": float,
    "f_loser_surface_volatility": float,
    "f_winner_blend_glicko": float,
    "f_loser_blend_glicko": float
})

for i in range(0, len(updates_df), BATCH_SIZE):
    batch_df = updates_df.iloc[i:i + BATCH_SIZE]
    logger.info(f"Writing batch {i // BATCH_SIZE + 1} ({len(batch_df)} rows)...")
    try:
        with engine.begin() as conn:
            for _, row in batch_df.iterrows():
                row_data = {k: (float(v) if isinstance(v, (np.float64, np.float32)) else int(v) if isinstance(v, (np.int64, np.int32)) else v)
                            for k, v in row.items()}
                conn.execute(text("""
                    UPDATE matched_atp_records
                    SET
                        f_winner_overall_glicko = :f_winner_overall_glicko,
                        f_winner_overall_rd = :f_winner_overall_rd,
                        f_winner_overall_volatility = :f_winner_overall_volatility,
                        f_loser_overall_glicko = :f_loser_overall_glicko,
                        f_loser_overall_rd = :f_loser_overall_rd,
                        f_loser_overall_volatility = :f_loser_overall_volatility,
                        f_winner_surface_glicko = :f_winner_surface_glicko,
                        f_winner_surface_rd = :f_winner_surface_rd,
                        f_winner_surface_volatility = :f_winner_surface_volatility,
                        f_loser_surface_glicko = :f_loser_surface_glicko,
                        f_loser_surface_rd = :f_loser_surface_rd,
                        f_loser_surface_volatility = :f_loser_surface_volatility,
                        f_winner_blend_glicko = :f_winner_blend_glicko,
                        f_loser_blend_glicko = :f_loser_blend_glicko
                    WHERE matchid = :matchid
                """), row_data)
        logger.info(f"Batch {i // BATCH_SIZE + 1} written successfully")
    except Exception as e:
        logger.error(f"Batch {i // BATCH_SIZE + 1} failed: {e}")
        write_success = False
        break

# Verify first-match ratings
logger.info("Verifying first-match ratings...")
with engine.connect() as conn:
    first_match_df = pd.read_sql(text("""
        SELECT matchid, date, winner_name, f_winner_overall_glicko, f_winner_overall_rd,
               loser_name, f_loser_overall_glicko, f_loser_overall_rd
        FROM matched_atp_records
        WHERE (winner_name, date) IN (
            SELECT winner_name, MIN(date)
            FROM matched_atp_records
            WHERE f_winner_overall_glicko IS NOT NULL
            GROUP BY winner_name
        ) OR (loser_name, date) IN (
            SELECT loser_name, MIN(date)
            FROM matched_atp_records
            WHERE f_loser_overall_glicko IS NOT NULL
            GROUP BY loser_name
        )
        ORDER BY date ASC
        LIMIT 50
    """), conn)
    for _, row in first_match_df.iterrows():
        match_id = row["matchid"]
        date = row["date"]
        winner = row["winner_name"]
        loser = row["loser_name"]
        w_glicko = row["f_winner_overall_glicko"]
        w_rd = row["f_winner_overall_rd"]
        l_glicko = row["f_loser_overall_glicko"]
        l_rd = row["f_loser_overall_rd"]
        if w_glicko != INITIAL_RATING or w_rd != INITIAL_RD:
            logger.error(f"First-match error for {winner}: match_id={match_id}, date={date}, Glicko={w_glicko}, RD={w_rd}, expected 1500/200")
        if l_glicko != INITIAL_RATING or l_rd != INITIAL_RD:
            logger.error(f"First-match error for {loser}: match_id={match_id}, date={date}, Glicko={l_glicko}, RD={l_rd}, expected 1500/200")

# Print sample output
logger.info("Sample updated rows (first 20):")
with engine.connect() as conn:
    sample_df = pd.read_sql(text("""
        SELECT winner_name, f_winner_overall_elo, f_winner_overall_glicko, f_winner_overall_rd,
               loser_name, f_loser_overall_elo, f_loser_overall_glicko, f_loser_overall_rd
        FROM matched_atp_records
        WHERE f_winner_overall_glicko IS NOT NULL
        ORDER BY date DESC
        LIMIT 20
    """), conn)
    for _, row in sample_df.iterrows():
        logger.info(f"{row['winner_name']} (Elo: {row['f_winner_overall_elo']:.2f}, Glicko: {row['f_winner_overall_glicko']:.2f}, RD: {row['f_winner_overall_rd']:.2f}) vs "
                    f"{row['loser_name']} (Elo: {row['f_loser_overall_elo']:.2f}, Glicko: {row['f_loser_overall_glicko']:.2f}, RD: {row['f_loser_overall_rd']:.2f})")

if write_success:
    logger.info("✅ Glicko ratings updated successfully.")
else:
    logger.error("❌ Glicko ratings update failed due to database errors.")
    sys.exit(1)