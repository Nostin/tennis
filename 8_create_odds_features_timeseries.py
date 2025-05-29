import sys
import os
import numpy as np
from sqlalchemy import text
from db_connect import get_engine

# Add project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Initialize DB engine
engine = get_engine()

# -------------------------------
# Helper function to calculate odds movement features
# -------------------------------
def calculate_odds_features(odds_list):
    clean_odds = [float(o) for o in odds_list if o is not None and o > 1.0]
    
    if len(clean_odds) < 3:
        return None  # Not enough data to meaningfully calculate trends

    # Fill missing values with forward/backward fill or linear interpolation if you want
    # For now, just proceed with available values
    probs = np.array([1.0 / float(o) for o in clean_odds], dtype=float)

    deltas = np.diff(probs)
    volatility = np.sum(np.abs(deltas))
    x = np.linspace(0, 1, len(probs))
    trend_slope = np.polyfit(x, probs, 1)[0] if len(probs) >= 2 else None
    max_swing = np.max(np.abs(deltas)) if len(deltas) > 0 else None
    early_move = probs[min(5, len(probs)-1)] - probs[0]
    late_move = probs[-1] - probs[min(5, len(probs)-1)]
    vol_ratio = abs(late_move) / (abs(early_move) + 1e-6)
    net_move = probs[-1] - probs[0]
    curvature = np.polyfit(x, probs, 2)[0] if len(probs) >= 3 else None

    features = {}
    for i in range(10):
        try:
            features[f"deltap_{i+1}"] = deltas[i]
        except IndexError:
            features[f"deltap_{i+1}"] = None

    features.update({
        "volatility": volatility,
        "trend_slope": trend_slope,
        "max_swing": max_swing,
        "early_move": early_move,
        "late_move": late_move,
        "vol_ratio": vol_ratio,
        "net_move": net_move,
        "curvature": curvature
    })

    return features


# -------------------------------
# Step 1: Drop old columns
# -------------------------------
drop_columns_sql = """
ALTER TABLE matched_atp_records 
    DROP COLUMN IF EXISTS f_winner_odds_volatility,
    DROP COLUMN IF EXISTS f_winner_odds_trend_slope,
    DROP COLUMN IF EXISTS f_winner_odds_max_swing,
    DROP COLUMN IF EXISTS f_winner_odds_early_move,
    DROP COLUMN IF EXISTS f_winner_odds_late_move,
    DROP COLUMN IF EXISTS f_winner_odds_vol_ratio,
    DROP COLUMN IF EXISTS f_winner_odds_net_move,
    DROP COLUMN IF EXISTS f_winner_odds_curvature,
    DROP COLUMN IF EXISTS f_loser_odds_volatility,
    DROP COLUMN IF EXISTS f_loser_odds_trend_slope,
    DROP COLUMN IF EXISTS f_loser_odds_max_swing,
    DROP COLUMN IF EXISTS f_loser_odds_early_move,
    DROP COLUMN IF EXISTS f_loser_odds_late_move,
    DROP COLUMN IF EXISTS f_loser_odds_vol_ratio,
    DROP COLUMN IF EXISTS f_loser_odds_net_move,
    DROP COLUMN IF EXISTS f_loser_odds_curvature;
"""

# -------------------------------
# Step 2: Add new columns
# -------------------------------
add_columns_sql = ""

for role in ["winner", "loser"]:
    for i in range(1, 11):
        add_columns_sql += f"ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_{role}_odds_deltap_{i} FLOAT;\n"
    add_columns_sql += f"""
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_{role}_odds_volatility FLOAT;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_{role}_odds_trend_slope FLOAT;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_{role}_odds_max_swing FLOAT;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_{role}_odds_early_move FLOAT;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_{role}_odds_late_move FLOAT;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_{role}_odds_vol_ratio FLOAT;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_{role}_odds_net_move FLOAT;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_{role}_odds_curvature FLOAT;
"""

# -------------------------------
# Step 3: Migration Runner
# -------------------------------
with engine.begin() as conn:
    print("🔧 Dropping old columns...")
    conn.execute(text(drop_columns_sql))
    print("✅ Old columns dropped.")

    print("🧱 Adding new feature columns...")
    conn.execute(text(add_columns_sql))
    print("✅ New columns added.")

# -------------------------------
# Step 4: Fetch, Calculate, and Update Features
# -------------------------------

print("📦 Fetching match odds data...")

with engine.begin() as conn:
    result = conn.execute(text("""
        SELECT
            mar.matchid AS match_id,
            mar.betfair_odds_id_winner,
            mar.betfair_odds_id_loser,
            bwo.odds_00pct AS w_00,
            bwo.odds_10pct AS w_10,
            bwo.odds_20pct AS w_20,
            bwo.odds_30pct AS w_30,
            bwo.odds_40pct AS w_40,
            bwo.odds_50pct AS w_50,
            bwo.odds_60pct AS w_60,
            bwo.odds_70pct AS w_70,
            bwo.odds_80pct AS w_80,
            bwo.odds_90pct AS w_90,
            bwo.odds_100pct AS w_100,
            blo.odds_00pct AS l_00,
            blo.odds_10pct AS l_10,
            blo.odds_20pct AS l_20,
            blo.odds_30pct AS l_30,
            blo.odds_40pct AS l_40,
            blo.odds_50pct AS l_50,
            blo.odds_60pct AS l_60,
            blo.odds_70pct AS l_70,
            blo.odds_80pct AS l_80,
            blo.odds_90pct AS l_90,
            blo.odds_100pct AS l_100
        FROM matched_atp_records mar
        LEFT JOIN betfair_odds bwo ON mar.betfair_odds_id_winner = bwo.record_id
        LEFT JOIN betfair_odds blo ON mar.betfair_odds_id_loser = blo.record_id
    """)).mappings().all()

    print(f"📈 Processing {len(result)} matches...")

    batch = []
    batch_size = 500
    updated_matches = 0

    for row in result:
        winner_odds = [row[f"w_{str(i*10).zfill(2)}"] for i in range(11)]
        loser_odds = [row[f"l_{str(i*10).zfill(2)}"] for i in range(11)]

        winner_features = calculate_odds_features(winner_odds)
        loser_features = calculate_odds_features(loser_odds)

        if not winner_features or not loser_features:
            continue  # Skip matches where odds are missing or broken

        # Build update SQL
        set_clause = ", ".join(
            [f"f_winner_odds_{k} = :w_{k}" for k in winner_features.keys()] +
            [f"f_loser_odds_{k} = :l_{k}" for k in loser_features.keys()]
        )
        update_sql = f"""
        UPDATE matched_atp_records
        SET {set_clause}
        WHERE matchid = :match_id
        """

        params = {f"w_{k}": float(v) if v is not None else None for k, v in winner_features.items()}
        params.update({f"l_{k}": float(v) if v is not None else None for k, v in loser_features.items()})
        params["match_id"] = row["match_id"]

        batch.append((update_sql, params))

        if len(batch) >= batch_size:
          for sql, p in batch:
              conn.execute(text(sql), p)
          print(f"✅ Processed {updated_matches + len(batch)} matches...")
          updated_matches += len(batch)
          batch = []

    # Final leftover batch
    for sql, p in batch:
        conn.execute(text(sql), p)
    conn.commit()
    updated_matches += len(batch)

    print(f"🏁 Completed updates for {updated_matches} matches.")
