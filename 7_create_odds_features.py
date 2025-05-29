import sys
import os
from sqlalchemy import text
from db_connect import get_engine

# Add project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

engine = get_engine()

# -------------------------------
# 1. Drop columns individually (PostgreSQL syntax requires separate statements)
# -------------------------------
drop_columns_sql = """
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_winner_odds_shape;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_winner_odds_strength;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_winner_shortened_fav;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_winner_late_money;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_winner_was_fav;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_winner_was_fav_closing;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_winner_became_fav;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_winner_lost_fav;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_winner_stayed_fav;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_loser_odds_shape;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_loser_odds_strength;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_loser_shortened_fav;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_loser_late_money;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_loser_was_fav;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_loser_was_fav_closing;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_loser_became_fav;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_loser_lost_fav;
ALTER TABLE matched_atp_records DROP COLUMN IF EXISTS f_loser_stayed_fav;
"""

# -------------------------------
# 2. Add columns (safely using IF NOT EXISTS)
# -------------------------------
add_columns_sql = """
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_winner_odds_shape TEXT;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_winner_odds_strength FLOAT;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_winner_shortened_fav BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_winner_late_money BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_winner_was_fav BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_winner_was_fav_closing BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_winner_became_fav BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_winner_lost_fav BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_winner_stayed_fav BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_loser_odds_shape TEXT;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_loser_odds_strength FLOAT;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_loser_shortened_fav BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_loser_late_money BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_loser_was_fav BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_loser_was_fav_closing BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_loser_became_fav BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_loser_lost_fav BOOLEAN;
ALTER TABLE matched_atp_records ADD COLUMN IF NOT EXISTS f_loser_stayed_fav BOOLEAN;
"""

# -------------------------------
# 3. Update values by joining with betfair_odds
# -------------------------------
update_values_sql = """
UPDATE matched_atp_records mar
SET
  f_winner_odds_shape = bwo.shape,
  f_winner_odds_strength = bwo.shape_strength,
  f_winner_shortened_fav = bwo.shortened_fav,
  f_winner_late_money = bwo.late_money,
  f_loser_odds_shape = blo.shape,
  f_loser_odds_strength = blo.shape_strength,
  f_loser_shortened_fav = blo.shortened_fav,
  f_loser_late_money = blo.late_money,
  f_winner_was_fav = (bwo.odds_00pct < 2.0 AND bwo.odds_00pct < blo.odds_00pct),
  f_loser_was_fav = (blo.odds_00pct < 2.0 AND blo.odds_00pct < bwo.odds_00pct),

  f_winner_was_fav_closing = (bwo.odds_100pct < 2.0 AND bwo.odds_100pct < blo.odds_100pct),
  f_loser_was_fav_closing = (blo.odds_100pct < 2.0 AND blo.odds_100pct < bwo.odds_100pct),

  f_winner_became_fav = (NOT (bwo.odds_00pct < 2.0 AND bwo.odds_00pct < blo.odds_00pct)) AND
                        (bwo.odds_100pct < 2.0 AND bwo.odds_100pct < blo.odds_100pct),

  f_loser_became_fav = (NOT (blo.odds_00pct < 2.0 AND blo.odds_00pct < bwo.odds_00pct)) AND
                       (blo.odds_100pct < 2.0 AND blo.odds_100pct < bwo.odds_100pct),

  f_winner_lost_fav = (bwo.odds_00pct < 2.0 AND bwo.odds_00pct < blo.odds_00pct) AND
                      NOT (bwo.odds_100pct < 2.0 AND bwo.odds_100pct < blo.odds_100pct),

  f_loser_lost_fav = (blo.odds_00pct < 2.0 AND blo.odds_00pct < bwo.odds_00pct) AND
                     NOT (blo.odds_100pct < 2.0 AND blo.odds_100pct < bwo.odds_100pct),

  f_winner_stayed_fav = (bwo.odds_00pct < 2.0 AND bwo.odds_00pct < blo.odds_00pct) AND
                        (bwo.odds_100pct < 2.0 AND bwo.odds_100pct < blo.odds_100pct),

  f_loser_stayed_fav = (blo.odds_00pct < 2.0 AND blo.odds_00pct < bwo.odds_00pct) AND
                       (blo.odds_100pct < 2.0 AND blo.odds_100pct < bwo.odds_100pct)
FROM betfair_odds bwo, betfair_odds blo
WHERE
  mar.betfair_odds_id_winner = bwo.record_id AND
  mar.betfair_odds_id_loser = blo.record_id;
"""

# -------------------------------
# Execute the migration
# -------------------------------
with engine.begin() as conn:
    print("🔧 Dropping old columns if they exist...")
    conn.execute(text(drop_columns_sql))
    print("✅ Old columns dropped.")

    print("🧱 Adding new columns...")
    conn.execute(text(add_columns_sql))
    print("✅ New columns added.")

    print("📥 Populating columns with odds shape data...")
    conn.execute(text(update_values_sql))
    print("✅ Data updated successfully.")
