import sys
import os
import re
from sqlalchemy import text

# Ensure imports work from parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_connect import get_engine
from mappings import atp_player_name_mapping

# Get the database engine
engine = get_engine()

def normalize_name(name):
    return re.sub(r'\s+', ' ', name).strip().lower() if name else None

def match_exact_player_names():
    """Create ATP unique names table and update normalized names in betfair_odds."""
    print("Creating ATP unique names table...")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS atp_unique_names CASCADE;"))
        conn.execute(text("""
            CREATE TABLE atp_unique_names AS
            SELECT DISTINCT name
            FROM (
                SELECT winner_name AS name FROM matched_atp_records
                UNION
                SELECT loser_name AS name FROM matched_atp_records
            ) AS all_names;
        """))

        print("Creating index on atp_unique_names for normalized matching...")
        conn.execute(text("""
            CREATE INDEX idx_normalized_name ON atp_unique_names (
                LOWER(REGEXP_REPLACE(name, '\\s+', ' ', 'g'))
            );
        """))

        print("Updating normalized player names using JOIN...")
        conn.execute(text("""
            UPDATE betfair_odds AS bo
            SET player1_name_normalised = un.name
            FROM atp_unique_names AS un
            WHERE LOWER(REGEXP_REPLACE(un.name, '\\s+', ' ', 'g')) = LOWER(REGEXP_REPLACE(bo.player1_name, '\\s+', ' ', 'g'));

            UPDATE betfair_odds AS bo
            SET player2_name_normalised = un.name
            FROM atp_unique_names AS un
            WHERE LOWER(REGEXP_REPLACE(un.name, '\\s+', ' ', 'g')) = LOWER(REGEXP_REPLACE(bo.player2_name, '\\s+', ' ', 'g'));
        """))
    print("Exact name matching complete.")

def normalize_names():
    """Normalize player names in betfair_odds using the mapping."""
    print("Normalizing player names using provided mapping...")

    print("Preparing data for bulk insert...")
    mapping_data = [(original, normalized) for original, normalized in atp_player_name_mapping.items()]

    with engine.begin() as conn:
        print("Creating temporary mapping table...")
        conn.execute(text("DROP TABLE IF EXISTS temp_name_mapping;"))
        conn.execute(text("""
            CREATE TEMP TABLE temp_name_mapping (
                original_name VARCHAR(255),
                normalized_name VARCHAR(255)
            );
        """))

        print(f"Inserting {len(mapping_data)} name mappings in bulk...")
        conn.execute(
            text("INSERT INTO temp_name_mapping (original_name, normalized_name) VALUES (:original, :normalized)"),
            [{"original": o, "normalized": n} for o, n in mapping_data]
        )

        print("Updating player1_name_normalised via JOIN...")
        conn.execute(text("""
            UPDATE betfair_odds AS bo
            SET player1_name_normalised = tm.normalized_name
            FROM temp_name_mapping AS tm
            WHERE bo.player1_name = tm.original_name;
        """))

        print("Updating player2_name_normalised via JOIN...")
        conn.execute(text("""
            UPDATE betfair_odds AS bo
            SET player2_name_normalised = tm.normalized_name
            FROM temp_name_mapping AS tm
            WHERE bo.player2_name = tm.original_name;
        """))

        print("Finalizing odds_player_name_normalised...")
        conn.execute(text("""
            UPDATE betfair_odds
            SET odds_player_name_normalised = 
                CASE
                    WHEN odds_player_name = player1_name THEN player1_name_normalised
                    WHEN odds_player_name = player2_name THEN player2_name_normalised
                    ELSE NULL
                END;
        """))
    print("Mapping-based normalization complete.")

def suggest_indexes():
    with engine.begin() as conn:
        print("Creating indexes to improve performance (if not already present)...")
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_betfair_player1_name ON betfair_odds(player1_name);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_betfair_player2_name ON betfair_odds(player2_name);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_betfair_odds_player_name ON betfair_odds(odds_player_name);"))
    print("Index suggestion complete.")

if __name__ == "__main__":
    suggest_indexes()
    match_exact_player_names()
    normalize_names()
