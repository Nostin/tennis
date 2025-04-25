import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from datetime import timedelta
from sqlalchemy import text
from db_connect import get_engine

engine = get_engine()
TABLE_NAME = "matched_atp_records"

def parse_sets(score):
    sets = str(score).split()
    return [s for s in sets if '-' in s and s[0].isdigit()]

def parse_tiebreaks(sets):
    return [s for s in sets if "7-6" in s or "6-7" in s]

# -------------------------
# Reset the feature columns
# -------------------------
with engine.begin() as conn:
    conn.execute(text("""
        ALTER TABLE matched_atp_records
        DROP COLUMN IF EXISTS f_w_tbs_played,
        DROP COLUMN IF EXISTS f_w_tbs_won,          
        DROP COLUMN IF EXISTS f_w_tb_win_pct,
        DROP COLUMN IF EXISTS f_w_tb_rate,
        DROP COLUMN IF EXISTS f_w_tbs_played_30d,
        DROP COLUMN IF EXISTS f_w_tbs_won_30d,
        DROP COLUMN IF EXISTS f_w_tb_win_pct_30d,
        DROP COLUMN IF EXISTS f_w_tb_rate_30d,
        DROP COLUMN IF EXISTS f_w_tbs_played_hard,
        DROP COLUMN IF EXISTS f_w_tbs_played_clay,
        DROP COLUMN IF EXISTS f_w_tbs_played_grass,
        DROP COLUMN IF EXISTS f_w_tbs_won_hard,
        DROP COLUMN IF EXISTS f_w_tbs_won_clay,
        DROP COLUMN IF EXISTS f_w_tbs_won_grass,
        DROP COLUMN IF EXISTS f_w_tb_win_pct_hard,
        DROP COLUMN IF EXISTS f_w_tb_win_pct_clay,
        DROP COLUMN IF EXISTS f_w_tb_win_pct_grass,
        DROP COLUMN IF EXISTS f_w_tb_rate_hard,
        DROP COLUMN IF EXISTS f_w_tb_rate_clay,
        DROP COLUMN IF EXISTS f_w_tb_rate_grass,
        DROP COLUMN IF EXISTS f_l_tbs_played,
        DROP COLUMN IF EXISTS f_l_tbs_won,          
        DROP COLUMN IF EXISTS f_l_tb_win_pct,
        DROP COLUMN IF EXISTS f_l_tb_rate,
        DROP COLUMN IF EXISTS f_l_tbs_played_30d,
        DROP COLUMN IF EXISTS f_l_tbs_won_30d,
        DROP COLUMN IF EXISTS f_l_tb_win_pct_30d,
        DROP COLUMN IF EXISTS f_l_tb_rate_30d,
        DROP COLUMN IF EXISTS f_l_tbs_played_hard,
        DROP COLUMN IF EXISTS f_l_tbs_played_clay,
        DROP COLUMN IF EXISTS f_l_tbs_played_grass,
        DROP COLUMN IF EXISTS f_l_tbs_won_hard,
        DROP COLUMN IF EXISTS f_l_tbs_won_clay,
        DROP COLUMN IF EXISTS f_l_tbs_won_grass,
        DROP COLUMN IF EXISTS f_l_tb_win_pct_hard,
        DROP COLUMN IF EXISTS f_l_tb_win_pct_clay,
        DROP COLUMN IF EXISTS f_l_tb_win_pct_grass,
        DROP COLUMN IF EXISTS f_l_tb_rate_hard,
        DROP COLUMN IF EXISTS f_l_tb_rate_clay,
        DROP COLUMN IF EXISTS f_l_tb_rate_grass;
    """))

    conn.execute(text("""
        ALTER TABLE matched_atp_records
        ADD COLUMN IF NOT EXISTS f_w_tbs_played INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_tbs_won INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_tb_win_pct DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_w_tb_rate DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_w_tbs_played_30d INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_tbs_won_30d INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_tb_win_pct_30d DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_w_tb_rate_30d DECIMAL(5,2),   
        ADD COLUMN IF NOT EXISTS f_w_tbs_played_hard INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_tbs_played_clay INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_tbs_played_grass INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_tbs_won_hard INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_tbs_won_clay INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_tbs_won_grass INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_w_tb_win_pct_hard DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_w_tb_win_pct_clay DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_w_tb_win_pct_grass DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_w_tb_rate_hard DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_w_tb_rate_clay DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_w_tb_rate_grass DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_l_tbs_played INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_tbs_won INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_tb_win_pct DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_l_tb_rate DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_l_tbs_played_30d INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_tbs_won_30d INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_tb_win_pct_30d DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_l_tb_rate_30d DECIMAL(5,2),   
        ADD COLUMN IF NOT EXISTS f_l_tbs_played_hard INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_tbs_played_clay INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_tbs_played_grass INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_tbs_won_hard INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_tbs_won_clay INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_tbs_won_grass INT DEFAULT 0,
        ADD COLUMN IF NOT EXISTS f_l_tb_win_pct_hard DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_l_tb_win_pct_clay DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_l_tb_win_pct_grass DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_l_tb_rate_hard DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_l_tb_rate_clay DECIMAL(5,2),
        ADD COLUMN IF NOT EXISTS f_l_tb_rate_grass DECIMAL(5,2);
    """))

# Load match data
with engine.connect() as conn:
    df = pd.read_sql(f"""
        SELECT matchid, date, winner_name, loser_name, surface, score
        FROM {TABLE_NAME}
        WHERE comment IS NULL OR comment != 'Walkover'
        ORDER BY date ASC
    """, conn)

df["date"] = pd.to_datetime(df["date"])

# Init tracking structures
player_data = {}

def update_player(player, match_date, surface, sets, tbs_won, tbs_total):
    history = player_data.setdefault(player, {
        "matches": [],
        "sets": [],
        "tbs": [],
        "tbs_won": [],
        "surfaces": {"Hard": [], "Clay": [], "Grass": []}
    })

    history["matches"].append((match_date, surface))
    history["sets"].append((match_date, len(sets)))
    history["tbs"].append((match_date, tbs_total))
    history["tbs_won"].append((match_date, tbs_won))
    history["surfaces"][surface].append((match_date, len(sets), tbs_won, tbs_total))

def calc_tb_features(player, current_date, surface):
    history = player_data.get(player, {})
    if not history:
        return [0]*18

    def recent(data, days=30):
        return [(d, *rest) for d, *rest in data if (current_date - d).days <= days]

    sets_all = sum(s for _, s in history.get("sets", []))
    tbs_all = sum(t for _, t in history.get("tbs", []))
    tbs_won_all = sum(w for _, w in history.get("tbs_won", []))

    tb_win_pct = round((tbs_won_all / tbs_all) * 100, 2) if tbs_all else 0
    tb_rate = round((tbs_all / sets_all) * 100, 2) if sets_all else 0

    sets_30 = recent(history.get("sets", []))
    tbs_30 = recent(history.get("tbs", []))
    tbs_won_30 = recent(history.get("tbs_won", []))

    sets30 = sum(s for _, s in sets_30)
    tbs30 = sum(t for _, t in tbs_30)
    won30 = sum(w for _, w in tbs_won_30)

    tb_pct_30 = round((won30 / tbs30) * 100, 2) if tbs30 else 0
    tb_rate_30 = round((tbs30 / sets30) * 100, 2) if sets30 else 0

    surf = recent(history["surfaces"].get(surface, []))
    surf_sets = sum(s for _, s, _, _ in surf)
    surf_tb = sum(tb for _, _, _, tb in surf)
    surf_won = sum(w for _, _, w, _ in surf)

    surf_tb_pct = round((surf_won / surf_tb) * 100, 2) if surf_tb else 0
    surf_tb_rate = round((surf_tb / surf_sets) * 100, 2) if surf_sets else 0

    return [tbs_all, tbs_won_all, tb_win_pct, tb_rate,
            tbs30, won30, tb_pct_30, tb_rate_30,
            surf_tb, surf_won, surf_tb_pct, surf_tb_rate]

# -------------------------
# Process and update
# -------------------------
with engine.begin() as conn:
    for _, row in df.iterrows():
        matchid = row["matchid"]
        match_date = row["date"]
        surface = row["surface"]
        winner = row["winner_name"]
        loser = row["loser_name"]
        score = row["score"]

        sets = parse_sets(score)
        tbs = parse_tiebreaks(sets)

        num_tbs = sum(1 for s in sets if "7-6" in s or "6-7" in s)
        winner_tbs_won = sum(1 for s in sets if "7-6" in s)
        loser_tbs_won = sum(1 for s in sets if "6-7" in s)

        # Calculate features BEFORE adding this match to history
        f_w_values = calc_tb_features(winner, match_date, surface)
        f_l_values = calc_tb_features(loser, match_date, surface)

        # THEN update history
        update_player(winner, match_date, surface, sets, winner_tbs_won, num_tbs)
        update_player(loser, match_date, surface, sets, loser_tbs_won, num_tbs)

        conn.execute(text(f"""
            UPDATE {TABLE_NAME}
            SET
                f_w_tbs_played = :w_tb_p,
                f_w_tbs_won = :w_tb_w,
                f_w_tb_win_pct = :w_pct,
                f_w_tb_rate = :w_rate,
                f_w_tbs_played_30d = :w_tb30,
                f_w_tbs_won_30d = :w_tbw30,
                f_w_tb_win_pct_30d = :w_pct30,
                f_w_tb_rate_30d = :w_rate30,
                f_w_tbs_played_{surface.lower()} = :w_tbs_surf,
                f_w_tbs_won_{surface.lower()} = :w_won_surf,
                f_w_tb_win_pct_{surface.lower()} = :w_pct_surf,
                f_w_tb_rate_{surface.lower()} = :w_rate_surf,

                f_l_tbs_played = :l_tb_p,
                f_l_tbs_won = :l_tb_w,
                f_l_tb_win_pct = :l_pct,
                f_l_tb_rate = :l_rate,
                f_l_tbs_played_30d = :l_tb30,
                f_l_tbs_won_30d = :l_tbw30,
                f_l_tb_win_pct_30d = :l_pct30,
                f_l_tb_rate_30d = :l_rate30,
                f_l_tbs_played_{surface.lower()} = :l_tbs_surf,
                f_l_tbs_won_{surface.lower()} = :l_won_surf,
                f_l_tb_win_pct_{surface.lower()} = :l_pct_surf,
                f_l_tb_rate_{surface.lower()} = :l_rate_surf

            WHERE matchid = :matchid
        """), {
            "matchid": matchid,
            **{f"w_tb_p": f_w_values[0], "w_tb_w": f_w_values[1], "w_pct": f_w_values[2], "w_rate": f_w_values[3],
               "w_tb30": f_w_values[4], "w_tbw30": f_w_values[5], "w_pct30": f_w_values[6], "w_rate30": f_w_values[7],
               "w_tbs_surf": f_w_values[8], "w_won_surf": f_w_values[9], "w_pct_surf": f_w_values[10], "w_rate_surf": f_w_values[11],
               "l_tb_p": f_l_values[0], "l_tb_w": f_l_values[1], "l_pct": f_l_values[2], "l_rate": f_l_values[3],
               "l_tb30": f_l_values[4], "l_tbw30": f_l_values[5], "l_pct30": f_l_values[6], "l_rate30": f_l_values[7],
               "l_tbs_surf": f_l_values[8], "l_won_surf": f_l_values[9], "l_pct_surf": f_l_values[10], "l_rate_surf": f_l_values[11]}
        })

print("✅ Tie-break features calculated and stored.")
