import sys
import os
import pandas as pd

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy import text
from db_connect import get_engine

# Connect to DB
engine = get_engine()
TABLE_NAME = "matched_atp_records"

print("🛠️ Updating database schema...")

with engine.begin() as conn:
    conn.execute(text(f"""
        ALTER TABLE {TABLE_NAME}
        DROP COLUMN IF EXISTS f_w_total_holds,
        DROP COLUMN IF EXISTS f_w_total_breaks,
        DROP COLUMN IF EXISTS f_w_hold_pct,
        DROP COLUMN IF EXISTS f_w_break_pct,
        DROP COLUMN IF EXISTS f_w_bp_conversion_pct,
        DROP COLUMN IF EXISTS f_l_total_holds,
        DROP COLUMN IF EXISTS f_l_total_breaks,
        DROP COLUMN IF EXISTS f_l_hold_pct,
        DROP COLUMN IF EXISTS f_l_break_pct,
        DROP COLUMN IF EXISTS f_l_bp_conversion_pct;
    """))

    conn.execute(text(f"""
        ALTER TABLE {TABLE_NAME}
        ADD COLUMN IF NOT EXISTS f_w_total_holds INT,
        ADD COLUMN IF NOT EXISTS f_w_total_breaks INT,
        ADD COLUMN IF NOT EXISTS f_w_hold_pct NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_break_pct NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_bp_conversion_pct NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_total_holds INT,
        ADD COLUMN IF NOT EXISTS f_l_total_breaks INT,
        ADD COLUMN IF NOT EXISTS f_l_hold_pct NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_break_pct NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_bp_conversion_pct NUMERIC(5, 2);
    """))

print("🔍 Loading match data...")

with engine.connect() as conn:
    df = pd.read_sql(text(f"""
        SELECT matchid, date, surface,
               winner_name, loser_name,
               w_svgms, l_svgms,
               w_bpfaced, w_bpsaved,
               l_bpfaced, l_bpsaved
        FROM {TABLE_NAME}
        WHERE comment IS NULL OR comment != 'Walkover'
        ORDER BY date ASC
    """), conn)

df["date"] = pd.to_datetime(df["date"])
player_stats = {}

def calc_features(player, current_date):
    records = player_stats.get(player, [])
    records = [r for r in records if r["date"] < current_date]

    svgms = sum(r["svgms"] for r in records)
    bpfaced = sum(r["bpfaced"] for r in records)
    bpsaved = sum(r["bpsaved"] for r in records)
    opp_bpfaced = sum(r["opp_bpfaced"] for r in records)
    opp_bpsaved = sum(r["opp_bpsaved"] for r in records)

    holds = svgms - bpfaced + bpsaved
    breaks = opp_bpfaced - opp_bpsaved

    hold_pct = round((holds / svgms) * 100, 2) if svgms else 0
    break_pct = round((breaks / (opp_bpfaced / 6)) * 100, 2) if opp_bpfaced else 0
    bp_conv_pct = round((breaks / opp_bpfaced) * 100, 2) if opp_bpfaced else 0

    return holds, breaks, hold_pct, break_pct, bp_conv_pct

def update_player(player, date, surface, svgms, bpfaced, bpsaved, opp_bpfaced, opp_bpsaved):
    player_stats.setdefault(player, []).append({
        "date": date,
        "surface": surface,
        "svgms": svgms or 0,
        "bpfaced": bpfaced or 0,
        "bpsaved": bpsaved or 0,
        "opp_bpfaced": opp_bpfaced or 0,
        "opp_bpsaved": opp_bpsaved or 0,
    })

def safe_int(val):
    try:
        return int(val) if pd.notna(val) else 0
    except:
        return 0

def safe_float(val):
    try:
        return float(val) if pd.notna(val) else 0.0
    except:
        return 0.0

print("📤 Updating match records...")

with engine.begin() as conn:
    for _, row in df.iterrows():
        matchid = row["matchid"]
        d = row["date"]
        surface = row["surface"]
        w = row["winner_name"]
        l = row["loser_name"]

        w_stats = calc_features(w, d)
        l_stats = calc_features(l, d)

        conn.execute(text(f"""
            UPDATE {TABLE_NAME}
            SET f_w_total_holds = :f_w_total_holds,
                f_w_total_breaks = :f_w_total_breaks,
                f_w_hold_pct = :f_w_hold_pct,
                f_w_break_pct = :f_w_break_pct,
                f_w_bp_conversion_pct = :f_w_bp_conversion_pct,
                f_l_total_holds = :f_l_total_holds,
                f_l_total_breaks = :f_l_total_breaks,
                f_l_hold_pct = :f_l_hold_pct,
                f_l_break_pct = :f_l_break_pct,
                f_l_bp_conversion_pct = :f_l_bp_conversion_pct
            WHERE matchid = :matchid
        """), {
            "matchid": int(matchid),
            "f_w_total_holds": safe_int(w_stats[0]),
            "f_w_total_breaks": safe_int(w_stats[1]),
            "f_w_hold_pct": safe_float(w_stats[2]),
            "f_w_break_pct": safe_float(w_stats[3]),
            "f_w_bp_conversion_pct": safe_float(w_stats[4]),
            "f_l_total_holds": safe_int(l_stats[0]),
            "f_l_total_breaks": safe_int(l_stats[1]),
            "f_l_hold_pct": safe_float(l_stats[2]),
            "f_l_break_pct": safe_float(l_stats[3]),
            "f_l_bp_conversion_pct": safe_float(l_stats[4]),
        })

        update_player(w, d, surface,
                      row["w_svgms"], row["w_bpfaced"], row["w_bpsaved"],
                      row["l_bpfaced"], row["l_bpsaved"])
        update_player(l, d, surface,
                      row["l_svgms"], row["l_bpfaced"], row["l_bpsaved"],
                      row["w_bpfaced"], row["w_bpsaved"])

print("✅ Done.")
