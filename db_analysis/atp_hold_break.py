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
        DROP COLUMN IF EXISTS f_l_bp_conversion_pct,
        DROP COLUMN IF EXISTS f_w_total_holds_30d,
        DROP COLUMN IF EXISTS f_w_total_breaks_30d,
        DROP COLUMN IF EXISTS f_w_hold_pct_30d,
        DROP COLUMN IF EXISTS f_w_break_pct_30d,
        DROP COLUMN IF EXISTS f_w_bp_conversion_pct_30d,
        DROP COLUMN IF EXISTS f_l_total_holds_30d,
        DROP COLUMN IF EXISTS f_l_total_breaks_30d,
        DROP COLUMN IF EXISTS f_l_hold_pct_30d,
        DROP COLUMN IF EXISTS f_l_break_pct_30d,
        DROP COLUMN IF EXISTS f_l_bp_conversion_pct_30d,
        DROP COLUMN IF EXISTS f_w_total_holds_hard,
        DROP COLUMN IF EXISTS f_w_total_breaks_hard,
        DROP COLUMN IF EXISTS f_w_hold_pct_hard,
        DROP COLUMN IF EXISTS f_w_break_pct_hard,
        DROP COLUMN IF EXISTS f_w_bp_conversion_pct_hard,
        DROP COLUMN IF EXISTS f_l_total_holds_hard,
        DROP COLUMN IF EXISTS f_l_total_breaks_hard,
        DROP COLUMN IF EXISTS f_l_hold_pct_hard,
        DROP COLUMN IF EXISTS f_l_break_pct_hard,
        DROP COLUMN IF EXISTS f_l_bp_conversion_pct_hard,
        DROP COLUMN IF EXISTS f_w_total_holds_clay,
        DROP COLUMN IF EXISTS f_w_total_breaks_clay,
        DROP COLUMN IF EXISTS f_w_hold_pct_clay,
        DROP COLUMN IF EXISTS f_w_break_pct_clay,
        DROP COLUMN IF EXISTS f_w_bp_conversion_pct_clay,
        DROP COLUMN IF EXISTS f_l_total_holds_clay,
        DROP COLUMN IF EXISTS f_l_total_breaks_clay,
        DROP COLUMN IF EXISTS f_l_hold_pct_clay,
        DROP COLUMN IF EXISTS f_l_break_pct_clay,
        DROP COLUMN IF EXISTS f_l_bp_conversion_pct_clay,
        DROP COLUMN IF EXISTS f_w_total_holds_grass,
        DROP COLUMN IF EXISTS f_w_total_breaks_grass,
        DROP COLUMN IF EXISTS f_w_hold_pct_grass,
        DROP COLUMN IF EXISTS f_w_break_pct_grass,
        DROP COLUMN IF EXISTS f_w_bp_conversion_pct_grass,
        DROP COLUMN IF EXISTS f_l_total_holds_grass,
        DROP COLUMN IF EXISTS f_l_total_breaks_grass,
        DROP COLUMN IF EXISTS f_l_hold_pct_grass,
        DROP COLUMN IF EXISTS f_l_break_pct_grass,
        DROP COLUMN IF EXISTS f_l_bp_conversion_pct_grass;
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
        ADD COLUMN IF NOT EXISTS f_l_bp_conversion_pct NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_total_holds_30d INT,
        ADD COLUMN IF NOT EXISTS f_w_total_breaks_30d INT,
        ADD COLUMN IF NOT EXISTS f_w_hold_pct_30d NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_break_pct_30d NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_bp_conversion_pct_30d NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_total_holds_30d INT,
        ADD COLUMN IF NOT EXISTS f_l_total_breaks_30d INT,
        ADD COLUMN IF NOT EXISTS f_l_hold_pct_30d NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_break_pct_30d NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_bp_conversion_pct_30d NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_total_holds_hard INT,
        ADD COLUMN IF NOT EXISTS f_w_total_breaks_hard INT,
        ADD COLUMN IF NOT EXISTS f_w_hold_pct_hard NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_break_pct_hard NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_bp_conversion_pct_hard NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_total_holds_hard INT,
        ADD COLUMN IF NOT EXISTS f_l_total_breaks_hard INT,
        ADD COLUMN IF NOT EXISTS f_l_hold_pct_hard NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_break_pct_hard NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_bp_conversion_pct_hard NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_total_holds_clay INT,
        ADD COLUMN IF NOT EXISTS f_w_total_breaks_clay INT,
        ADD COLUMN IF NOT EXISTS f_w_hold_pct_clay NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_break_pct_clay NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_bp_conversion_pct_clay NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_total_holds_clay INT,
        ADD COLUMN IF NOT EXISTS f_l_total_breaks_clay INT,
        ADD COLUMN IF NOT EXISTS f_l_hold_pct_clay NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_break_pct_clay NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_bp_conversion_pct_clay NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_total_holds_grass INT,
        ADD COLUMN IF NOT EXISTS f_w_total_breaks_grass INT,
        ADD COLUMN IF NOT EXISTS f_w_hold_pct_grass NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_break_pct_grass NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_w_bp_conversion_pct_grass NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_total_holds_grass INT,
        ADD COLUMN IF NOT EXISTS f_l_total_breaks_grass INT,
        ADD COLUMN IF NOT EXISTS f_l_hold_pct_grass NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_break_pct_grass NUMERIC(5, 2),
        ADD COLUMN IF NOT EXISTS f_l_bp_conversion_pct_grass NUMERIC(5, 2);
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

    def filter_records(condition):
        return [r for r in records if condition(r)]

    def compute_stats(filtered):
        svgms = sum(r.get("svgms", 0) or 0 for r in filtered)
        bpfaced = sum(r.get("bpfaced", 0) or 0 for r in filtered)
        bpsaved = sum(r.get("bpsaved", 0) or 0 for r in filtered)
        opp_bpfaced = sum(r.get("opp_bpfaced", 0) or 0 for r in filtered)
        opp_bpsaved = sum(r.get("opp_bpsaved", 0) or 0 for r in filtered)

        holds = svgms - bpfaced + bpsaved
        breaks = opp_bpfaced - opp_bpsaved

        holds = holds if pd.notna(holds) else 0
        breaks = breaks if pd.notna(breaks) else 0

        hold_pct = round((holds / svgms) * 100, 2) if svgms else 0
        break_pct = round((breaks / (opp_bpfaced / 6)) * 100, 2) if opp_bpfaced else 0
        bp_conv_pct = round((breaks / opp_bpfaced) * 100, 2) if opp_bpfaced else 0

        return {
            "holds": int(holds),
            "breaks": int(breaks),
            "hold_pct": hold_pct,
            "break_pct": break_pct,
            "bp_conversion_pct": bp_conv_pct
        }


    return {
        "all": compute_stats(records),
        "30d": compute_stats(filter_records(lambda r: (current_date - r["date"]).days <= 30)),
        "hard": compute_stats(filter_records(lambda r: r["surface"] == "Hard")),
        "clay": compute_stats(filter_records(lambda r: r["surface"] == "Clay")),
        "grass": compute_stats(filter_records(lambda r: r["surface"] == "Grass")),
    }


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
            SET
                -- Overall
                f_w_total_holds = :f_w_total_holds,
                f_w_total_breaks = :f_w_total_breaks,
                f_w_hold_pct = :f_w_hold_pct,
                f_w_break_pct = :f_w_break_pct,
                f_w_bp_conversion_pct = :f_w_bp_conversion_pct,
                f_l_total_holds = :f_l_total_holds,
                f_l_total_breaks = :f_l_total_breaks,
                f_l_hold_pct = :f_l_hold_pct,
                f_l_break_pct = :f_l_break_pct,
                f_l_bp_conversion_pct = :f_l_bp_conversion_pct,

                -- 30 Day
                f_w_total_holds_30d = :f_w_total_holds_30d,
                f_w_total_breaks_30d = :f_w_total_breaks_30d,
                f_w_hold_pct_30d = :f_w_hold_pct_30d,
                f_w_break_pct_30d = :f_w_break_pct_30d,
                f_w_bp_conversion_pct_30d = :f_w_bp_conversion_pct_30d,
                f_l_total_holds_30d = :f_l_total_holds_30d,
                f_l_total_breaks_30d = :f_l_total_breaks_30d,
                f_l_hold_pct_30d = :f_l_hold_pct_30d,
                f_l_break_pct_30d = :f_l_break_pct_30d,
                f_l_bp_conversion_pct_30d = :f_l_bp_conversion_pct_30d,

                -- Hard
                f_w_total_holds_hard = :f_w_total_holds_hard,
                f_w_total_breaks_hard = :f_w_total_breaks_hard,
                f_w_hold_pct_hard = :f_w_hold_pct_hard,
                f_w_break_pct_hard = :f_w_break_pct_hard,
                f_w_bp_conversion_pct_hard = :f_w_bp_conversion_pct_hard,
                f_l_total_holds_hard = :f_l_total_holds_hard,
                f_l_total_breaks_hard = :f_l_total_breaks_hard,
                f_l_hold_pct_hard = :f_l_hold_pct_hard,
                f_l_break_pct_hard = :f_l_break_pct_hard,
                f_l_bp_conversion_pct_hard = :f_l_bp_conversion_pct_hard,

                -- Clay
                f_w_total_holds_clay = :f_w_total_holds_clay,
                f_w_total_breaks_clay = :f_w_total_breaks_clay,
                f_w_hold_pct_clay = :f_w_hold_pct_clay,
                f_w_break_pct_clay = :f_w_break_pct_clay,
                f_w_bp_conversion_pct_clay = :f_w_bp_conversion_pct_clay,
                f_l_total_holds_clay = :f_l_total_holds_clay,
                f_l_total_breaks_clay = :f_l_total_breaks_clay,
                f_l_hold_pct_clay = :f_l_hold_pct_clay,
                f_l_break_pct_clay = :f_l_break_pct_clay,
                f_l_bp_conversion_pct_clay = :f_l_bp_conversion_pct_clay,

                -- Grass
                f_w_total_holds_grass = :f_w_total_holds_grass,
                f_w_total_breaks_grass = :f_w_total_breaks_grass,
                f_w_hold_pct_grass = :f_w_hold_pct_grass,
                f_w_break_pct_grass = :f_w_break_pct_grass,
                f_w_bp_conversion_pct_grass = :f_w_bp_conversion_pct_grass,
                f_l_total_holds_grass = :f_l_total_holds_grass,
                f_l_total_breaks_grass = :f_l_total_breaks_grass,
                f_l_hold_pct_grass = :f_l_hold_pct_grass,
                f_l_break_pct_grass = :f_l_break_pct_grass,
                f_l_bp_conversion_pct_grass = :f_l_bp_conversion_pct_grass
            WHERE matchid = :matchid
        """), {
            "matchid": int(matchid),
            # Overall
            "f_w_total_holds": safe_int(w_stats["all"]["holds"]),
            "f_w_total_breaks": safe_int(w_stats["all"]["breaks"]),
            "f_w_hold_pct": safe_float(w_stats["all"]["hold_pct"]),
            "f_w_break_pct": safe_float(w_stats["all"]["break_pct"]),
            "f_w_bp_conversion_pct": safe_float(w_stats["all"]["bp_conversion_pct"]),
            "f_l_total_holds": safe_int(l_stats["all"]["holds"]),
            "f_l_total_breaks": safe_int(l_stats["all"]["breaks"]),
            "f_l_hold_pct": safe_float(l_stats["all"]["hold_pct"]),
            "f_l_break_pct": safe_float(l_stats["all"]["break_pct"]),
            "f_l_bp_conversion_pct": safe_float(l_stats["all"]["bp_conversion_pct"]),
            # 30 Day
            "f_w_total_holds_30d": safe_int(w_stats["30d"]["holds"]),
            "f_w_total_breaks_30d": safe_int(w_stats["30d"]["breaks"]),
            "f_w_hold_pct_30d": safe_float(w_stats["30d"]["hold_pct"]),
            "f_w_break_pct_30d": safe_float(w_stats["30d"]["break_pct"]),
            "f_w_bp_conversion_pct_30d": safe_float(w_stats["30d"]["bp_conversion_pct"]),
            "f_l_total_holds_30d": safe_int(l_stats["30d"]["holds"]),
            "f_l_total_breaks_30d": safe_int(l_stats["30d"]["breaks"]),
            "f_l_hold_pct_30d": safe_float(l_stats["30d"]["hold_pct"]),
            "f_l_break_pct_30d": safe_float(l_stats["30d"]["break_pct"]),
            "f_l_bp_conversion_pct_30d": safe_float(l_stats["30d"]["bp_conversion_pct"]),
            # Hard
            "f_w_total_holds_hard": safe_int(w_stats["hard"]["holds"]),
            "f_w_total_breaks_hard": safe_int(w_stats["hard"]["breaks"]),
            "f_w_hold_pct_hard": safe_float(w_stats["hard"]["hold_pct"]),
            "f_w_break_pct_hard": safe_float(w_stats["hard"]["break_pct"]),
            "f_w_bp_conversion_pct_hard": safe_float(w_stats["hard"]["bp_conversion_pct"]),
            "f_l_total_holds_hard": safe_int(l_stats["hard"]["holds"]),
            "f_l_total_breaks_hard": safe_int(l_stats["hard"]["breaks"]),
            "f_l_hold_pct_hard": safe_float(l_stats["hard"]["hold_pct"]),
            "f_l_break_pct_hard": safe_float(l_stats["hard"]["break_pct"]),
            "f_l_bp_conversion_pct_hard": safe_float(l_stats["hard"]["bp_conversion_pct"]),
            # Clay
            "f_w_total_holds_clay": safe_int(w_stats["clay"]["holds"]),
            "f_w_total_breaks_clay": safe_int(w_stats["clay"]["breaks"]),
            "f_w_hold_pct_clay": safe_float(w_stats["clay"]["hold_pct"]),
            "f_w_break_pct_clay": safe_float(w_stats["clay"]["break_pct"]),
            "f_w_bp_conversion_pct_clay": safe_float(w_stats["clay"]["bp_conversion_pct"]),
            "f_l_total_holds_clay": safe_int(l_stats["clay"]["holds"]),
            "f_l_total_breaks_clay": safe_int(l_stats["clay"]["breaks"]),
            "f_l_hold_pct_clay": safe_float(l_stats["clay"]["hold_pct"]),
            "f_l_break_pct_clay": safe_float(l_stats["clay"]["break_pct"]),
            "f_l_bp_conversion_pct_clay": safe_float(l_stats["clay"]["bp_conversion_pct"]),
            # Grass
            "f_w_total_holds_grass": safe_int(w_stats["grass"]["holds"]),
            "f_w_total_breaks_grass": safe_int(w_stats["grass"]["breaks"]),
            "f_w_hold_pct_grass": safe_float(w_stats["grass"]["hold_pct"]),
            "f_w_break_pct_grass": safe_float(w_stats["grass"]["break_pct"]),
            "f_w_bp_conversion_pct_grass": safe_float(w_stats["grass"]["bp_conversion_pct"]),
            "f_l_total_holds_grass": safe_int(l_stats["grass"]["holds"]),
            "f_l_total_breaks_grass": safe_int(l_stats["grass"]["breaks"]),
            "f_l_hold_pct_grass": safe_float(l_stats["grass"]["hold_pct"]),
            "f_l_break_pct_grass": safe_float(l_stats["grass"]["break_pct"]),
            "f_l_bp_conversion_pct_grass": safe_float(l_stats["grass"]["bp_conversion_pct"]),
        })

        update_player(w, d, surface,
                      row["w_svgms"], row["w_bpfaced"], row["w_bpsaved"],
                      row["l_bpfaced"], row["l_bpsaved"])
        update_player(l, d, surface,
                      row["l_svgms"], row["l_bpfaced"], row["l_bpsaved"],
                      row["w_bpfaced"], row["w_bpsaved"])

print("✅ Done.")
