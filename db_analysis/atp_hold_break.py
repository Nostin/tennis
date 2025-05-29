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

# Store match history per player
player_stats = {}

def calculate_holds(svgms, bpfaced, bpsaved):
    if pd.isna(svgms) or pd.isna(bpfaced) or pd.isna(bpsaved):
        return 0
    return int(svgms - (bpfaced - bpsaved))

def calculate_breaks(opp_bpfaced, opp_bpsaved):
    if pd.isna(opp_bpfaced) or pd.isna(opp_bpsaved):
        return 0
    return int(opp_bpfaced - opp_bpsaved)

def update_player(player, date, surface, svgms, bpfaced, bpsaved, opp_bpfaced, opp_bpsaved):
    """
    Update a player's stats with a new match.
    Note: This should only be called AFTER the database has been updated with the current match's features.
    """
    if player not in player_stats:
        player_stats[player] = {
            "all": {"holds": 0, "breaks": 0, "svgms": 0, "opp_bpfaced": 0},
            "30d": {"holds": 0, "breaks": 0, "svgms": 0, "opp_bpfaced": 0},
            "hard": {"holds": 0, "breaks": 0, "svgms": 0, "opp_bpfaced": 0},
            "clay": {"holds": 0, "breaks": 0, "svgms": 0, "opp_bpfaced": 0},
            "grass": {"holds": 0, "breaks": 0, "svgms": 0, "opp_bpfaced": 0}
        }
    
    # Calculate holds and breaks for this match
    holds = calculate_holds(svgms, bpfaced, bpsaved)
    breaks = calculate_breaks(opp_bpfaced, opp_bpsaved)
    
    # Update all-time stats
    player_stats[player]["all"]["holds"] += holds
    player_stats[player]["all"]["breaks"] += breaks
    player_stats[player]["all"]["svgms"] += svgms if pd.notna(svgms) else 0
    player_stats[player]["all"]["opp_bpfaced"] += opp_bpfaced if pd.notna(opp_bpfaced) else 0
    
    # Update surface-specific stats
    surface_key = surface.lower()
    if surface_key in ["hard", "clay", "grass"]:
        player_stats[player][surface_key]["holds"] += holds
        player_stats[player][surface_key]["breaks"] += breaks
        player_stats[player][surface_key]["svgms"] += svgms if pd.notna(svgms) else 0
        player_stats[player][surface_key]["opp_bpfaced"] += opp_bpfaced if pd.notna(opp_bpfaced) else 0
    
    # Update 30-day stats
    thirty_days_ago = date - pd.Timedelta(days=30)
    if date >= thirty_days_ago:
        player_stats[player]["30d"]["holds"] += holds
        player_stats[player]["30d"]["breaks"] += breaks
        player_stats[player]["30d"]["svgms"] += svgms if pd.notna(svgms) else 0
        player_stats[player]["30d"]["opp_bpfaced"] += opp_bpfaced if pd.notna(opp_bpfaced) else 0

def calc_stats(stats):
    if stats["svgms"] == 0:
        return {
            "total_holds": 0,
            "total_breaks": 0,
            "hold_pct": 0.0,
            "break_pct": 0.0,
            "bp_conversion_pct": 0.0
        }
    
    hold_pct = round((stats["holds"] / stats["svgms"]) * 100, 2) if stats["svgms"] else 0.0
    break_pct = round((stats["breaks"] / (stats["opp_bpfaced"] / 6)) * 100, 2) if stats["opp_bpfaced"] else 0.0
    bp_conversion_pct = round((stats["breaks"] / stats["opp_bpfaced"]) * 100, 2) if stats["opp_bpfaced"] else 0.0
    
    return {
        "total_holds": stats["holds"],
        "total_breaks": stats["breaks"],
        "hold_pct": hold_pct,
        "break_pct": break_pct,
        "bp_conversion_pct": bp_conversion_pct
    }

print("🛠️ Updating rows...")

with engine.begin() as conn:
    for idx, row in df.iterrows():
        matchid = row["matchid"]
        date = row["date"]
        surface = row["surface"]
        w = row["winner_name"]
        l = row["loser_name"]
        
        # Get current stats for both players (only includes previous matches)
        w_stats = {k: calc_stats(player_stats.get(w, {}).get(k, {"holds": 0, "breaks": 0, "svgms": 0, "opp_bpfaced": 0})) for k in ["all", "30d", "hard", "clay", "grass"]}
        l_stats = {k: calc_stats(player_stats.get(l, {}).get(k, {"holds": 0, "breaks": 0, "svgms": 0, "opp_bpfaced": 0})) for k in ["all", "30d", "hard", "clay", "grass"]}
        
        # Update database with current stats (before updating player stats)
        conn.execute(text(f"""
            UPDATE {TABLE_NAME}
            SET
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
            "matchid": matchid,
            # all
            **{f"f_w_{k}": w_stats["all"][k] for k in ["total_holds", "total_breaks", "hold_pct", "break_pct", "bp_conversion_pct"]},
            **{f"f_l_{k}": l_stats["all"][k] for k in ["total_holds", "total_breaks", "hold_pct", "break_pct", "bp_conversion_pct"]},
            # 30d
            **{f"f_w_{k}_30d": w_stats["30d"][k] for k in ["total_holds", "total_breaks", "hold_pct", "break_pct", "bp_conversion_pct"]},
            **{f"f_l_{k}_30d": l_stats["30d"][k] for k in ["total_holds", "total_breaks", "hold_pct", "break_pct", "bp_conversion_pct"]},
            # hard
            **{f"f_w_{k}_hard": w_stats["hard"][k] for k in ["total_holds", "total_breaks", "hold_pct", "break_pct", "bp_conversion_pct"]},
            **{f"f_l_{k}_hard": l_stats["hard"][k] for k in ["total_holds", "total_breaks", "hold_pct", "break_pct", "bp_conversion_pct"]},
            # clay
            **{f"f_w_{k}_clay": w_stats["clay"][k] for k in ["total_holds", "total_breaks", "hold_pct", "break_pct", "bp_conversion_pct"]},
            **{f"f_l_{k}_clay": l_stats["clay"][k] for k in ["total_holds", "total_breaks", "hold_pct", "break_pct", "bp_conversion_pct"]},
            # grass
            **{f"f_w_{k}_grass": w_stats["grass"][k] for k in ["total_holds", "total_breaks", "hold_pct", "break_pct", "bp_conversion_pct"]},
            **{f"f_l_{k}_grass": l_stats["grass"][k] for k in ["total_holds", "total_breaks", "hold_pct", "break_pct", "bp_conversion_pct"]},
        })
        
        # Only after updating the database, update player stats for future matches
        update_player(w, date, surface, row["w_svgms"], row["w_bpfaced"], row["w_bpsaved"], row["l_bpfaced"], row["l_bpsaved"])
        update_player(l, date, surface, row["l_svgms"], row["l_bpfaced"], row["l_bpsaved"], row["w_bpfaced"], row["w_bpsaved"])

print("✅ Done.")
