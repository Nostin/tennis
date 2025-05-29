# Training features list
use_odds = True
PARAMS_FILE = "best_xgboost_odds_params.json"

features_no_odds = [
    "elo_diff", "surface", "surface_elo_diff", "avg_elo_faced_diff", "avg_surface_elo_faced_diff",
    "glicko_diff", "glicko_surface_diff", "p1_overall_rd", "p2_overall_rd", "p1_surface_rd", "p2_surface_rd",
    "tournament_fatigue_diff",
    "win_pct_last_30d_diff", "recent_matches_30d_diff", "tournament_strength",
    "hold_pct_diff", "hold_surface_pct_diff", "break_pct_diff", "break_surface_pct_diff",
    "break_point_conversion_diff", "break_point_surface_conversion_diff",
    "tiebreak_rate_diff", "tiebreak_win_diff", "tiebreak_surface_rate_diff",
    "tiebreak_surface_win_diff", "home_adv_diff",
    "h2h_wins_diff", "h2h_surface_wins_diff",
]

features_odds = [
    "elo_diff", "surface", "surface_elo_diff", "avg_elo_faced_diff", "avg_surface_elo_faced_diff",
    "glicko_diff", "glicko_surface_diff", "p1_overall_rd", "p2_overall_rd", "p1_surface_rd", "p2_surface_rd",
    "tournament_fatigue_diff",
    "win_pct_last_30d_diff", "recent_matches_30d_diff", "tournament_strength",
    "hold_pct_diff", "hold_surface_pct_diff", "break_pct_diff", "break_surface_pct_diff",
    "break_point_conversion_diff", "break_point_surface_conversion_diff",
    "tiebreak_rate_diff", "tiebreak_win_diff", "tiebreak_surface_rate_diff",
    "tiebreak_surface_win_diff", "home_adv_diff",
    "h2h_wins_diff", "h2h_surface_wins_diff",
    "p1_odds_shape_flat", "p2_odds_shape_flat", "p1_odds_shape_inv_u", "p2_odds_shape_inv_u",
    "p1_odds_shape_mixed", "p2_odds_shape_mixed",
    "p1_odds_shape_strength", "p2_odds_shape_strength", "p1_odds_shortened_fav", "p2_odds_shortened_fav",
    "p1_odds_late_money", "p2_odds_late_money",
    "p1_was_fav", "p1_was_fav_closing", "p1_became_fav", "p1_lost_fav", "p1_stayed_fav",
    "p2_was_fav", "p2_was_fav_closing", "p2_became_fav", "p2_lost_fav", "p2_stayed_fav",
    "odds_volatility_diff", "odds_trend_slope_diff", "odds_max_swing_diff", "odds_early_move_diff", "odds_late_move_diff", "odds_vol_ratio_diff", "odds_net_move_diff", "odds_curvature_diff"
    # "p1_odds_shape_u", "p2_odds_shape_u", "p1_odds_shape_drift", "p2_odds_shape_drift", "p1_odds_shape_short", "p2_odds_shape_short", "p1_odds_shape_unknown", "p2_odds_shape_unknown",
    # "deltap_1_diff", "deltap_2_diff", "deltap_3_diff", "deltap_4_diff", "deltap_5_diff", "deltap_6_diff", "deltap_7_diff", "deltap_8_diff", "deltap_9_diff", "deltap_10_diff"
]

if use_odds:
    features = features_odds
else:
    PARAMS_FILE = "best_xgboost_params.json"
    features = features_no_odds