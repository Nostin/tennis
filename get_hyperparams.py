import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_connect import get_engine

import json
import optuna
import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import log_loss
from sklearn.preprocessing import StandardScaler

# -------------------------
# CONFIGURATION
# -------------------------
TABLE_NAME = "xgboost_data_feed"
PARAMS_FILE = "best_xgboost_params.json"
SCALER = StandardScaler()

# Features list
features = [
    "elo_diff", "surface", "surface_elo_diff", "avg_elo_faced_diff", "avg_surface_elo_faced_diff",
    "glicko_diff", "glicko_surface_diff", "p1_overall_rd", "p2_overall_rd", "p1_surface_rd", "p2_surface_rd",
    "tournament_fatigue_diff", "h2h_wins_diff", "h2h_surface_wins_diff",
    "win_pct_last_30d_diff", "recent_matches_30d_diff", "tournament_strength",
    "hold_pct_diff", "hold_surface_pct_diff", "break_pct_diff", "break_surface_pct_diff",
    "break_point_conversion_diff", "break_point_surface_conversion_diff",
    "tiebreak_rate_diff", "tiebreak_win_diff", "tiebreak_surface_rate_diff",
    "tiebreak_surface_win_diff", "home_adv_diff"
]

# -------------------------
# LOAD DATA FROM DATABASE
# -------------------------
print("Loading data from database...")
engine = get_engine()
with engine.connect() as conn:
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} ORDER BY date ASC", conn)

# Ensure required columns exist
required_cols = ["date", "target"] + features
if not all(col in df.columns for col in required_cols):
    raise ValueError("Missing required columns in dataset.")

# Convert date to datetime and sort
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

# Prepare feature matrix and target
X = df[features]
y = df["target"]

# One-hot encode surface
X = pd.get_dummies(X, columns=["surface"], drop_first=False)

# Scale numeric features
numeric_cols = X.columns.difference([col for col in X.columns if "surface" in col])
X[numeric_cols] = SCALER.fit_transform(X[numeric_cols])

# Split into train (2015-2022) and validation (2023)
train_mask = df["date"] < "2023-01-01"
X_train, y_train = X[train_mask], y[train_mask]
X_val, y_val = X[~train_mask], y[~train_mask]

# -------------------------
# RUN OPTUNA HYPERPARAMETER TUNING
# -------------------------
def tune_xgboost(X_train, y_train, X_val, y_val):
    """Run Optuna hyperparameter tuning once and save the best parameters."""

    def objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 100, 2000, step=100),
            "learning_rate": trial.suggest_float("learning_rate", 0.005, 0.3, log=True),  # Fixed
            "max_depth": trial.suggest_int("max_depth", 3, 15),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "lambda": trial.suggest_float("lambda", 1e-3, 10.0, log=True),  # Fixed
            "alpha": trial.suggest_float("alpha", 1e-3, 10.0, log=True),  # Fixed
        }

        model = XGBClassifier(
            objective="binary:logistic",
            eval_metric="logloss",
            **params  # Removed deprecated 'use_label_encoder'
        )

        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
        y_pred_proba = model.predict_proba(X_val)[:, 1]
        return log_loss(y_val, y_pred_proba)

    print("\nRunning Optuna hyperparameter tuning... (This may take a while)")
    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=50, timeout=1800)  # 50 trials, 30-minute max

    best_params = study.best_params

    # Save the best parameters to a file
    with open(PARAMS_FILE, "w") as f:
        json.dump(best_params, f, indent=4)

    print("\n✅ Best hyperparameters saved to 'best_xgboost_params.json'")
    return best_params

# Run hyperparameter tuning
tune_xgboost(X_train, y_train, X_val, y_val)
