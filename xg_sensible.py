import json
import pandas as pd
import numpy as np
import optuna
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from xgboost import XGBClassifier
import warnings

warnings.filterwarnings("ignore")

# -------------------------
# CONFIGURATION
# -------------------------
DB_NAME = "tennis"
DB_USER = "seanthompson"
DB_PASS = ""
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "model_data_feed"
PARAMS_FILE = "best_xgboost_params.json"

# Updated feature list
features = [
    "elo_diff", "surface", "surface_elo_diff", "tournament_fatigue_diff", "h2h_wins_diff",
    "win_pct_3m_diff", "dominance_roll_diff", "recent_matches_30d_diff",
    "days_since_last_diff", "tournament_strength",
    "ace_rate_3m_diff", "ace_rate_6m_diff", "df_rate_3m_diff", "df_rate_6m_diff",
    "bpsaved_rate_3m_diff", "bpsaved_rate_6m_diff", "bpfaced_rate_3m_diff", "bpfaced_rate_6m_diff",
    "first_serve_pct_3m_diff",
    "first_serve_win_pct_3m_surface_diff", "second_serve_win_pct_3m_surface_diff",
    "recent_form_6matches_diff", "avg_elo_faced_diff", "elo_first_serve_interaction",
    "first_serve_win_pct_3m_diff", "second_serve_win_pct_3m_diff"
]

# -------------------------
# LOAD DATA
# -------------------------
print("Loading data from database...")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
with engine.connect() as conn:
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} ORDER BY date ASC", conn)

# Ensure required columns exist
required_cols = ["date", "target"] + features
if not all(col in df.columns for col in required_cols):
    raise ValueError("Missing required columns in dataset.")

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

# Prepare feature matrix and target
X = df[features]
y = df["target"]

# One-hot encode surface
X = pd.get_dummies(X, columns=["surface"], drop_first=False)

# -------------------------
# LOAD OR OPTIMIZE HYPERPARAMETERS
# -------------------------
def load_or_tune_xgboost(X_train, y_train, X_val, y_val):
    """Load pre-tuned XGBoost parameters or run Optuna if not available."""
    try:
        with open(PARAMS_FILE, "r") as f:
            best_params = json.load(f)
        print("\n✅ Loaded best XGBoost parameters from file.")
    except FileNotFoundError:
        print("\n⚠️ No saved hyperparameters found. Running Optuna first...")

        def objective(trial):
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 100, 2000, step=100),
                "learning_rate": trial.suggest_float("learning_rate", 0.005, 0.3, log=True),
                "max_depth": trial.suggest_int("max_depth", 3, 15),
                "subsample": trial.suggest_float("subsample", 0.5, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
                "lambda": trial.suggest_float("lambda", 1e-3, 10.0, log=True),
                "alpha": trial.suggest_float("alpha", 1e-3, 10.0, log=True),
            }

            model = XGBClassifier(objective="binary:logistic", eval_metric="logloss", **params)
            model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
            y_pred_proba = model.predict_proba(X_val)[:, 1]
            return log_loss(y_val, y_pred_proba)

        print("\nRunning Optuna hyperparameter tuning... (This may take a while)")
        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=50, timeout=1800)

        best_params = study.best_params
        with open(PARAMS_FILE, "w") as f:
            json.dump(best_params, f, indent=4)

        print("\n✅ Best hyperparameters saved to 'best_xgboost_params.json'")

    return best_params


# -------------------------
# WEEK-BY-WEEK RETRAINING
# -------------------------

# Train on 2015 - end 2023
train_mask = df["date"] < "2024-01-01"
X_train, y_train = X[train_mask], y[train_mask]

# Load best parameters for XGBoost
best_xgb_params = load_or_tune_xgboost(X_train, y_train, X_train, y_train)

# Initialize model
model = XGBClassifier(objective="binary:logistic", eval_metric="logloss", **best_xgb_params)
model.fit(X_train, y_train)

# Track total performance
total_y_true = []
total_y_pred_proba = []

# Iterate over 2024 week by week
weeks = sorted(df[df["date"] >= "2024-01-01"]["date"].dt.to_period("W").unique())

print("\n📅 Running week-by-week prediction and retraining...")
for week in weeks:
    print(f"\n📆 Predicting matches for week: {week}")

    # Get matches for this week
    week_mask = df["date"].dt.to_period("W") == week
    X_test, y_test = X[week_mask], y[week_mask]

    if X_test.empty:
        continue

    # Predict probabilities
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_pred_proba > 0.5).astype(int)

    # Store predictions for final evaluation
    total_y_true.extend(y_test)
    total_y_pred_proba.extend(y_pred_proba)

    # Evaluate this week's predictions
    accuracy = accuracy_score(y_test, y_pred)
    log_loss_score = log_loss(y_test, y_pred_proba)
    roc_auc = roc_auc_score(y_test, y_pred_proba)

    print(f"  Accuracy: {accuracy:.4f}, Log Loss: {log_loss_score:.4f}, ROC-AUC: {roc_auc:.4f}")

    # Add this week's data to training and retrain
    X_train = pd.concat([X_train, X_test])
    y_train = pd.concat([y_train, y_test])

    model.fit(X_train, y_train)

# -------------------------
# FINAL MODEL EVALUATION
# -------------------------

total_accuracy = accuracy_score(total_y_true, np.array(total_y_pred_proba) > 0.5)
total_log_loss = log_loss(total_y_true, total_y_pred_proba)
total_roc_auc = roc_auc_score(total_y_true, total_y_pred_proba)

print("\n✅ Final Total Model Performance:")
print(f"Total Accuracy: {total_accuracy:.4f}")
print(f"Total Log Loss: {total_log_loss:.4f}")
print(f"Total ROC-AUC Score: {total_roc_auc:.4f}")

# -------------------------
# FEATURE IMPORTANCE
# -------------------------
feature_importance = pd.DataFrame({
    "Feature": X.columns,
    "Importance": model.feature_importances_
}).sort_values("Importance", ascending=False)

print("\n📊 Feature Importance (Top 10 Features):")
print(feature_importance.head(10))

# Save feature importance to CSV for further analysis
feature_importance.to_csv("feature_importance.csv", index=False)
print("\n✅ Feature importance saved to 'feature_importance.csv'")
