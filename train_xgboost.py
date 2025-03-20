import pandas as pd
import numpy as np
import optuna
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.calibration import CalibratedClassifierCV
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

# Updated features list
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

# Basic data validation
if df.empty:
    raise ValueError("No data loaded from database.")
if not all(col in df.columns for col in ["date", "target", "avgw", "avgl"] + features):
    raise ValueError("Missing required columns in dataset.")

# -------------------------
# PREPROCESSING
# -------------------------
X = df[features]
y = df["target"]

# One-hot encode surface
X = pd.get_dummies(X, columns=["surface"], drop_first=False)

# Temporal split: pre-2024 train, 2024 test
train_mask = pd.to_datetime(df["date"]) < "2024-01-01"
X_train = X[train_mask]
y_train = y[train_mask]
X_test = X[~train_mask]
y_test = y[~train_mask]
test_df = df[~train_mask].copy()  # For odds comparison

if X_train.empty or X_test.empty:
    raise ValueError("Train or test set is empty after temporal split.")

# Scale numeric features
scaler = StandardScaler()
numeric_cols = X.columns.difference([col for col in X.columns if "surface" in col])
X_train_scaled = X_train.copy()
X_test_scaled = X_test.copy()
X_train_scaled[numeric_cols] = scaler.fit_transform(X_train[numeric_cols])
X_test_scaled[numeric_cols] = scaler.transform(X_test[numeric_cols])

# -------------------------
# OPTUNA HYPERPARAMETER TUNING
# -------------------------
def objective(trial):
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 2000, step=100),
        "learning_rate": trial.suggest_loguniform("learning_rate", 0.005, 0.3),
        "max_depth": trial.suggest_int("max_depth", 3, 15),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "lambda": trial.suggest_loguniform("lambda", 1e-3, 10.0),
        "alpha": trial.suggest_loguniform("alpha", 1e-3, 10.0),
    }

    model = XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        use_label_encoder=False,
        **params
    )
    
    model.fit(
        X_train_scaled, 
        y_train,
        eval_set=[(X_test_scaled, y_test)],
        verbose=False
    )
    
    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
    return roc_auc_score(y_test, y_pred_proba)

print("Starting Optuna hyperparameter tuning...")
study = optuna.create_study(direction="maximize")
study.optimize(objective, n_trials=100, timeout=1200)

best_params = study.best_params
print("\nBest Hyperparameters:", best_params)

# -------------------------
# TRAIN BEST XGBoost MODEL
# -------------------------
print("\nTraining XGBoost with best hyperparameters...")
best_model = XGBClassifier(
    objective="binary:logistic",
    eval_metric="logloss",
    use_label_encoder=False,
    **best_params
)
best_model.fit(X_train_scaled, y_train)

print("\nCalibrating probabilities with isotonic regression...")
calibrated_model = CalibratedClassifierCV(best_model, method="isotonic", cv=5)
calibrated_model.fit(X_train_scaled, y_train)

# Predict probabilities on test set
y_pred_proba = calibrated_model.predict_proba(X_test_scaled)[:, 1]
y_pred = (y_pred_proba > 0.5).astype(int)

# -------------------------
# EVALUATE MODEL
# -------------------------
accuracy = accuracy_score(y_test, y_pred)
logloss = log_loss(y_test, y_pred_proba)
roc_auc = roc_auc_score(y_test, y_pred_proba)

print(f"\nFinal Model Evaluation:")
print(f"Test Accuracy: {accuracy:.4f}")
print(f"Test Log Loss: {logloss:.4f}")
print(f"Test ROC-AUC: {roc_auc:.4f}")

# Feature Importance
importances = pd.DataFrame({
    "Feature": X_train_scaled.columns,
    "Importance": best_model.feature_importances_
}).sort_values("Importance", ascending=False)
print("\nFeature Importance (from uncalibrated model):")
print(importances)

# -------------------------
# COMPARE TO ODDS
# -------------------------
test_df["model_prob_p1"] = y_pred_proba
test_df["model_prob_p2"] = 1 - y_pred_proba
test_df["implied_prob_p1"] = np.where(test_df["target"] == 1, 1/test_df["avgw"], 1/test_df["avgl"])
test_df["implied_prob_p2"] = np.where(test_df["target"] == 1, 1/test_df["avgl"], 1/test_df["avgw"])
test_df["prob_diff_p1"] = test_df["model_prob_p1"] - test_df["implied_prob_p1"]

threshold = 0.1
mispriced = test_df[test_df["prob_diff_p1"].abs() > threshold]
print(f"\nDetected {len(mispriced)} mispriced matches (threshold: {threshold}):")
print(mispriced[["matchid", "player1_name", "player2_name", "model_prob_p1", "implied_prob_p1", "prob_diff_p1"]].head())

# -------------------------
# SAVE RESULTS
# -------------------------
importances.to_csv("feature_importance.csv", index=False)
mispriced.to_csv("mispriced_matches.csv", index=False)
print("\n✅ Saved feature importance and mispriced matches.")