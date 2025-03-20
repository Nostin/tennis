import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.calibration import CalibratedClassifierCV
import optuna
import pymc as pm
import arviz as az
from sklearn.preprocessing import StandardScaler

# Database connection
engine = create_engine("postgresql://seanthompson:@localhost:5432/tennis")

# Load data
df = pd.read_sql("SELECT * FROM model_data_feed WHERE date < '2024-01-01'", engine)
test_df = pd.read_sql("SELECT * FROM model_data_feed WHERE date >= '2024-01-01'", engine)

# Features
features = [
    "elo_diff", "surface_elo_diff", "tournament_fatigue_diff", "h2h_wins_diff",
    "win_pct_3m_diff", "dominance_roll_diff", "recent_matches_30d_diff", 
    "days_since_last_diff", "tournament_strength",
    "ace_rate_3m_diff", "ace_rate_6m_diff", "df_rate_3m_diff", "df_rate_6m_diff",
    "bpsaved_rate_3m_diff", "bpsaved_rate_6m_diff", "bpfaced_rate_3m_diff", "bpfaced_rate_6m_diff",
    "first_serve_pct_3m_diff", "first_serve_win_pct_3m_surface_diff", "second_serve_win_pct_3m_surface_diff",
    "recent_form_6matches_diff", "avg_elo_faced_diff", "elo_first_serve_interaction",
    "first_serve_win_pct_3m_diff", "second_serve_win_pct_3m_diff", "odds_movement"
]

# Feature Engineering - Training Data
df["momentum"] = df["win_pct_3m_diff"] * 0.6 + df["win_pct_6m_diff"] * 0.4
df["p1_closing_odds"] = np.where(df["target"] == 1, df["avgw"], df["avgl"])
df["p2_closing_odds"] = np.where(df["target"] == 1, df["avgl"], df["avgw"])
df["odds_movement"] = df["p1_closing_odds"] - df["p2_closing_odds"]

# Feature Engineering - Test Data (Ensure p1_closing_odds & p2_closing_odds exist)
test_df["momentum"] = test_df["win_pct_3m_diff"] * 0.6 + test_df["win_pct_6m_diff"] * 0.4
test_df["p1_closing_odds"] = np.where(test_df["target"] == 1, test_df["avgw"], test_df["avgl"])
test_df["p2_closing_odds"] = np.where(test_df["target"] == 1, test_df["avgl"], test_df["avgw"])
test_df["odds_movement"] = test_df["p1_closing_odds"] - test_df["p2_closing_odds"]


# Prepare Data
X = df[features]
y = df["target"]
X_test = test_df[features]
y_test = test_df["target"]

# Scale features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
X_test_scaled = scaler.transform(X_test)

# XGBoost Optimization
def objective(trial):
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 800, 1500),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.03),
        "max_depth": trial.suggest_int("max_depth", 4, 7),
        "subsample": trial.suggest_float("subsample", 0.7, 0.9),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 0.8),
        "lambda": trial.suggest_float("lambda", 0.01, 0.2),
        "alpha": trial.suggest_float("alpha", 1.0, 5.0),
        "random_state": 42
    }
    model = xgb.XGBClassifier(**params)
    model.fit(X_scaled, y)
    preds = model.predict_proba(X_test_scaled)[:, 1]
    return log_loss(y_test, preds)

study = optuna.create_study(direction="minimize")
study.optimize(objective, n_trials=50)
print("Best Hyperparameters:", study.best_params)

# Train XGBoost
xgb_model = xgb.XGBClassifier(**study.best_params)
xgb_model.fit(X_scaled, y)
xgb_probs = xgb_model.predict_proba(X_test_scaled)[:, 1]

# Bayesian Neural Network
with pm.Model() as bayes_model:
    weights = pm.Normal("weights", mu=0, sigma=1, shape=X_scaled.shape[1])
    bias = pm.Normal("bias", mu=0, sigma=1)
    logits = pm.math.dot(X_scaled, weights) + bias
    pm.Bernoulli("y", logit_p=pm.math.sigmoid(logits), observed=y)
    trace = pm.sample(2000, tune=1000, return_inferencedata=False)

logits_test = np.dot(X_test_scaled, trace["weights"].mean(axis=0)) + trace["bias"].mean()
bayes_probs = 1 / (1 + np.exp(-logits_test))

# Ensemble Model
ensemble_probs = (xgb_probs + bayes_probs) / 2

# Platt Scaling Calibration
calibrator = CalibratedClassifierCV(xgb_model, method="sigmoid", cv="prefit")
calibrator.fit(X_scaled, y)
calibrated_probs = calibrator.predict_proba(X_test_scaled)[:, 1]

# Evaluation
print("Final Model Evaluation:")
print(f"Test Accuracy: {accuracy_score(y_test, ensemble_probs > 0.5):.4f}")
print(f"Test Log Loss: {log_loss(y_test, ensemble_probs):.4f}")
print(f"Test ROC-AUC: {roc_auc_score(y_test, ensemble_probs):.4f}")

# Feature Importance
feature_importance = pd.DataFrame({
    "Feature": features,
    "Importance": xgb_model.feature_importances_
}).sort_values("Importance", ascending=False)
print("\nFeature Importance (from XGBoost):")
print(feature_importance)

# Detect Mispriced Matches
test_df["model_prob_p1"] = ensemble_probs
test_df["implied_prob_p1"] = 1 / test_df["avgw"]
test_df["prob_diff_p1"] = test_df["model_prob_p1"] - test_df["implied_prob_p1"]
mispriced = test_df[abs(test_df["prob_diff_p1"]) > 0.1][["matchid", "player1_name", "player2_name", "model_prob_p1", "implied_prob_p1", "prob_diff_p1"]]
print(f"\nDetected {len(mispriced)} mispriced matches (threshold: 0.1):")
print(mispriced.head())
