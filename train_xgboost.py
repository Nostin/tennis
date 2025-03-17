import pandas as pd
import numpy as np
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

# -------------------------
# LOAD DATA
# -------------------------
print("Loading data from database...")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
with engine.connect() as conn:
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} ORDER BY date", conn)

# -------------------------
# PREPROCESSING
# -------------------------
# Define features (exclude ALL odds-related and potentially leaky match-specific stats)
features = [
    "elo_diff", "surface_elo_diff", "fatigue_diff", "h2h_wins_diff",
    "win_pct_3m_diff", "dominance_roll_diff", "recent_matches_30d_diff", 
    "days_since_last_diff", "tournament_strength", "surface"
]
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
test_df = df[~train_mask].copy()

# Scale numeric features
scaler = StandardScaler()
numeric_cols = X.columns.difference([col for col in X.columns if "surface" in col])
X_train_scaled = X_train.copy()
X_test_scaled = X_test.copy()
X_train_scaled[numeric_cols] = scaler.fit_transform(X_train[numeric_cols])
X_test_scaled[numeric_cols] = scaler.transform(X_test[numeric_cols])

# -------------------------
# TRAIN AND CALIBRATE XGBoost
# -------------------------
print("Training XGBoost model...")
base_model = XGBClassifier(
    objective="binary:logistic",
    eval_metric="logloss",
    use_label_encoder=False,
    n_estimators=200,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42
)
base_model.fit(X_train_scaled, y_train)

print("Calibrating probabilities with Platt scaling...")
calibrated_model = CalibratedClassifierCV(base_model, method="sigmoid", cv=5)
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
print(f"Test Accuracy: {accuracy:.4f}")
print(f"Test Log Loss: {logloss:.4f}")
print(f"Test ROC-AUC: {roc_auc:.4f}")

# Feature importance
importances = pd.DataFrame({
    "Feature": X_train_scaled.columns,
    "Importance": base_model.feature_importances_
}).sort_values("Importance", ascending=False)
print("\nFeature Importance:")
print(importances)

# -------------------------
# COMPARE TO BETTING ODDS
# -------------------------
# Reconstruct odds from odds_diff
test_df["avgw"] = test_df["odds_diff"].apply(lambda x: max(1.5 + x/2, 1.01) if x >= 0 else max(1.5 - x/2, 1.01))
test_df["avgl"] = test_df["odds_diff"].apply(lambda x: max(1.5 - x/2, 1.01) if x >= 0 else max(1.5 + x/2, 1.01))

# Compute implied probabilities with overround normalization
test_df["implied_prob_p1_raw"] = 1 / test_df["avgw"]
test_df["implied_prob_p2_raw"] = 1 / test_df["avgl"]
overround = test_df["implied_prob_p1_raw"] + test_df["implied_prob_p2_raw"]
test_df["implied_prob_p1"] = test_df["implied_prob_p1_raw"] / overround
test_df["implied_prob_p2"] = test_df["implied_prob_p2_raw"] / overround

# Add model probabilities
test_df["model_prob_p1"] = y_pred_proba
test_df["model_prob_p2"] = 1 - y_pred_proba

# Detect mispricing
test_df["prob_diff_p1"] = test_df["model_prob_p1"] - test_df["implied_prob_p1"]
test_df["prob_diff_p2"] = test_df["model_prob_p2"] - test_df["implied_prob_p2"]

# -------------------------
# TUNE THRESHOLD AND SIMULATE BETTING PERFORMANCE
# -------------------------
print("\nSimulating betting performance across multiple thresholds...")
thresholds = [0.05, 0.08, 0.1, 0.12, 0.15]
results = []

for threshold in thresholds:
    # Define bets
    test_df["bet_on_p1"] = test_df["prob_diff_p1"] > threshold
    test_df["bet_on_p2"] = test_df["prob_diff_p2"] > threshold
    
    # Calculate bet outcomes
    test_df["bet_outcome"] = np.where(test_df["bet_on_p1"] & (test_df["target"] == 1), test_df["avgw"] - 1,
                                      np.where(test_df["bet_on_p1"] & (test_df["target"] == 0), -1,
                                               np.where(test_df["bet_on_p2"] & (test_df["target"] == 0), test_df["avgl"] - 1,
                                                        np.where(test_df["bet_on_p2"] & (test_df["target"] == 1), -1, 0))))
    
    # Compute metrics
    num_bets_p1 = test_df["bet_on_p1"].sum()
    num_bets_p2 = test_df["bet_on_p2"].sum()
    total_bets = test_df[["bet_on_p1", "bet_on_p2"]].any(axis=1).sum()
    total_profit = test_df["bet_outcome"].sum()
    roi = total_profit / total_bets if total_bets > 0 else 0
    
    # Mispriced matches for this threshold
    mispriced = test_df[
        (test_df["prob_diff_p1"].abs() > threshold) | 
        (test_df["prob_diff_p2"].abs() > threshold)
    ]
    
    results.append({
        "Threshold": threshold,
        "Total Bets": total_bets,
        "Bets on P1": num_bets_p1,
        "Bets on P2": num_bets_p2,
        "ROI": roi,
        "Mispriced Matches": len(mispriced)
    })
    
    # Print detailed results for threshold 0.1 (original)
    if threshold == 0.1:
        print(f"\nDetailed results for threshold {threshold}:")
        print(f"Detected {len(mispriced)} mispriced matches:")
        print(mispriced[["matchid", "player1_name", "player2_name", "model_prob_p1", 
                         "implied_prob_p1", "prob_diff_p1", "target"]].head())

# Display threshold tuning results
results_df = pd.DataFrame(results)
print("\nThreshold Tuning Results:")
print(results_df)

# Select best threshold based on ROI
best_threshold = results_df.loc[results_df["ROI"].idxmax()]
print(f"\nBest Threshold: {best_threshold['Threshold']}")
print(f"Total Bets: {best_threshold['Total Bets']} (P1: {best_threshold['Bets on P1']}, P2: {best_threshold['Bets on P2']})")
print(f"Best ROI: {best_threshold['ROI']:.4f}")

# -------------------------
# SAVE RESULTS
# -------------------------
test_df.to_csv("model_predictions.csv", index=False)
mispriced.to_csv("mispriced_matches.csv", index=False)  # Saves the last threshold's mispriced matches
print("Saved full predictions to 'model_predictions.csv' and mispriced matches to 'mispriced_matches.csv'")