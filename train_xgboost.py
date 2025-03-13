import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, log_loss
from xgboost import XGBClassifier
import warnings
warnings.filterwarnings("ignore")  # Suppress warnings for cleaner output

# -------------------------
# CONFIGURATION
# -------------------------
DB_NAME = "tennis"
DB_USER = "seanthompson"
DB_PASS = ""  # Add password if necessary
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "model_data_feed"

# -------------------------
# LOAD DATA
# -------------------------
print("Loading data from database...")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
with engine.connect() as conn:
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)

# Add implied probabilities from odds for later comparison (not used in training)
df["avgw"] = df["odds_diff"].apply(lambda x: 1.5 + x/2 if x >= 0 else 1.5 - x/2)  # Rough reconstruction
df["avgl"] = df["odds_diff"].apply(lambda x: 1.5 - x/2 if x >= 0 else 1.5 + x/2)
df["implied_prob_w"] = 1 / df["avgw"]
df["implied_prob_l"] = 1 / df["avgl"]

# -------------------------
# PREPROCESSING
# -------------------------
# Define features (exclude odds_diff for training)
features = [
    "elo_diff", "surface_elo_diff", "fatigue_diff", "h2h_wins_diff",
    "win_pct_3m_diff", "dominance_roll_diff", "recent_matches_30d_diff", 
    "days_since_last_diff", "tournament_strength", "ace_pct_diff", 
    "df_pct_diff", "first_serve_pct_diff", "first_serve_win_pct_diff",
    "second_serve_win_pct_diff", "bp_saved_pct_diff", "surface"
]
X = df[features]
y = df["target"]

# One-hot encode surface
X = pd.get_dummies(X, columns=["surface"], drop_first=False)

# Split data (temporal split: pre-2024 train, 2024 test)
train_mask = pd.to_datetime(df["date"]) < "2024-01-01"
X_train = X[train_mask]
y_train = y[train_mask]
X_test = X[~train_mask]
y_test = y[~train_mask]
test_df = df[~train_mask].copy()  # Keep original data for odds comparison

# Scale numeric features
scaler = StandardScaler()
numeric_cols = X.columns.difference([col for col in X.columns if "surface" in col])
X_train_scaled = X_train.copy()
X_test_scaled = X_test.copy()
X_train_scaled[numeric_cols] = scaler.fit_transform(X_train[numeric_cols])
X_test_scaled[numeric_cols] = scaler.transform(X_test[numeric_cols])

# -------------------------
# TRAIN XGBoost
# -------------------------
print("Training XGBoost model...")
model = XGBClassifier(
    objective="binary:logistic",
    eval_metric="logloss",
    use_label_encoder=False,
    n_estimators=100,
    max_depth=5,
    learning_rate=0.1,
    random_state=42
)
model.fit(X_train_scaled, y_train)

# Predict probabilities and classes
y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]  # Probability Player 1 wins
y_pred = (y_pred_proba > 0.5).astype(int)

# -------------------------
# EVALUATE MODEL
# -------------------------
accuracy = accuracy_score(y_test, y_pred)
logloss = log_loss(y_test, y_pred_proba)
print(f"Test Accuracy: {accuracy:.4f}")
print(f"Test Log Loss: {logloss:.4f}")

# Feature importance
importances = pd.DataFrame({
    "Feature": X_train_scaled.columns,
    "Importance": model.feature_importances_
}).sort_values("Importance", ascending=False)
print("\nFeature Importance:")
print(importances)

# -------------------------
# COMPARE TO BETTING ODDS
# -------------------------
test_df["model_prob_p1"] = y_pred_proba
test_df["model_prob_p2"] = 1 - y_pred_proba

# Adjust implied probabilities based on target (Player 1 vs Player 2)
test_df["implied_prob_p1"] = np.where(test_df["target"] == 1, 
                                      test_df["implied_prob_w"], 
                                      test_df["implied_prob_l"])
test_df["implied_prob_p2"] = np.where(test_df["target"] == 1, 
                                      test_df["implied_prob_l"], 
                                      test_df["implied_prob_w"])

# Detect mispricing (significant differences)
test_df["prob_diff_p1"] = test_df["model_prob_p1"] - test_df["implied_prob_p1"]
test_df["prob_diff_p2"] = test_df["model_prob_p2"] - test_df["implied_prob_p2"]
threshold = 0.1  # Adjust this threshold for sensitivity
mispriced = test_df[
    (test_df["prob_diff_p1"].abs() > threshold) | 
    (test_df["prob_diff_p2"].abs() > threshold)
]

print(f"\nDetected {len(mispriced)} mispriced matches (threshold: {threshold}):")
print(mispriced[["matchid", "player1_name", "player2_name", "model_prob_p1", 
                 "implied_prob_p1", "prob_diff_p1", "target"]].head())

# -------------------------
# SAVE RESULTS
# -------------------------
mispriced.to_csv("mispriced_matches.csv", index=False)
print("Saved mispriced matches to 'mispriced_matches.csv'")