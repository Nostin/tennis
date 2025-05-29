import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_connect import get_engine
import json
import pandas as pd
import numpy as np
import optuna
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.calibration import CalibratedClassifierCV
from xgboost import XGBClassifier
import warnings
from sqlalchemy import text
from features import features, PARAMS_FILE

warnings.filterwarnings("ignore")

# -------------------------
# CONFIGURATION
# -------------------------
TABLE_NAME = "xgboost_data_feed"

# -------------------------
# LOAD DATA
# -------------------------
print("Loading data from database...")
engine = get_engine()
with engine.connect() as conn:
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} ORDER BY date ASC", conn)

required_cols = ["date", "target"] + features
if not all(col in df.columns for col in required_cols):
    raise ValueError("Missing required columns in dataset.")

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

X = df[features]
y = df["target"]
X = pd.get_dummies(X, columns=["surface"], drop_first=False)

# -------------------------
# LOAD OR OPTIMIZE HYPERPARAMETERS
# -------------------------
def load_or_tune_xgboost(X_train, y_train, X_val, y_val):
    try:
        with open(PARAMS_FILE, "r") as f:
            best_params = json.load(f)
        print("\n✅ Loaded best XGBoost parameters from file.")
    except FileNotFoundError:
        print("\n⚠️ No saved hyperparameters found. Running Optuna...")

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

            base_model = XGBClassifier(objective="binary:logistic", eval_metric="logloss", **params)
            model = CalibratedClassifierCV(base_model, method="sigmoid", cv=3)
            model.fit(X_train, y_train)
            y_pred_proba = model.predict_proba(X_val)[:, 1]
            return log_loss(y_val, y_pred_proba)

        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=50, timeout=1800)

        best_params = study.best_params
        with open(PARAMS_FILE, "w") as f:
            json.dump(best_params, f, indent=4)

        print("\n✅ Best hyperparameters saved to 'best_xgboost_params.json'")

    return best_params

# see which features made the most difference
def get_feature_importance(model, feature_names):
    """
    Extract and average feature importance from a model.
    Supports both raw XGBClassifier and CalibratedClassifierCV-wrapped models.
    """
    importances = []

    if isinstance(model, CalibratedClassifierCV):
        if hasattr(model, "calibrated_classifiers_"):
            for clf in model.calibrated_classifiers_:
                # Access the original XGBClassifier inside _CalibratedClassifier
                est = clf.estimator
                if hasattr(est, "feature_importances_"):
                    importances.append(est.feature_importances_)
    elif hasattr(model, "feature_importances_"):
        importances.append(model.feature_importances_)
    else:
        raise ValueError("Model does not expose feature importances.")

    if not importances:
        raise ValueError("No feature importances found in the model.")

    mean_importance = np.mean(importances, axis=0)
    return pd.DataFrame({
        "Feature": feature_names,
        "Importance": mean_importance
    }).sort_values("Importance", ascending=False)


# -------------------------
# WEEK-BY-WEEK RETRAINING
# -------------------------
train_mask = df["date"] < "2024-01-01"
X_train, y_train = X[train_mask], y[train_mask]

best_xgb_params = load_or_tune_xgboost(X_train, y_train, X_train, y_train)
base_model = XGBClassifier(objective="binary:logistic", eval_metric="logloss", **best_xgb_params)
model = CalibratedClassifierCV(base_model, method="sigmoid", cv=5)
model.fit(X_train, y_train)

total_y_true = []
total_y_pred_proba = []

weeks = sorted(df[df["date"] >= "2024-01-01"]["date"].dt.to_period("W").unique())
print("\n📅 Running week-by-week prediction and retraining...")

for week in weeks:
    print(f"\n📆 Predicting matches for week: {week}")
    week_mask = df["date"].dt.to_period("W") == week
    X_test, y_test = X[week_mask], y[week_mask]

    if X_test.empty:
        continue

    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_pred_proba > 0.5).astype(int)

    total_y_true.extend(y_test)
    total_y_pred_proba.extend(y_pred_proba)

    acc = accuracy_score(y_test, y_pred)
    ll = log_loss(y_test, y_pred_proba)
    auc = roc_auc_score(y_test, y_pred_proba)

    print(f"  Accuracy: {acc:.4f}, Log Loss: {ll:.4f}, ROC-AUC: {auc:.4f}")

    X_train = pd.concat([X_train, X_test])
    y_train = pd.concat([y_train, y_test])
    model.fit(X_train, y_train)

# -------------------------
# SAVE PREDICTIONS TO DATABASE
# -------------------------
print("\n📦 Saving model probabilities to the database...")

# Add predictions to the original dataframe to match matchid
df_results = df[df["date"] >= "2024-01-01"].copy()
df_results = df_results.reset_index(drop=True)

# Ensure lengths match
assert len(df_results) == len(total_y_pred_proba), "Mismatch in prediction length."

# Assign probabilities and matchid (assumes matchid is in the table)
df_results["model_prob_p1"] = total_y_pred_proba
df_results["model_prob_p2"] = 1 - df_results["model_prob_p1"]

# Subset to matchid and probabilities
if "matchid" not in df_results.columns:
    raise ValueError("matchid column missing from dataset. Ensure it's in your data feed.")

# Use actual player names for predicted winner
df_results["predicted_winner"] = np.where(
    df_results["model_prob_p1"] > df_results["model_prob_p2"],
    df_results["player1_name"],
    df_results["player2_name"]
)

# Save extra context for use in later joins
columns_to_save = [
    "matchid", "model_prob_p1", "model_prob_p2", "predicted_winner", "player1_name", "player2_name", "winner_name", "loser_name"
]
df_to_save = df_results[columns_to_save]

# Save to database
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS xgboost_predictions CASCADE;"))
    df_to_save.to_sql("xgboost_predictions", con=conn, index=False)

print("✅ Saved predicted probabilities to table 'xgboost_predictions'")


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

feature_importance = get_feature_importance(model, X.columns)

print("\n📊 Feature Importance (Top 10 Features):")
print(feature_importance.head(10))

feature_importance.to_csv("feature_importance.csv", index=False)
print("\n✅ Feature importance saved to 'feature_importance.csv'")
