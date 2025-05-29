# 🎾 Tennis

# Current Model Evaluation

## ✅ Final Model Performance (With Odds Features)

| Metric           | Value   |
|------------------|---------|
| Accuracy         | 68.04%  |
| Log Loss         | 0.5989  |
| ROC-AUC Score    | 0.7393  |

---

## 📊 Feature Importance (Top Features)

## 📊 Feature Importance (Top Features With Odds)

| Rank | Feature                  | Importance | Description                                                  |
|------|--------------------------|------------|--------------------------------------------------------------|
| 1    | p2_was_fav               | 0.360572   | Player 2 opened as favourite                                 |
| 2    | p1_was_fav               | 0.140197   | Player 1 opened as favourite                                 |
| 3    | p2_was_fav_closing       | 0.037656   | Player 2 was favourite at close                              |
| 4    | p1_was_fav_closing       | 0.032315   | Player 1 was favourite at close                              |
| 5    | p1_stayed_fav            | 0.027372   | Player 1 opened and closed as favourite                      |
| 6    | glicko_diff              | 0.022775   | Overall Glicko rating difference                             |
| 7    | p2_stayed_fav            | 0.018636   | Player 2 opened and closed as favourite                      |
| 8    | p2_lost_fav              | 0.014697   | Player 2 opened favourite but lost it by close               |
| 9    | elo_diff                 | 0.014335   | Elo rating difference                                        |
| 10   | p1_lost_fav              | 0.011935   | Player 1 opened favourite but lost it by close               |
| 11   | glicko_surface_diff      | 0.011417   | Glicko surface-specific rating difference                    |
| 12   | p1_odds_shape_strength   | 0.011206   | Strength of Player 1 odds movement shape                     |
| 13   | p2_became_fav            | 0.010804   | Player 2 was not favourite early but became it later         |
| 14   | p1_became_fav            | 0.010622   | Player 1 was not favourite early but became it later         |
| 15   | p2_odds_shape_strength   | 0.009595   | Strength of Player 2 odds movement shape                     |

---

## 📉 XGBoost Historical Ratings Only (No Odds Features)

| Metric           | Value   |
|------------------|---------|
| Accuracy         | 64.91%  |
| Log Loss         | 0.6167  |
| ROC-AUC Score    | 0.7154  |

## 📊 Feature Importance (Top Features Without Odds)

| Rank | Feature                              | Importance | Description                                                  |
|------|--------------------------------------|------------|--------------------------------------------------------------|
| 1    | glicko_diff                          | 0.185738   | Overall Glicko rating difference                             |
| 2    | elo_diff                              | 0.089338   | Overall Elo rating difference                                |
| 3    | glicko_surface_diff                  | 0.072971   | Glicko surface-specific rating difference                    |
| 4    | hold_surface_pct_diff                | 0.035126   | Difference in hold percentage on surface                     |
| 5    | surface_elo_diff                     | 0.035022   | Elo surface-specific rating difference                       |
| 6    | tournament_strength                  | 0.034158   | Event tier (Grand Slam, Masters, ATP500, etc.)               |
| 7    | recent_matches_30d_diff              | 0.032316   | Difference in matches played in the last 30 days             |
| 8    | avg_elo_faced_diff                   | 0.030425   | Difference in average Elo of opponents faced                 |
| 9    | tournament_fatigue_diff              | 0.024950   | Minutes played in tournament so far                          |
| 10   | p1_surface_rd                        | 0.024887   | Player 1 surface-specific Glicko rating deviation            |
| 11   | hold_pct_diff                        | 0.024873   | Overall hold percentage difference                           |
| 12   | break_point_surface_conversion_diff  | 0.024419   | Surface-specific break point conversion % difference         |
| 13   | p1_overall_rd                        | 0.024001   | Player 1 overall Glicko rating deviation                     |
| 14   | break_surface_pct_diff               | 0.023716   | Surface-specific break percentage difference                 |
| 15   | h2h_wins_diff                        | 0.023350   | Difference in head-to-head wins between the two players      |



## Database

Open Terminal

Install brew if you don't have it:

    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

Install PostgreSQL locally

    brew install postgresql

Run the DB engine

    brew services start postgresql

Create the tennis DB

    createdb tennis

Run this:

    psql -l

You should see 'tennis' in the list of databases.

It helps to have a Database GUI to pore through the records.  I like Beekeeper Studio.  It's free. https://www.beekeeperstudio.io/ 

![Beekeeper Studio connection details](img/BeeKeeperStudioConn.png)

## 📌 Python Setup

### 1️⃣ Install Python 3 (Mac)
    brew install python3

### 2️⃣ Install required dependency for XGBoost (Mac)
    brew install libomp

### 3️⃣ Create a virtual environment (Recommended)
    python3 -m venv venv

### 4️⃣ Activate virtual environment
#### (Mac/Linux)
    source venv/bin/activate
#### (fish shell)
    source venv/bin/activate.fish
#### (Windows PowerShell)
    venv\Scripts\Activate.ps1
#### (Windows Command Prompt)
    venv\Scripts\activate.bat

### 5️⃣ Upgrade pip
    pip install --upgrade pip

### 6️⃣ Install required Python packages
    pip install -r requirements.txt

> **💡 Alternative:** If `requirements.txt` is missing, install manually:
> ```sh
> pip install pandas sqlalchemy psycopg2-binary scikit-learn xgboost optuna pymc
> ```

---

If you want to train a model without odds as a feature, open features.py and change the boolean from use_odds = True to use_odds = False

As long as you have run everything at least once if you want to switch odds on/off you just need to run steps 10 and 11.

## 📌 Data Preparation

### 1️⃣ Import data from CSV files into the database
    python 1_import_spreadsheet_data.py

### 2️⃣ Join ATP match records
    python 2_join_records_atp.py

### 3️⃣ Create structured table for model processing
    python 3_create_atp_matches_table.py

### 4️⃣ Match Betfair odds intervals to matches
    python 4_join_betfair_records_atp.py

### 5️⃣ Populate analysis table with computed features
    python 5_create_analysis_data.py

### 6️⃣ Assess odds only features such as late-money, drift and shape
    python 6_create_betfair_records_analysis.py

### 7️⃣ Populate matches table with odds features
    python 7_create_odds_features.py

### 8️⃣ Populate matches table with odds timeseries interval features
    python 8_create_odds_features_timeseries.py

### 9️⃣ Populate a separate table with model-ready data
    python 9_create_data_feed_table.py

### 🔟 Calculate best hyperparameters for model tuning predictions
    python 10_get_hyperparameters.py

### 1️⃣1️⃣ 📌 Training & Testing XGBoost prediction model
    python 11_train_xg_boost_model.py

---

> **💡 Note:** Does this save the model? If yes, where? If no, should we load a previous model instead?

---

✅ **Final Step:** Add `venv/` to `.gitignore`  
    ```sh
    echo "venv/" >> .gitignore
    ```