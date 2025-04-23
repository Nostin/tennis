# tennis

# Current Model Evaluation
XGBoost without Glicko:
Total Accuracy: 0.6512
Total Log Loss: 0.6232
Total ROC-AUC Score: 0.7066

With Glicko Ratings:
Total Accuracy: 0.6448
Total Log Loss: 0.6194
Total ROC-AUC Score: 0.7106

## 🔍 Feature Importance (Top 10 Features)

| Rank | Feature                     | Importance | Description                                                |
|------|-----------------------------|------------|------------------------------------------------------------|
| 1    | `elo_diff`                  | 0.2438     | Elo rating difference – strongest predictor                |
| 2    | `surface_elo_diff`          | 0.0811     | Elo difference adjusted for surface                        |
| 3    | `avg_elo_faced_diff`        | 0.0759     | Strength of opponents faced                                |
| 4    | `recent_matches_30d_diff`   | 0.0553     | Match volume over the past 30 days                         |
| 5    | `tournament_strength`       | 0.0441     | Tournament level (e.g. Grand Slam, ATP 250, etc.)          |
| 6    | `avg_surface_elo_faced_diff`| 0.0386     | Surface-specific strength of opponents faced               |
| 7    | `tournament_fatigue_diff`   | 0.0384     | Relative fatigue from minutes played                       |
| 8    | `h2h_wins_diff`             | 0.0370     | Overall head-to-head record                                |
| 9    | `hold_surface_pct_diff`     | 0.0343     | Surface-adjusted hold-of-serve percentage difference       |
| 10   | `win_pct_last_30d_diff`     | 0.0340     | Recent win percentage over last 30 days                    |


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

## 📌 Data Preparation

### 7️⃣ Import data from CSV files into the database
    python import_spreadsheet_data.py

### 8️⃣ Join ATP match records
    python join_records_atp.py

### 9️⃣ Create structured table for model processing
    python create_atp_matches_table.py

### 🔟 Match Betfair odds intervals to matches
    python join_betfair_odds.py

### 1️⃣1️⃣ Populate analysis table with computed features
    python create_analysis_data.py

### 1️⃣2️⃣ Create final model input dataset
    python create_data_feed_table.py

---

## 📌 Model Training & Testing

### 1️⃣3️⃣ Train the XGBoost Model
    python train_xgboost.py

> **💡 Note:** Does this save the model? If yes, where? If no, should we load a previous model instead?

---

✅ **Final Step:** Add `venv/` to `.gitignore`  
    ```sh
    echo "venv/" >> .gitignore
    ```

## What is happening

### Generating the data set

There are four CSV files in the `spreadsheet_raw` directory which contain all WTA and ATP tennis matches from tennis-data.co.uk and tennis abstract https://github.com/JeffSackmann/tennis_atp https://github.com/JeffSackmann/tennis_wta.  The two sources have similar buf slightly different data about the matches.  The tennis-data records have the betting odds and the tennis abstract data has things like duration, service holds/breaks, aces etc.. so we need to coalesce these two data sources together to create a holistic view of each tennis match.  We import the womens and mens CSVs for both into separate database tables and then join them using common matching fields using strict_match_atp.py and strict_match_wta.py

### Generating ELO ratings for each player

The entire generated data set is iterated and an ELO rating is calculated for each player using the create_elo_ratings.py script.  When ordered by the Overall rating this should generally resemble the list of names here: https://tennisabstract.com/reports/atp_elo_ratings.html (not necessarily the numbers though).

The ELO rating incorporates:

- a dynamic K factor adjusted for how many matches the player has played
- separate surface specific ratings which are blended with the player's overall rating using logarithmic weighting
- rating decay due to inactivity
- decay from last played match up until the date the ratings are generated
- The ELO compounds over the full period - there's no rolling window
- Serve / Return strength adjustment
- Adjustment for tie breaks to account for close matches



prediction models

create ELO for each player factoring in:
- surface adjustment: (w × Surface Rating) + ((1 − w) × Overall Rating) => w is based on the number of matches played on that surface.
- decay for inactivity
- additional decay from last match played up until current date

Only consider betting if a player has at least 5 recent matches AND 25+ career matches.

If surface matches < 3 in the last 12 months, ignore surface-specific adjustments (small sample size).

Per match played:
- number of aces
- number of doubles faults
- number of serve points
- number of first serves made
- number of first-serve points won
- number of second-serve points won
- number of serve games
- number of break points saved
- number of break points faced
- minutes

Approach: Blend Career & Recent Form
We can weight ratings using a decaying factor:


Blended Elo=(α×Recent Elo)+((1−α)×Overall Elo)
Recent Elo: Last 6 months (or last 10 matches, whichever is greater).
Overall Elo: Lifetime rating.
Alpha (Recency Weight): 0.65 (for fast-changing players) or 0.35 (for consistent players like Djokovic).