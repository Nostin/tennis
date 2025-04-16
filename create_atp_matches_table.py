import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from sqlalchemy import text
from db_connect import get_engine

# Get the database engine
engine = get_engine()

TABLE_NAME = "matched_atp_records"

# -------------------------
# CREATE TABLE (REPLACING EXISTING)
# -------------------------
create_table_query = f"""
DROP TABLE IF EXISTS {TABLE_NAME};
CREATE TABLE {TABLE_NAME} (
    matchid INT PRIMARY KEY,
    tourney_id VARCHAR(20),
    tournament TEXT,
    tournament_ioc TEXT,
    date DATE,
    avgw FLOAT,
    avgl FLOAT,
    comment TEXT,
    surface TEXT,
    series TEXT,
    gender TEXT,
    winner_name TEXT,
    loser_name TEXT,
    winner_ioc TEXT,
    loser_ioc TEXT,
    winner_age FLOAT,
    loser_age FLOAT,
    score TEXT,
    minutes INT,
    w_ace INT,
    l_ace INT,
    w_df INT,
    l_df INT,
    w_svpt INT,
    l_svpt INT,
    w_1stin INT,
    l_1stin INT,
    w_1stwon INT,
    l_1stwon INT,
    w_2ndwon INT,
    l_2ndwon INT,
    w_svgms INT,
    l_svgms INT,
    w_bpsaved INT,
    l_bpsaved INT,
    w_bpfaced INT,
    l_bpfaced INT
);
"""
print(f"Creating table '{TABLE_NAME}'...")
with engine.connect() as connection:
    connection.execute(text(create_table_query))
    connection.commit()
    print(f"✅ Table '{TABLE_NAME}' created successfully!")

# -------------------------
# INSERT MATCHED DATA
# -------------------------
insert_query = f"""
INSERT INTO {TABLE_NAME} (
    matchid, tourney_id, tournament, tournament_ioc, date, avgw, avgl, comment, surface, series, gender,
    winner_name, loser_name, winner_ioc, loser_ioc, winner_age, loser_age, score, minutes, 
    w_ace, l_ace, w_df, l_df, w_svpt, l_svpt,
    w_1stin, l_1stin, w_1stWon, l_1stWon, w_2ndWon, l_2ndWon,
    w_SvGms, l_SvGms, w_bpSaved, l_bpSaved, w_bpFaced, l_bpFaced
)
SELECT 
    td."MatchId", ta.tourney_id, td."Tournament", td."Tournament_IOC", td."Date", td."AvgW", td."AvgL", td."Comment", td."Surface", td."Series", td."Gender",
    ta.winner_name, ta.loser_name, ta.winner_ioc, ta.loser_ioc, ta.winner_age, ta.loser_age, ta.score, ta.minutes, 
    ta.w_ace, ta.l_ace, ta.w_df, ta.l_df, ta.w_svpt, ta.l_svpt,
    ta."w_1stIn", ta."l_1stIn", ta."w_1stWon", ta."l_1stWon", ta."w_2ndWon", ta."l_2ndWon",
    ta."w_SvGms", ta."l_SvGms", ta."w_bpSaved", ta."l_bpSaved", ta."w_bpFaced", ta."l_bpFaced"
FROM td_atp_2015_2024 td
JOIN ta_atp_2015_2024 ta
ON td."TA_Match_Id" = ta."MatchId"
WHERE td."TA_Match_Id" IS NOT NULL;
"""
print("Inserting matched records into 'matched_atp_records'...")
with engine.connect() as connection:
    connection.execute(text(insert_query))
    connection.commit()
    print(f"✅ Matched records successfully inserted into '{TABLE_NAME}'!")

# -------------------------
# VERIFY DATA
# -------------------------
print("Verifying inserted data...")
with engine.connect() as connection:
    result = connection.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
    count = result.scalar()
    print(f"✅ Total records in '{TABLE_NAME}': {count}")

print("✅ Data processing complete!")
