import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------
# CONFIGURATION
# -------------------------
DB_NAME = "tennis"
DB_USER = "seanthompson"
DB_PASS = ""  # Add password if necessary
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "matched_atp_records"

# -------------------------
# CONNECT TO POSTGRESQL
# -------------------------
print("Connecting to the database...")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -------------------------
# CREATE TABLE (REPLACING EXISTING)
# -------------------------
create_table_query = f"""
DROP TABLE IF EXISTS {TABLE_NAME};
CREATE TABLE {TABLE_NAME} (
    MatchId INT PRIMARY KEY,
    Tournament TEXT,
    Tournament_IOC TEXT,
    Date DATE,
    AvgW FLOAT,
    AvgL FLOAT,
    Comment TEXT,
    Surface TEXT,
    Series TEXT,
    Gender TEXT,
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
    w_1stWon INT,
    l_1stWon INT,
    w_2ndWon INT,
    l_2ndWon INT,
    w_SvGms INT,
    l_SvGms INT,
    w_bpSaved INT,
    l_bpSaved INT,
    w_bpFaced INT,
    l_bpFaced INT,
    winner_days_since_last INT DEFAULT NULL,
    loser_days_since_last INT DEFAULT NULL,
    winner_recent_matches_30d INT DEFAULT 0,
    loser_recent_matches_30d INT DEFAULT 0,
    w_h2h_wins INT DEFAULT 0,
    l_h2h_wins INT DEFAULT 0,
    w_h2h_wins_clay INT DEFAULT 0,
    l_h2h_wins_clay INT DEFAULT 0,
    w_h2h_wins_grass INT DEFAULT 0,
    l_h2h_wins_grass INT DEFAULT 0,
    w_h2h_wins_hard INT DEFAULT 0,
    l_h2h_wins_hard INT DEFAULT 0,
    winner_fatigue NUMERIC(8,2) DEFAULT 0.00,
    loser_fatigue NUMERIC(8,2) DEFAULT 0.00,
    winner_win_pct_3m NUMERIC(5,2),
    loser_win_pct_3m NUMERIC(5,2),
    winner_win_pct_6m NUMERIC(5,2),
    loser_win_pct_6m NUMERIC(5,2),
    winner_hold_pct_total NUMERIC(5,2),
    loser_hold_pct_total NUMERIC(5,2),
    winner_hold_pct_roll NUMERIC(5,2),
    loser_hold_pct_roll NUMERIC(5,2),
    winner_break_pct_total NUMERIC(5,2),
    winner_break_pct_roll NUMERIC(5,2),
    loser_break_pct_total NUMERIC(5,2),
    loser_break_pct_roll NUMERIC(5,2),
    winner_hold_pct_total_hard NUMERIC(5,2),
    loser_hold_pct_total_hard NUMERIC(5,2),
    winner_hold_pct_roll_hard NUMERIC(5,2),
    loser_hold_pct_roll_hard NUMERIC(5,2),
    winner_break_pct_total_hard NUMERIC(5,2),
    loser_break_pct_total_hard NUMERIC(5,2),
    winner_break_pct_roll_hard NUMERIC(5,2),
    loser_break_pct_roll_hard NUMERIC(5,2),
    winner_hold_pct_total_grass NUMERIC(5,2),
    loser_hold_pct_total_grass NUMERIC(5,2),
    winner_hold_pct_roll_grass NUMERIC(5,2),
    loser_hold_pct_roll_grass NUMERIC(5,2),
    winner_break_pct_total_grass NUMERIC(5,2),
    loser_break_pct_total_grass NUMERIC(5,2),
    winner_break_pct_roll_grass NUMERIC(5,2),
    loser_break_pct_roll_grass NUMERIC(5,2),
    winner_hold_pct_total_clay NUMERIC(5,2),
    loser_hold_pct_total_clay NUMERIC(5,2),
    winner_hold_pct_roll_clay NUMERIC(5,2),
    loser_hold_pct_roll_clay NUMERIC(5,2),
    winner_break_pct_total_clay NUMERIC(5,2),
    loser_break_pct_total_clay NUMERIC(5,2),
    winner_break_pct_roll_clay NUMERIC(5,2),
    loser_break_pct_roll_clay NUMERIC(5,2),
    winner_tb_win_pct NUMERIC(5,2),
    loser_tb_win_pct NUMERIC(5,2),
    winner_overall_elo FLOAT,
    winner_surface_elo FLOAT,
    winner_total_matches INT,
    winner_avg_elo_faced FLOAT,
    loser_overall_elo FLOAT,
    loser_surface_elo FLOAT,
    loser_total_matches INT,
    loser_avg_elo_faced FLOAT
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
    MatchId, Tournament, Tournament_IOC, Date, AvgW, AvgL, Comment, Surface, Series, Gender,
    winner_name, loser_name, winner_ioc, loser_ioc, winner_age, loser_age, score, minutes, 
    w_ace, l_ace, w_df, l_df, w_svpt, l_svpt,
    w_1stin, l_1stin, w_1stWon, l_1stWon, w_2ndWon, l_2ndWon,
    w_SvGms, l_SvGms, w_bpSaved, l_bpSaved, w_bpFaced, l_bpFaced
)
SELECT 
    td."MatchId", td."Tournament", td."Tournament_IOC", td."Date", td."AvgW", td."AvgL", td."Comment", td."Surface", td."Series", td."Gender",
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
