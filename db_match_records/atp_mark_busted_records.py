# Marks as busted a few of the rows which for whatever reason have unresolvable problems
import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from sqlalchemy import text
from db_connect import get_engine

# Get the database engine
engine = get_engine()

# -------------------------
# ENSURE COLUMN EXISTS
# -------------------------
with engine.connect() as connection:
    connection.execute(text('ALTER TABLE td_atp_2015_2024 ADD COLUMN IF NOT EXISTS "is_busted" TEXT;'))
    connection.commit()
print("✅ Column 'is_busted' ensured in td_atp_2015_2024.")

# -------------------------
# BUSTED GAMES LIST
# -------------------------
busted_games = [
    ("2023-04-06", "Kuzmanov D.", "Carballes Baena R."),
    ("2024-11-14", "De Minaur A.", "Fritz T."),
    ("2021-11-14", "Berrettini M.", "Zverev A."),
    ("2023-10-04", "Kecmanovic M.", "Bu Y.")
    # Add more records here...
]

# -------------------------
# UPDATE IS_BUSTED COLUMN
# -------------------------
updated_count = 0

for td_date, td_winner, td_loser in busted_games:
    print(f"\n🔄 Processing: {td_winner} vs {td_loser} on {td_date}")

    # Select match from td_atp_2015_2024
    with engine.connect() as connection:
        query_td = text("""
            SELECT "MatchId" FROM td_atp_2015_2024
            WHERE "Date" = :td_date
            AND "Winner" = :td_winner
            AND "Loser" = :td_loser
            AND "TA_Match_Id" IS NULL
        """)
        df_td = pd.read_sql(query_td, connection, params={"td_date": td_date, "td_winner": td_winner, "td_loser": td_loser})

    # Validate matches
    if len(df_td) == 1:
        td_match_id = int(df_td.iloc[0]["MatchId"])  # ✅ Convert numpy.int64 to native Python int

        # Update is_busted column
        with engine.connect() as connection:
            update_query = text("""
                UPDATE td_atp_2015_2024
                SET "is_busted" = 'Busted'
                WHERE "MatchId" = :td_match_id
            """)
            connection.execute(update_query, {"td_match_id": td_match_id})
            connection.commit()

        print(f"✅ Updated MatchId {td_match_id} as 'Busted'")
        updated_count += 1
    elif len(df_td) == 0:
        print(f"⚠️ No match found for {td_winner} vs {td_loser} on {td_date}.")
    else:
        print(f"⚠️ Multiple matches found for {td_winner} vs {td_loser} on {td_date}, skipping.")

# -------------------------
# SUMMARY
# -------------------------
print("\n✅ Batch Update Complete!")
print(f"✔️ Successfully updated: {updated_count} records")
