import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------
# CONFIGURATION
# -------------------------
DB_NAME = "tennis"
DB_USER = "seanthompson"
DB_PASS = ""  # Set if required
DB_HOST = "localhost"
DB_PORT = "5432"

# -------------------------
# DATABASE CONNECTION
# -------------------------
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -------------------------
# MATCHING PAIRS TO UPDATE
# -------------------------
match_pairs = [
    # Format: (td_date, td_winner, td_loser, ta_date, ta_winner, ta_loser)
    ("2015-01-12", "De Schepper K.", "Giraldo S.", "2015-01-12", "Kenny De Schepper", "Santiago Giraldo"),
    ("2015-01-13", "Falla A.", "De Schepper K.", "2015-01-12", "Alejandro Falla", "Kenny De Schepper"),
    ("2015-01-13", "Johnson S.", "Carreno-Busta P.", "2015-01-12", "Steve Johnson", "Pablo Carreno Busta"),
    ("2015-01-13", "Schwartzman D.", "Garcia-Lopez G.", "2015-01-12", "Diego Schwartzman", "Guillermo Garcia Lopez"),
    ("2015-01-13", "Ramos-Vinolas A.", "Lorenzi P.", "2015-01-12", "Albert Ramos", "Paolo Lorenzi"),
    ("2015-01-13", "Carreno-Busta P.", "Coric B.", "2015-01-12", "Pablo Carreno Busta", "Borna Coric"),
    ("2015-01-13", "Del Potro J.M.", "Stakhovsky S.", "2015-01-11", "Juan Martin del Potro", "Sergiy Stakhovsky"),
    ("2015-01-14", "Mannarino A.", "Bautista R.", "2015-01-12", "Adrian Mannarino", "Roberto Bautista Agut"),
    ("2015-01-14", "Ramos-Vinolas A.", "Lu Y.H.", "2015-01-12", "Albert Ramos", "Yen Hsun Lu"),
    ("2015-01-14", "Del Potro J.M.", "Fognini F.", "2015-01-11", "Juan Martin del Potro", "Fabio Fognini"),
    ("2015-01-15", "Pouille L.", "Ramos-Vinolas A.", "2015-01-12", "Lucas Pouille", "Albert Ramos"),
    ("2015-04-07", "Giraldo S.", "Dutra Silva R.", "2015-04-06", "Santiago Giraldo", "Rogerio Dutra Silva"),
    ("2015-07-22", "Delbonis F.", "Dutra Silva R.", "2015-07-20", "Federico Delbonis", "Rogerio Dutra Silva"),
    ("2015-08-03", "Giraldo S.", "Dutra Silva R.", "2015-08-03", "Santiago Giraldo", "Rogerio Dutra Silva"),
    ("2016-02-01", "Carreno-Busta P.", "Dutra Silva R.", "2016-02-01", "Pablo Carreno Busta", "Rogerio Dutra Silva"),
    ("2016-03-24", "Kuznetsov An.", "Dutra Silva R.", "2016-03-21", "Andrey Kuznetsov", "Rogerio Dutra Silva"),
    
    # Add more records here...
]

# -------------------------
# PROCESS MATCH PAIRS
# -------------------------
updated_count = 0
skipped_count = 0

for td_date, td_winner, td_loser, ta_date, ta_winner, ta_loser in match_pairs:
    print(f"\n🔄 Processing: {td_winner} vs {td_loser} on {td_date}")

    # Select match from td_atp_2015_2024
    with engine.connect() as connection:
        query_td = f"""
            SELECT * FROM td_atp_2015_2024
            WHERE "Date" = '{td_date}'
            AND "Winner" = '{td_winner}'
            AND "Loser" = '{td_loser}'
            AND "TA_Match_Id" IS NULL
        """
        df_td = pd.read_sql(query_td, connection)

    # Select match from ta_atp_2015_2024
    with engine.connect() as connection:
        query_ta = f"""
            SELECT * FROM ta_atp_2015_2024
            WHERE "tourney_date" = '{ta_date}'
            AND "winner_name" = '{ta_winner}'
            AND "loser_name" = '{ta_loser}'
        """
        df_ta = pd.read_sql(query_ta, connection)

    # Validate matches
    if len(df_td) == 1 and len(df_ta) == 1:
        td_match_id = df_td.iloc[0]["MatchId"]
        ta_match_id = df_ta.iloc[0]["MatchId"]

        # Update TA_Match_Id
        with engine.connect() as connection:
            update_query = f"""
                UPDATE td_atp_2015_2024
                SET "TA_Match_Id" = {ta_match_id}
                WHERE "MatchId" = {td_match_id}
            """
            connection.execute(text(update_query))
            connection.commit()

        print(f"✅ Updated MatchId {td_match_id} with TA_Match_Id {ta_match_id}")
        updated_count += 1

    elif len(df_td) == 0:
        print("⚠️ No match found in td_atp_2015_2024.")
        skipped_count += 1
    elif len(df_ta) == 0:
        print("⚠️ No match found in ta_atp_2015_2024.")
        skipped_count += 1
    else:
        print("⚠️ Multiple matches found, update skipped.")
        skipped_count += 1

# -------------------------
# SUMMARY
# -------------------------
print("\n✅ Batch Processing Complete!")
print(f"✔️ Successfully updated: {updated_count} records")
print(f"⚠️ Skipped (unmatched/multiple matches): {skipped_count} records")
