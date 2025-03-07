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
# MATCHING PAIRS TO UPDATE
# -------------------------
match_pairs = [
    # Format: (td_date, td_winner, td_loser, ta_date, ta_winner, ta_loser)
    ("2020-08-22", "Auger-Aliassime F.", "Basilashvili N.", "2020-08-24", "Felix Auger Aliassime", "Nikoloz Basilashvili"),
    ("2020-08-22", "Carreno Busta P.", "Lajovic D.", "2020-08-24", "Pablo Carreno Busta", "Dusan Lajovic"),
    ("2020-08-22", "Struff J.L.", "De Minaur A.", "2020-08-24", "Jan Lennard Struff", "Alex De Minaur"),
    ("2020-08-22", "Anderson K.", "Edmund K.", "2020-08-24", "Kevin Anderson", "Kyle Edmund"),
    ("2020-08-22", "Opelka R.", "Norrie C.", "2020-08-24", "Reilly Opelka", "Cameron Norrie"),
    ("2020-08-22", "Coric B.", "Paire B.", "2020-08-24", "Borna Coric", "Benoit Paire"),
    ("2020-08-22", "Fritz T.", "Harris L.", "2020-08-24", "Taylor Fritz", "Lloyd Harris"),
    ("2020-08-22", "Krajinovic F.", "Caruso S.", "2020-08-24", "Filip Krajinovic", "Salvatore Caruso"),
    ("2020-08-22", "Murray A.", "Tiafoe F.", "2020-08-24", "Andy Murray", "Frances Tiafoe"),
    ("2020-08-22", "Berankis R.", "Paul T.", "2020-08-24", "Ricardas Berankis", "Tommy Paul"),
    ("2020-09-03", "Khachanov K.", "Kuznetsov An.", "2020-08-31", "Karen Khachanov", "Andrey Kuznetsov"),
    ("2020-09-01", "Kuznetsov An.", "Querrey S.", "2020-08-31", "Andrey Kuznetsov", "Sam Querrey"),
    ("2023-03-01", "Mcdonald M.", "Darderi L.", "2023-02-27", "Mackenzie Mcdonald", "Luciano Darderi"),
    ("2023-01-10", "Haase R.", "Bonzi B.", "2023-01-09", "Robin Haase", "Benjamin Bonzi"),
    ("2021-11-07", "Djokovic N.", "Medvedev D.", "2021-11-01", "Novak Djokovic", "Daniil Medvedev"),
    ("2023-11-12", "Sinner J.", "Tsitsipas S.", "2023-11-13", "Jannik Sinner", "Stefanos Tsitsipas"),
    ("2023-11-12", "Djokovic N.", "Rune H.", "2023-11-13", "Novak Djokovic", "Holger Rune"),
    ("2021-10-10", "Medvedev D.", "Mcdonald M.", "2021-10-04", "Daniil Medvedev", "Mackenzie Mcdonald"),
    ("2021-10-07", "Mcdonald M.", "Duckworth J.", "2021-10-04", "Mackenzie Mcdonald", "James Duckworth"),
    ("2024-08-07", "Rune H.", "Bautista Agut R.", "2024-08-05", "Holger Rune", "Roberto Bautista Agut"),
    ("2023-05-28", "Carballes Baena R.", "Nava E.", "2023-05-29", "Roberto Carballes Baena", "Eduardo Nava"),
    ("2015-01-14", "Mannarino A.", "Bautista R.", "2015-01-12", "Adrian Mannarino", "Roberto Bautista Agut"),
    ("2017-05-17", "Nadal R.", "Almagro N.", "2017-05-15", "Rafael Nadal", "Nicolas Almagro"),
    ("2024-03-20", "Machac T.", "Blanch D.", "2024-03-18", "Tomas Machac", "Darwin Blanch"),
    ("2016-04-26", "Cervantes I.", "Munoz De La Nava D.", "2016-04-25", "Inigo Cervantes Huegun", "Daniel Munoz de la Nava"),
    ("2016-04-27", "Kyrgios N.", "Cervantes I.", "2016-04-25", "Nick Kyrgios", "Inigo Cervantes Huegun"),
    ("2018-04-15", "Zverev M.", "Auger-Aliassime F.", "2018-04-16", "Mischa Zverev", "Felix Auger Aliassime"),
    ("2017-08-08", "Shapovalov D.", "Dutra Silva R.", "2017-08-07", "Denis Shapovalov", "Rogerio Dutra Silva"),
    ("2019-06-14", "Mannarino A.", "Goffin D.", "2019-06-10", "Adrian Mannarino", "David Goffin")
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
