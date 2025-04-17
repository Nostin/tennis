import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from sqlalchemy import text
from db_connect import get_engine

# Get the database engine
engine = get_engine()

def match_additional_odds():
    """Attempt to match remaining unmatched ATP matches with Betfair odds using a 3-day date buffer."""
    print("Matching additional ATP matches with Betfair odds (4-day window)...")

    with engine.connect() as conn:
        # Load unmatched ATP matches
        atp_matches = pd.read_sql("""
            SELECT matchid, winner_name, loser_name, date
            FROM matched_atp_records
            WHERE date IS NOT NULL
              AND betfair_odds_id_winner IS NULL
              AND betfair_odds_id_loser IS NULL
        """, conn)

        # Load Betfair odds records with normalized player names
        betfair_odds = pd.read_sql("""
            SELECT record_id, event_id, date, 
                   player1_name_normalised, player2_name_normalised, 
                   odds_player_name_normalised
            FROM betfair_odds
            WHERE player1_name_normalised IS NOT NULL 
              AND player2_name_normalised IS NOT NULL
              AND odds_player_name_normalised IS NOT NULL
        """, conn)

        print(f"Loaded {len(atp_matches)} unmatched ATP matches and {len(betfair_odds)} Betfair odds records.")

        atp_matches['date'] = pd.to_datetime(atp_matches['date'])
        betfair_odds['date'] = pd.to_datetime(betfair_odds['date'])

        updates = []
        match_count = 0

        for idx, match in atp_matches.iterrows():
            if idx % 1000 == 0:
                print(f"Processing match {idx + 1} of {len(atp_matches)}...")

            winner = match['winner_name']
            loser = match['loser_name']
            match_date = match['date']

            date_window = betfair_odds[
                (betfair_odds['date'] >= match_date - pd.Timedelta(days=4)) &
                (betfair_odds['date'] <= match_date + pd.Timedelta(days=4))
            ]

            for _, odds in date_window.iterrows():
                p1 = odds['player1_name_normalised']
                p2 = odds['player2_name_normalised']
                odds_p = odds['odds_player_name_normalised']
                rec_id = odds['record_id']

                if ((p1 == winner and p2 == loser) or (p1 == loser and p2 == winner)):
                  updated = False
                  if odds_p == winner:
                      updates.append(('betfair_odds_id_winner', match['matchid'], rec_id))
                      updated = True
                  if odds_p == loser:
                      updates.append(('betfair_odds_id_loser', match['matchid'], rec_id))
                      updated = True
                  if updated:
                      match_count += 1


        print(f"\nTotal matches found and to be updated: {match_count}")

        for col, matchid, betfair_id in updates:
            print(f"Updating matchid={matchid}, column={col}, betfair_id={betfair_id}")
            conn.execute(
                text(f"""
                    UPDATE matched_atp_records
                    SET {col} = :betfair_id
                    WHERE matchid = :matchid AND {col} IS NULL
                """),
                {'matchid': matchid, 'betfair_id': betfair_id}
            )

        conn.commit()

        # Log how many still unmatched
        remaining = conn.execute(text("""
            SELECT COUNT(*) FROM matched_atp_records
            WHERE betfair_odds_id_winner IS NULL AND betfair_odds_id_loser IS NULL
        """)).scalar()
        print(f"Remaining unmatched records: {remaining}")

    print("Additional odds matching complete.")

if __name__ == "__main__":
    match_additional_odds()
