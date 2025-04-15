import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from sqlalchemy import text
from db_connect import get_engine

# Get the database engine
engine = get_engine()

def add_betfair_odds_columns():
    """Add Betfair odds columns to the atp_matches table."""
    with engine.connect() as conn:
        # Drop existing columns if they exist
        conn.execute(text("""
            ALTER TABLE matched_atp_records 
            DROP COLUMN IF EXISTS betfair_odds_id_winner,
            DROP COLUMN IF EXISTS betfair_odds_id_loser;
        """))
        conn.commit()
        
        # Add new columns
        conn.execute(text("""
            ALTER TABLE matched_atp_records 
            ADD COLUMN betfair_odds_id_winner VARCHAR(255),
            ADD COLUMN betfair_odds_id_loser VARCHAR(255);
        """))
        conn.commit()

def match_exact_odds():
    """Match ATP matches with Betfair odds using exact player names and dates."""
    print("Matching ATP matches with Betfair odds...")
    
    with engine.connect() as conn:
        # Load ATP matches and Betfair odds
        atp_matches = pd.read_sql("""
            SELECT matchid, winner_name, loser_name, date
            FROM matched_atp_records
            WHERE date IS NOT NULL
        """, conn)
        
        betfair_odds = pd.read_sql("""
            SELECT record_id, event_id, date, 
                   player1_name_normalised, player2_name_normalised, 
                   odds_player_name_normalised
            FROM betfair_odds
            WHERE player1_name_normalised IS NOT NULL 
            AND player2_name_normalised IS NOT NULL
            AND odds_player_name_normalised IS NOT NULL
        """, conn)
        
        print(f"Loaded {len(atp_matches)} ATP matches and {len(betfair_odds)} Betfair odds records with normalized names")
        
        # Convert dates to datetime for comparison
        atp_matches['date'] = pd.to_datetime(atp_matches['date'])
        betfair_odds['date'] = pd.to_datetime(betfair_odds['date'])
        
        # Create dictionaries to store matches
        winner_matches = []
        loser_matches = []
        potential_matches_count = 0
        player_matches_count = 0
        
        # Iterate through ATP matches
        for idx, atp_match in atp_matches.iterrows():
            if idx % 1000 == 0:
                print(f"Processing match {idx + 1} of {len(atp_matches)}...")
                
            winner = atp_match['winner_name']
            loser = atp_match['loser_name']
            atp_date = atp_match['date']
            
            # Find Betfair records within 2 days of ATP match date
            date_mask = (betfair_odds['date'] >= atp_date - pd.Timedelta(days=2)) & \
                       (betfair_odds['date'] <= atp_date + pd.Timedelta(days=2))
            potential_matches = betfair_odds[date_mask]
            potential_matches_count += len(potential_matches)
            
            # Check for matches where both players are in the same record
            for _, betfair_match in potential_matches.iterrows():
                player1 = betfair_match['player1_name_normalised']
                player2 = betfair_match['player2_name_normalised']
                odds_player = betfair_match['odds_player_name_normalised']
                
                # Check if both players are in this record
                if ((player1 == winner and player2 == loser) or 
                    (player1 == loser and player2 == winner)):
                    player_matches_count += 1
                    
                    # If this is the winner's odds
                    if odds_player == winner:
                        winner_matches.append({
                            'atp_matchid': atp_match['matchid'],
                            'betfair_record_id': betfair_match['record_id']
                        })
                    
                    # If this is the loser's odds
                    if odds_player == loser:
                        loser_matches.append({
                            'atp_matchid': atp_match['matchid'],
                            'betfair_record_id': betfair_match['record_id']
                        })
        
        print(f"\nMatching Statistics:")
        print(f"Total potential date matches: {potential_matches_count}")
        print(f"Total player matches: {player_matches_count}")
        print(f"Winner matches found: {len(winner_matches)}")
        print(f"Loser matches found: {len(loser_matches)}")
        
        # Update the database with winner matches
        for match in winner_matches:
            conn.execute(
                text("""
                    UPDATE matched_atp_records
                    SET betfair_odds_id_winner = :betfair_id
                    WHERE matchid = :atp_id
                """),
                {
                    'betfair_id': match['betfair_record_id'],
                    'atp_id': match['atp_matchid']
                }
            )
        
        # Update the database with loser matches
        for match in loser_matches:
            conn.execute(
                text("""
                    UPDATE matched_atp_records
                    SET betfair_odds_id_loser = :betfair_id
                    WHERE matchid = :atp_id
                """),
                {
                    'betfair_id': match['betfair_record_id'],
                    'atp_id': match['atp_matchid']
                }
            )
        
        conn.commit()
    
    print("Exact odds matching complete")

def main():
    print("Adding Betfair odds columns to matched_atp_records table...")
    add_betfair_odds_columns()
    
    print("Finding exact matches between ATP matches and Betfair odds...")
    match_exact_odds()
    
    print("Process completed successfully!")

if __name__ == "__main__":
    main() 