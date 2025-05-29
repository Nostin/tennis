import subprocess
import sys

# Debug: Print which Python is being used
print(f"Using Python interpreter: {sys.executable}")

# List of scripts to run in order
scripts = [
    "db_match_records/atp_initial_strict_matching.py",
    "db_match_records/atp_matching_looser_tournaments.py",
    "db_match_records/atp_matching_looser_location.py",
    "db_match_records/atp_matching_looser.py",
    "db_match_records/atp_match_name_location.py",
    "db_match_records/atp_match_name_tournament.py",
    "db_match_records/atp_final_manual_matches.py",
    "db_match_records/atp_mark_busted_records.py"
]

for script in scripts:
    print(f"🚀 Running {script} using {sys.executable} ...")
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)

    # Print output from the script
    print(result.stdout)

    # Print errors if any
    if result.stderr:
        print(f"⚠️ Error in {script}: {result.stderr}")
        break  # Stop if any script fails

print("✅ All scripts executed successfully!")
