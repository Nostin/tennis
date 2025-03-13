import subprocess
import sys

# Debug: Print which Python is being used
print(f"Using Python interpreter: {sys.executable}")

# List of scripts to run in order
scripts = [
    "db_analysis/atp_create_ratings.py",
    "db_analysis/atp_head_to_head.py",
    "db_analysis/atp_rolling_win_pct.py",
    "db_analysis/atp_assess_fatigue.py",
    "db_analysis/atp_serve_hold_break_pct.py",
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
