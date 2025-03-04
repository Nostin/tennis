import subprocess

# List of scripts to run in order
scripts = [
    "strict_match_atp_initial.py",
    "strict_match_atp_trn.py",
    "strict_match_atp_loc.py",
    "strict_match_atp.py",
    "strict_match_atp_second.py",  # First pass with stricter matching
    "strict_match_atp_third.py"  # Second pass to fill remaining NULLs
]

for script in scripts:
    print(f"🚀 Running {script}...")
    result = subprocess.run(["python3", script], capture_output=True, text=True)
    
    # Print output from the script
    print(result.stdout)
    
    # Print errors if any
    if result.stderr:
        print(f"⚠️ Error in {script}: {result.stderr}")
        break  # Stop if any script fails

print("✅ All scripts executed successfully!")
