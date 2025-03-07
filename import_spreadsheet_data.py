import subprocess
import sys

# Automatically detect the correct Python path
python_path = sys.executable  # This will always get the correct Python

scripts = [
    "db_import/import_td_atp.py",
    "db_import/import_ta_atp.py",
    "db_import/import_td_wta.py",
    "db_import/import_ta_wta.py",
]

for script in scripts:
    print(f"🚀 Running {script} using {python_path} ...")
    result = subprocess.run([python_path, script], capture_output=True, text=True)

    print(result.stdout)
    if result.stderr:
        print(f"⚠️ Error in {script}: {result.stderr}")
        break  # Stop if any script fails

print("✅ All scripts executed successfully!")
