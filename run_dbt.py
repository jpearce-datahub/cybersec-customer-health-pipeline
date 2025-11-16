import subprocess
import os

os.chdir('dbt')
result = subprocess.run(['python', '-c', 'import dbt.cli.main; dbt.cli.main.cli()', 'run', '--profiles-dir', '.'], 
                       capture_output=True, text=True)
print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)