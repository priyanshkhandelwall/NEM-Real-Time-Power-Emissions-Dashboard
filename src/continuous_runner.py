"""
COMP5339 Assignment 2 – Task 5: Continuous Execution Runner
Team: Priyansh Khandelwal, Zhenzhe Wu
"""

import time
import subprocess
import sys
import os

TASK_SCRIPTS = [
    "data_pipeline.py",
    "data_integration.py",
    "mqtt_publisher.py",
]

CYCLE_DELAY_SECONDS = 60

def run_script(script_name):
    print(f"\n{'='*50}")
    print(f"STARTING: {script_name}")
    print(f"{'='*50}")
    try:
        env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=env
        )
        print(f"SUCCESS: {script_name} completed.")
    except subprocess.CalledProcessError as e:
        print(f"ERROR: {script_name} failed with code {e.returncode}")
        print(f"--- STDERR ---\n{e.stderr}")
        print(f"--- STDOUT ---\n{e.stdout}")
        print("Skipping to next cycle...")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False
    return True

def continuous_execution_loop():
    cycle_count = 1
    while True:
        print(f"\n\n{'#'*60}")
        print(f"STARTING CYCLE #{cycle_count}")
        print(f"{'#'*60}")

        success = True
        for script in TASK_SCRIPTS:
            if not run_script(script):
                success = False
                break

        print(f"\n{'='*50}")
        if success:
            print(f"CYCLE #{cycle_count} COMPLETE. Waiting {CYCLE_DELAY_SECONDS}s...")
        else:
            print(f"CYCLE #{cycle_count} FAILED. Retrying in {CYCLE_DELAY_SECONDS}s...")
        print(f"{'='*50}")
        time.sleep(CYCLE_DELAY_SECONDS)
        cycle_count += 1

if __name__ == "__main__":
    try:
        continuous_execution_loop()
    except KeyboardInterrupt:
        print("\n\nStopped by user (Ctrl+C).")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nCRITICAL FAILURE: {e}")
        sys.exit(1)