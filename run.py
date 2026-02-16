"""
Main entrypoint for the Mario project.

Usage:
  python run.py general   Run update new forms (parallel, all CIKs)
  python run.py monitor  Run update new forms with priority CIKs in a loop (sleep 1 hour between runs)
"""

import sys
import subprocess
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
UPDATE_SCRIPT = "scripts.update_new_forms_parallel"
SLEEP_SECONDS = 3600  # 1 hour


def run_update(use_priority_ciks=False, extra_args=None):
    """Run the update_new_forms_parallel script with the given options."""
    cmd = [sys.executable, "-m", UPDATE_SCRIPT]
    if use_priority_ciks:
        cmd.append("--use-priority-ciks")
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(cmd, cwd=PROJECT_ROOT)


def main():
    if len(sys.argv) < 2:
        print("Usage: python run.py general | monitor")
        print("  general  - Run update new forms (parallel, all CIKs)")
        print("  monitor  - Run update new forms with priority CIKs, loop every 1 hour")
        sys.exit(1)

    mode = sys.argv[1].strip().lower()
    extra = sys.argv[2:] if len(sys.argv) > 2 else None

    if mode == "general":
        rc = run_update(use_priority_ciks=False, extra_args=extra)
        sys.exit(rc.returncode)

    if mode == "monitor":
        run_count = 0
        while True:
            run_count += 1
            print(f"\n{'='*60}")
            print(f"Monitor run #{run_count} (priority CIKs only)")
            print(f"{'='*60}\n")
            rc = run_update(use_priority_ciks=True, extra_args=extra)
            if rc.returncode != 0:
                print(f"Update exited with code {rc.returncode}")
            print(f"\nSleeping {SLEEP_SECONDS} seconds (1 hour) until next run...")
            time.sleep(SLEEP_SECONDS)

    print(f"Unknown mode: {mode}. Use 'general' or 'monitor'.")
    sys.exit(1)


if __name__ == "__main__":
    main()
