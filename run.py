"""
Main entrypoint for the Mario project.

Usage:
  python run.py general   Run update new forms (parallel, all CIKs)
  python run.py monitor  Run update new forms with priority CIKs in a loop (sleep 1 hour).
                         Uses a single Dropbox connection so you only auth once.
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SLEEP_SECONDS = 3600  # 1 hour


def main():
    if len(sys.argv) < 2:
        print("Usage: python run.py general | monitor")
        print("  general  - Run update new forms (parallel, all CIKs)")
        print("  monitor  - Run update new forms with priority CIKs, loop every 1 hour (single auth)")
        sys.exit(1)

    mode = sys.argv[1].strip().lower()
    extra = sys.argv[2:] if len(sys.argv) > 2 else []

    from scripts.update_new_forms_parallel import run, get_parser, APP_KEY, APP_SECRET, REFRESH_TOKEN

    parser = get_parser()

    if mode == "general":
        args = parser.parse_args(extra)
        run(args, dbx_manager=None)
        return

    if mode == "monitor":
        from utils.dropbox_ops import DropboxManager

        dbx_manager = DropboxManager(APP_KEY, APP_SECRET, REFRESH_TOKEN)
        run_count = 0
        while True:
            run_count += 1
            print(f"\n{'='*60}")
            print(f"Monitor run #{run_count} (priority CIKs only)")
            print(f"{'='*60}\n")
            args = parser.parse_args(["--use-priority-ciks"] + extra)
            run(args, dbx_manager=dbx_manager)
            print(f"\nSleeping {SLEEP_SECONDS} seconds (1 hour) until next run...")
            time.sleep(SLEEP_SECONDS)

    print(f"Unknown mode: {mode}. Use 'general' or 'monitor'.")
    sys.exit(1)


if __name__ == "__main__":
    main()
