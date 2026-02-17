"""
Parallel update of new forms from Dropbox forms_table.
- Optionally runs get_all_parquet to refresh dropbox_parquet_list.csv.
- Processes multiple CSV files and records in parallel.
- Optional: restrict to priority CIKs (--use-priority-ciks).
- Logs each record to a CSV (--log-csv).

Run from project root: python -m scripts.update_new_forms_parallel [options]
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import argparse
import io
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd

from utils.dropbox_ops import DropboxManager
from core.runner_acsn import ProcessACSN
from scripts.get_all_parquet import refresh_parquet_list

# --- CONFIGURATION ---
APP_KEY = "dtm7p8v46wtwjh7"
APP_SECRET = "ocp7hvlybeyoyqg"
REFRESH_TOKEN = None
# Default: priority_ciks folder in project root
PRIORITY_CIKS_DIR = _root / "priority_ciks"
PRIORITY_CIKS_DEFAULT_FILE = "priority_ciks_feb.json"
DROPBOX_LIST_CSV = "dropbox_parquet_list.csv"
TARGET_FOLDER = "/Nizar/forms_table"
DEFAULT_LOG_CSV = "update_forms_run_log.csv"


def load_existing_acsns():
    try:
        df_existing = pd.read_csv(DROPBOX_LIST_CSV)
        df_existing["acsn"] = df_existing["file_name"].map(
            lambda x: "-".join(x.split("_")[1:-3])
        )
        return set(df_existing["acsn"].astype(str).unique())
    except FileNotFoundError:
        print("Warning: dropbox_parquet_list.csv not found. Processing all records.")
        return set()


def get_priority_ciks_path(custom_path=None):
    """Resolve priority CIKs file: --priority-ciks-file, or priority_ciks/<default> in project root."""
    if custom_path is not None:
        p = Path(custom_path)
        return p if p.is_absolute() else (_root / p)
    return PRIORITY_CIKS_DIR / PRIORITY_CIKS_DEFAULT_FILE


def load_priority_ciks(path=None):
    path = path or get_priority_ciks_path()
    path = Path(path) if not isinstance(path, Path) else path
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {str(c).zfill(10) for c in data} if data else set()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Warning: Could not load priority CIKs from {path}: {e}")
        return set()


def process_one_file(file_info, existing_acsns, dbx_manager, record_workers):
    path_display = file_info["path_display"]
    file_name = file_info["file_name"]
    metadata, response = dbx_manager.download_file(path_display)
    if not response:
        return {"file": file_name, "ok": False, "error": "download failed", "n_processed": 0}
    try:
        with response:
            df = pd.read_csv(io.BytesIO(response.content))
    except Exception as e:
        return {"file": file_name, "ok": False, "error": str(e), "n_processed": 0}
    df["accessionNumber"] = df["accessionNumber"].astype(str)
    df_filter = df[~df["accessionNumber"].isin(existing_acsns)]
    if df_filter.empty:
        return {"file": file_name, "ok": True, "skipped": True, "n_processed": 0}
    records = df_filter.to_dict("records")

    def run_one_record(record):
        cik = str(record.get("cik", "")).zfill(10)
        acsn = str(record.get("accessionNumber", ""))
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "cik": cik,
            "accession_number": acsn,
            "source_file": file_name,
            "status": "",
            "error_message": "",
            "timestamp": ts,
        }
        try:
            processor = ProcessACSN(record, dbx_manager)
            processor.run()
            log_entry["status"] = "success"
            return None, log_entry
        except Exception as e:
            log_entry["status"] = "error"
            log_entry["error_message"] = str(e)[:500]
            return str(e), log_entry

    log_entries = []
    errors = []
    n_ok = 0
    if record_workers <= 1:
        for record in records:
            err, log_entry = run_one_record(record)
            log_entries.append(log_entry)
            if err:
                errors.append(err)
            else:
                n_ok += 1
    else:
        with ThreadPoolExecutor(max_workers=record_workers) as pool:
            futures = {pool.submit(run_one_record, r): r for r in records}
            for fut in as_completed(futures):
                err, log_entry = fut.result()
                log_entries.append(log_entry)
                if err:
                    errors.append(err)
                else:
                    n_ok += 1
    return {
        "file": file_name,
        "ok": len(errors) == 0,
        "n_processed": n_ok,
        "n_total": len(records),
        "errors": errors[:5],
        "log_entries": log_entries,
    }


def get_parser():
    """Build the argument parser (so run.py or others can reuse it)."""
    parser = argparse.ArgumentParser(
        description="Update new forms from Dropbox forms_table (parallel)."
    )
    parser.add_argument(
        "--use-priority-ciks",
        action="store_true",
        help="Process only CIKs listed in priority_ciks/priority_ciks_feb.json (or --priority-ciks-file)",
    )
    parser.add_argument(
        "--priority-ciks-file",
        type=str,
        default=None,
        metavar="PATH",
        help="Path to priority CIKs JSON file (default: priority_ciks/priority_ciks_feb.json)",
    )
    parser.add_argument(
        "--file-workers",
        type=int,
        default=4,
        metavar="N",
        help="Number of CSV files to process in parallel (default: 4)",
    )
    parser.add_argument(
        "--record-workers",
        type=int,
        default=1,
        metavar="N",
        help="Number of records to process in parallel per file (default: 1)",
    )
    parser.add_argument(
        "--log-csv",
        type=str,
        default=DEFAULT_LOG_CSV,
        metavar="PATH",
        help=f"CSV file to log each record result (default: {DEFAULT_LOG_CSV})",
    )
    parser.add_argument(
        "--skip-refresh-parquet-list",
        action="store_true",
        help="Skip running parquet list refresh before main",
    )
    return parser


def run(args, dbx_manager=None):
    """
    Run the update logic. If dbx_manager is None, create one (single auth).
    Call this with a shared dbx_manager from run.py monitor to avoid re-auth each loop.
    """
    if dbx_manager is None:
        dbx_manager = DropboxManager(APP_KEY, APP_SECRET, REFRESH_TOKEN)

    if not args.skip_refresh_parquet_list:
        print("Updating dropbox parquet list...")
        files = refresh_parquet_list(dbx_manager, output_csv=DROPBOX_LIST_CSV)
        if files:
            print(f"Dropbox parquet list updated ({len(files)} files).\n")
        else:
            print("Dropbox parquet list updated (no files or error).\n")

    existing_acsns = load_existing_acsns()
    priority_ciks = None
    if args.use_priority_ciks:
        priority_ciks = load_priority_ciks(path=get_priority_ciks_path(args.priority_ciks_file))
        if not priority_ciks:
            print("No priority CIKs loaded; exiting.")
            return
        print(f"Using priority CIKs only ({len(priority_ciks)} CIKs).")

    all_files = dbx_manager.get_all_files_metadata(TARGET_FOLDER, recursive=False)
    csv_files = [f for f in all_files if f["file_name"].lower().endswith(".csv")]
    if priority_ciks is not None:
        csv_files = [
            f for f in csv_files
            if str(f["file_name"].split(".")[0]).zfill(10) in priority_ciks
        ]
        print(f"Filtered to {len(csv_files)} CSV files (priority CIKs).")
    else:
        print(f"Found {len(csv_files)} CSV files (all CIKs).")
    if not csv_files:
        print("No files to process.")
        return

    total_processed = 0
    total_errors = 0
    all_log_entries = []
    with ThreadPoolExecutor(max_workers=args.file_workers) as executor:
        futures = {
            executor.submit(
                process_one_file,
                file_info,
                existing_acsns,
                dbx_manager,
                args.record_workers,
            ): file_info["file_name"]
            for file_info in csv_files
        }
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                result = fut.result()
                total_processed += result.get("n_processed", 0)
                if result.get("errors"):
                    total_errors += len(result["errors"])
                all_log_entries.extend(result.get("log_entries", []))
                if result.get("skipped"):
                    print(f"  [skip] {result['file']}: already up to date")
                else:
                    print(
                        f"  [done] {result['file']}: "
                        f"{result.get('n_processed', 0)}/{result.get('n_total', 0)} processed"
                    )
                    for err in result.get("errors", []):
                        print(f"    error: {err}")
            except Exception as e:
                print(f"  [fail] {name}: {e}")
                total_errors += 1

    if all_log_entries:
        log_df = pd.DataFrame(all_log_entries)
        log_df.to_csv(args.log_csv, index=False)
        print(f"\nLog written to {args.log_csv} ({len(all_log_entries)} rows).")
    print(f"\nTotal processed: {total_processed}; errors: {total_errors}")


def main(argv=None):
    """Entrypoint when run as script. argv=None uses sys.argv."""
    parser = get_parser()
    args = parser.parse_args(argv)
    run(args, dbx_manager=None)


if __name__ == "__main__":
    main()
