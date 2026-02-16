"""
Fetch SEC 13F filings per CIK from cik_chunks, upload CSVs to Dropbox forms_table.
Run from project root: python -m scripts.run_updated_table
Input: CIK list is loaded from cik_chunks/*.json files (those with 'update' in the name).
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import json
import os
import argparse
import io
import time
import pandas as pd
import requests
from requests.exceptions import Timeout, RequestException

from utils.dropbox_ops import DropboxManager

# --- CONFIGURATION ---
EMAILS = [
    "nizar.rizax@gmail.com",
    "project.mario.1@example.com",
]
REQUEST_TIMEOUT = 30
APP_KEY = "dtm7p8v46wtwjh7"
APP_SECRET = "ocp7hvlybeyoyqg"
REFRESH_TOKEN = None

# Use project root for cik_chunks (relative to mario_project folder)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CIK_CHUNKS_DIR = PROJECT_ROOT / "cik_chunks"


def load_input_csv():
    """Load CIK list from cik_chunks/*.json files that contain 'update' in name."""
    if not CIK_CHUNKS_DIR.exists():
        raise FileNotFoundError(f"Directory not found: {CIK_CHUNKS_DIR}")
    all_result = []
    for c in os.listdir(CIK_CHUNKS_DIR):
        if "update" in c and c.endswith(".json"):
            path = CIK_CHUNKS_DIR / c
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                all_result += data
            except Exception as e:
                print(f"Failed {c}: {e}")
    df_cik = pd.DataFrame(all_result)
    if df_cik.empty:
        return df_cik
    if "13f_rows" in df_cik.columns:
        df_cik = df_cik[df_cik["13f_rows"] != 0]
    return df_cik


def get_headers(email):
    return {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9,ko;q=0.8,id;q=0.7,de;q=0.6,de-CH;q=0.5",
        "origin": "https://www.sec.gov",
        "priority": "u=1, i",
        "referer": "https://www.sec.gov/",
        "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": f"Mario bot project {email}",
    }


def upload_dataframe_to_dropbox(dbx_handler, df, cik_padded):
    if df.empty:
        return False
    dropbox_path = f"/Nizar/forms_table/{cik_padded}.csv"
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")
        meta = dbx_handler.upload_stream(csv_bytes, dropbox_path)
        return meta is not None
    except Exception as e:
        print(f"  [Dropbox Error] Failed to upload {cik_padded}: {e}")
        return False


def process_cik_item(item, emails, dbx_handler):
    original_name = item.get("name", "Unknown")
    cik_raw = item.get("cik")
    if pd.isna(cik_raw):
        return {"error": "CIK is NaN"}, False
    cik_padded = str(int(cik_raw)).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    last_error = "Unknown Error"
    for attempt in range(3):
        email = emails[attempt % len(emails)]
        headers = get_headers(email)
        try:
            time.sleep(1.1)
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                form_result = response.json()
                business_address = ""
                mailing_address = ""
                try:
                    business_address = ", ".join(
                        [i for i in list(form_result["addresses"]["business"].values()) if i]
                    )
                except Exception:
                    pass
                try:
                    mailing_address = ", ".join(
                        [i for i in list(form_result["addresses"]["mailing"].values()) if i]
                    )
                except Exception:
                    pass
                df_13f = pd.DataFrame()
                if "filings" in form_result and "recent" in form_result["filings"]:
                    df_all = pd.DataFrame(form_result["filings"]["recent"])
                    df_all["cik"] = cik_raw
                    df_all["business_address"] = business_address
                    df_all["mailing_address"] = mailing_address
                    if not df_all.empty and "form" in df_all.columns:
                        df_13f = df_all[df_all["form"].str.contains("13F", na=False)].copy()
                if not df_13f.empty:
                    upload_success = upload_dataframe_to_dropbox(dbx_handler, df_13f, cik_padded)
                    status_msg = "success + uploaded" if upload_success else "success + upload_failed"
                else:
                    status_msg = "success + no_13f_found"
                return {
                    "name": original_name,
                    "cik": cik_padded,
                    "13f_count": len(df_13f),
                    "status": status_msg,
                }, True
            elif response.status_code == 429:
                print(f"[{cik_padded}] Rate limited by SEC. Sleeping 10s...")
                time.sleep(10)
            else:
                last_error = f"HTTP {response.status_code}"
        except (Timeout, RequestException) as e:
            last_error = str(e)
        print(f"[{cik_padded}] Attempt {attempt+1} failed: {last_error}")
        time.sleep(2)
    return {"name": original_name, "cik": cik_raw, "error": last_error}, False


def main():
    parser = argparse.ArgumentParser(
        description="Fetch SEC 13F data for CIKs from cik_chunks and upload to Dropbox."
    )
    parser.add_argument(
        "--output-prefix",
        type=str,
        default="run",
        help="Prefix for output JSON files (default: run)",
    )
    args = parser.parse_args()

    try:
        df_input = load_input_csv()
        if df_input.empty:
            print("No CIK data found in cik_chunks.")
            return
        if "cik" not in df_input.columns:
            print("Error: Data must have a 'cik' column.")
            return
        data_to_process = df_input.to_dict("records")
    except Exception as e:
        print(f"Failed to read input: {e}")
        return

    output_success = f"{args.output_prefix}_results.json"
    output_failed = f"{args.output_prefix}_failed.json"
    success_list = []
    failed_list = []

    dbx_handler = DropboxManager(APP_KEY, APP_SECRET, REFRESH_TOKEN)
    print(f"Starting process for {len(data_to_process)} CIKs...")

    try:
        for i, item in enumerate(data_to_process):
            result, is_success = process_cik_item(item, EMAILS, dbx_handler)
            if is_success:
                success_list.append(result)
            else:
                failed_list.append(result)
            if i % 5 == 0:
                print(f"Progress: {i}/{len(data_to_process)} | Success: {len(success_list)} | Failed: {len(failed_list)}")
            with open(output_success, "w", encoding="utf-8") as f:
                json.dump(success_list, f, indent=4)
    except KeyboardInterrupt:
        print("\n[!] Process interrupted. Progress saved.")

    if failed_list:
        with open(output_failed, "w", encoding="utf-8") as f:
            json.dump(failed_list, f, indent=4)
        print(f"Saved {len(failed_list)} failures to {output_failed}")
    print(f"Finished. Total processed: {len(success_list)}. Check {output_success} for details.")


if __name__ == "__main__":
    main()
