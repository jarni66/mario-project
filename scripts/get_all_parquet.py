"""
Refresh the list of parquet files in Dropbox and save to CSV.
Run from project root: python -m scripts.get_all_parquet
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from utils.dropbox_ops import DropboxManager

# Configure or use env
APP_KEY = "dtm7p8v46wtwjh7"
APP_SECRET = "ocp7hvlybeyoyqg"
REFRESH_TOKEN = None
TARGET_FOLDER = ""
OUTPUT_FILE = "dropbox_parquet_list.csv"


def refresh_parquet_list(dbx_manager, folder_path=None, output_csv=None):
    """
    Scan Dropbox for parquet files and write the list to CSV.
    """
    folder_path = folder_path if folder_path is not None else TARGET_FOLDER
    output_csv = output_csv if output_csv is not None else OUTPUT_FILE
    files = dbx_manager.get_parquet_files(
        folder_path=folder_path,
        output_csv=output_csv,
    )
    return files or []


def main():
    try:
        dbx_manager = DropboxManager(APP_KEY, APP_SECRET, REFRESH_TOKEN)
        print("\n" + "=" * 50)
        print("STARTING PARQUET INVENTORY SCAN")
        print("=" * 50 + "\n")
        files = refresh_parquet_list(dbx_manager)
        if files:
            print(f"\nSuccess! Found {len(files)} files.")
            print(f"Your CSV is ready at: {OUTPUT_FILE}")
        else:
            print("\nNo Parquet files found or an error occurred.")
    except KeyboardInterrupt:
        print("\n[STOPPED] Script stopped by user. CSV contains data found up to this point.")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")


if __name__ == "__main__":
    main()
