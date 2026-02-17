import dropbox
import dropbox.files
from dropbox import DropboxOAuth2FlowNoRedirect
from dropbox.exceptions import ApiError, AuthError
import os
import csv
import io


class DropboxManager:
    """
    A wrapper class for Dropbox API v2 operations.
    Covers: CRUD, Listing (Recursive & Non-Recursive), and Stream Uploading.
    Auto-handles Authentication using Refresh Tokens.
    """

    def __init__(self, app_key, app_secret, refresh_token=None):
        """
        Initialize connection.
        If refresh_token is missing, it initiates the OAuth flow to generate one.
        """
        self.app_key = app_key
        self.app_secret = app_secret

        # 1. If no token provided, get one via user interaction
        if not refresh_token:
            print("\n[!] No Refresh Token provided. Starting First-Run Authorization...")
            refresh_token = self._authorize_interactive()

        self.refresh_token = refresh_token

        try:
            # 2. Connect using the Refresh Token
            self.dbx = dropbox.Dropbox(
                app_key=self.app_key,
                app_secret=self.app_secret,
                oauth2_refresh_token=self.refresh_token,
            )

            # 3. Verify connection
            account = self.dbx.users_get_current_account()
            print(f"Connected to Dropbox as: {account.name.display_name}")

        except AuthError as e:
            print("Error: Authentication Failed. Your Refresh Token might be invalid.")
            print("Try running without a refresh token to generate a new one.")
            raise e

    def _authorize_interactive(self):
        """Helper to handle the OAuth handshake inside the script."""
        auth_flow = DropboxOAuth2FlowNoRedirect(
            self.app_key,
            self.app_secret,
            token_access_type="offline",
        )

        authorize_url = auth_flow.start()

        print(f"\n{'='*60}")
        print("AUTHORIZATION REQUIRED")
        print(f"{'='*60}")
        print("1. Go to this URL: \n", authorize_url)
        print("2. Click 'Allow' (Log in if needed).")
        print("3. Copy the code starting with 'z...' and paste it below.")
        print(f"{'='*60}\n")

        auth_code = input("Enter Auth Code: ").strip()

        try:
            oauth_result = auth_flow.finish(auth_code)
            new_token = oauth_result.refresh_token
            print("\n[SUCCESS] New Refresh Token Generated!")
            print(f"SAVE THIS TOKEN: {new_token}")
            print("(Pass this token next time to skip this manual step)\n")
            return new_token
        except Exception as e:
            print(f"Authorization failed: {e}")
            raise e

    # ==========================================
    # 1. LISTING & METADATA (READ)
    # ==========================================

    def get_all_files_metadata(self, folder_path, recursive=True):
        """Retrieves metadata for ALL files recursively."""
        print(f"Recursively scanning folder: {folder_path}...")
        files_data = []

        try:
            if folder_path == "/" or folder_path == ".":
                folder_path = ""

            result = self.dbx.files_list_folder(folder_path, recursive=recursive)

            def process_entries(entries):
                for entry in entries:
                    if isinstance(entry, dropbox.files.FileMetadata):
                        path_parts = entry.path_display.split("/")
                        folder_name = path_parts[-2] if len(path_parts) > 1 else ""

                        files_data.append({
                            "folder_name": folder_name,
                            "file_name": entry.name,
                            "path_display": entry.path_display,
                            "size_bytes": entry.size,
                            "server_modified": entry.server_modified.isoformat(),
                            "id": entry.id,
                        })

            process_entries(result.entries)

            while result.has_more:
                print(f"Fetching more items... (Current count: {len(files_data)})")
                result = self.dbx.files_list_folder_continue(result.cursor)
                process_entries(result.entries)

            print(f"Scan complete. Found {len(files_data)} files.")
            return files_data

        except ApiError as e:
            print(f"Error listing folder {folder_path}: {e}")
            return []

    def save_metadata_to_csv(self, folder_path, output_csv="dropbox_files.csv"):
        """Wraps get_all_files_metadata and saves to CSV."""
        data = self.get_all_files_metadata(folder_path)
        if not data:
            print("No data to save.")
            return

        fieldnames = ["folder_name", "file_name", "size_bytes", "server_modified", "path_display", "id"]
        try:
            with open(output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            print(f"Metadata saved to: {output_csv}")
        except IOError as e:
            print(f"Error saving CSV: {e}")

    # ==========================================
    # 2. OPERATIONS (Create, Upload, etc.)
    # ==========================================

    def create_folder(self, dropbox_path):
        try:
            meta = self.dbx.files_create_folder_v2(dropbox_path)
            return meta
        except ApiError as e:
            if (
                isinstance(e.error, dropbox.files.CreateFolderError)
                and e.error.is_path()
                and e.error.get_path().is_conflict()
            ):
                return None
            print(f"Folder creation issue: {e}")
            return None

    def download_file(self, dropbox_path):
        """
        Downloads a file and returns the metadata and the raw response.
        The response object can be read directly into Pandas.
        """
        try:
            metadata, response = self.dbx.files_download(dropbox_path)
            return metadata, response
        except ApiError as e:
            print(f"Error downloading {dropbox_path}: {e}")
            return None, None

    def upload_stream(self, file_bytes, dropbox_path):
        try:
            if hasattr(file_bytes, "read"):
                data = file_bytes.read()
            else:
                data = file_bytes

            meta = self.dbx.files_upload(
                data, dropbox_path, mode=dropbox.files.WriteMode("overwrite")
            )
            return meta
        except ApiError as e:
            print(f"Stream upload failed: {e}")
            return None

    # Max entries per list_folder request (Dropbox API cap)
    LIST_FOLDER_LIMIT = 2000
    CSV_WRITE_BATCH_SIZE = 500
    PROGRESS_PRINT_INTERVAL = 10000

    def get_parquet_files(self, folder_path, output_csv="parquet_inventory.csv"):
        """
        Retrieves .parquet files and saves them to CSV. Uses max page size for
        fewer API round trips and batched writes for better performance.
        """
        print(f"Scanning for Parquet files in: {folder_path}...")
        print(f"Progress will be saved to: {output_csv}")

        parquet_files = []
        total_items_checked = 0
        fieldnames = ["folder_name", "file_name", "size_bytes", "server_modified", "path_display", "id"]

        try:
            if folder_path == "/" or folder_path == ".":
                folder_path = ""

            with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                batch = []

                def flush_batch():
                    if batch:
                        writer.writerows(batch)
                        csvfile.flush()
                        batch.clear()

                def process_entries(entries):
                    nonlocal total_items_checked
                    for entry in entries:
                        total_items_checked += 1
                        if total_items_checked % self.PROGRESS_PRINT_INTERVAL == 0 and total_items_checked > 0:
                            print(f"--- Scanned {total_items_checked} items, {len(parquet_files)} parquet so far ---")

                        if isinstance(entry, dropbox.files.FileMetadata):
                            if entry.name.lower().endswith(".parquet"):
                                path_parts = entry.path_display.split("/")
                                folder_name = path_parts[-2] if len(path_parts) > 1 else ""

                                row = {
                                    "folder_name": folder_name,
                                    "file_name": entry.name,
                                    "path_display": entry.path_display,
                                    "size_bytes": entry.size,
                                    "server_modified": entry.server_modified.isoformat(),
                                    "id": entry.id,
                                }

                                parquet_files.append(row)
                                batch.append(row)
                                if len(batch) >= self.CSV_WRITE_BATCH_SIZE:
                                    flush_batch()

                result = self.dbx.files_list_folder(
                    folder_path, recursive=True, limit=self.LIST_FOLDER_LIMIT
                )
                process_entries(result.entries)

                while result.has_more:
                    result = self.dbx.files_list_folder_continue(result.cursor)
                    process_entries(result.entries)

                flush_batch()

            print(f"\n[FINISHED] Total items scanned: {total_items_checked}")
            print(f"[FINISHED] Parquet files found: {len(parquet_files)}")
            print(f"[FINISHED] CSV file finalized: {output_csv}")
            return parquet_files

        except ApiError as e:
            print(f"Error scanning for Parquet files: {e}")
            return parquet_files
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return parquet_files
