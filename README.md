# Mario Project

Structured SEC 13F forms pipeline: fetch filings, parse info tables (XML/HTML/LLM), and upload parquet/CSV to Dropbox.

## Layout

```
mario_project/
├── config/          # Shared config (e.g. BigQuery dtypes)
├── utils/           # Dropbox, LLM parser, custom runners
├── core/            # ProcessACSN: per-accession fetch + parse + upload
├── scripts/         # Entrypoints
│   ├── get_all_parquet.py           # Refresh Dropbox parquet list CSV
│   ├── update_new_forms_parallel.py  # Update new forms (parallel)
│   └── run_updated_table.py         # CIK → 13F CSV upload to Dropbox
├── priority_ciks/   # Priority CIK JSON file(s) for --use-priority-ciks
├── run.py           # Main entry: run.py general | run.py monitor
├── requirements.txt
├── .env.example
└── README.md
```

## Setup

1. **Clone / enter project**
   ```bash
   cd mario_project
   ```

2. **Virtual env and install**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # Windows
   pip install -r requirements.txt
   ```

3. **Environment**
   - Copy `.env.example` to `.env`.
   - Set Dropbox credentials (and optionally `OPENAI_API_KEY` for LLM fallback).
   - Or set `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN` in `.env` and load in scripts if you switch to env-based config.

## Main script (run from `mario_project` root)

- **`python run.py general`** — Run update new forms (parallel, all CIKs). Single run.
- **`python run.py monitor`** — Run update new forms with **priority CIKs** in a loop; sleeps 1 hour between each run. Use Ctrl+C to stop.

Any extra arguments are passed through to the update script (e.g. `python run.py general --file-workers 8`).

## Scripts (run from `mario_project` root)

- **Refresh parquet list**  
  Scans Dropbox for parquet files and writes `dropbox_parquet_list.csv`:
  ```bash
  python -m scripts.get_all_parquet
  ```

- **Update new forms (parallel)**  
  Uses the parquet list to skip already-done accessions; processes CSVs in `/Nizar/forms_table` in parallel:
  ```bash
  python -m scripts.update_new_forms_parallel
  python -m scripts.update_new_forms_parallel --use-priority-ciks --file-workers 4 --record-workers 2 --log-csv run_log.csv
  python -m scripts.update_new_forms_parallel --use-priority-ciks --priority-ciks-file priority_ciks/my_list.json
  python -m scripts.update_new_forms_parallel --skip-refresh-parquet-list
  ```
  By default, priority CIKs are read from `priority_ciks/priority_ciks_feb.json` (JSON array of CIK numbers). Override with `--priority-ciks-file`.

- **Run updated table**  
  Loads CIKs from `cik_chunks/*.json` (files whose name contains `update`), fetches SEC submissions, filters 13F, uploads per-CIK CSV to Dropbox `/Nizar/forms_table/`:
  ```bash
  python -m scripts.run_updated_table
  python -m scripts.run_updated_table --output-prefix my_run
  ```
  Ensure a `cik_chunks` directory exists under `mario_project` with the expected JSON files.

## Utilities

- **utils.dropbox_ops**: `DropboxManager` — list, download, upload (stream), parquet inventory.
- **utils.llm_parser**: SEC 13F table extraction via LLM (used when XML/HTML parsers fail). Supports **OpenAI** (default) or **Gemini**: set `LLM_PARSER_MODEL=gemini` and `GEMINI_API_KEY` in `.env` to use the Gemini API; optional `GEMINI_MODEL` (default `gemini-1.5-flash`).
- **utils.custom_runners**: `CRunner` for running agents with structured (Pydantic) output.
- **core.runner_acsn**: `ProcessACSN(record, dbx_manager)` — one accession: fetch, parse (parser1/2/3 or LLM), save parquet to Dropbox.

## Config

- **config**: `bq_dtype` and shared constants used by `core.runner_acsn` for building the output parquet schema.

## Credentials

- Dropbox: create an app in the [Dropbox App Console](https://www.dropbox.com/developers/apps), use App key and secret; on first run without a refresh token, the scripts will prompt for OAuth.
- **OpenAI**: required for the LLM parser when using default provider; set `OPENAI_API_KEY` in `.env`.
- **Gemini**: optional; set `LLM_PARSER_MODEL=gemini`, `GEMINI_API_KEY`, and optionally `GEMINI_MODEL` (e.g. `gemini-1.5-flash`) in `.env` to use Google’s Gemini API for the parser fallback.
