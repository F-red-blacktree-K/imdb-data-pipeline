# IMDb Data Pipeline

Build a small data pipeline using Python:

1. Obtain IMDb data
2. Clean and merge
3. Write to SQLite
4. Output analysis reports

## Tech Stack

- Python
- requests / tqdm / pandas / sqlite3
- Git

## Project Structure

- `download_datasets.py`: Download IMDb datasets with optional preview output
- `data/`: Data directory
- `output/`: Export results

## Quick Start

```bash
pip install -r requirements.txt
python download_datasets.py
```

Default behavior:
- download all datasets (`--dataset all`)
- skip existing files unless `--force` is provided
- do not preview rows (`--preview 0`)

Optional example:

```bash
python download_datasets.py --preview 3
```

## Downloader Arguments

- `--dataset`: `basics` / `ratings` / `all` (default: `all`)
- `--out-dir`: download directory (default: `data/raw`)
- `--preview`: print first N rows after download (default: `0`)
- `--force`: re-download even if files already exist

## Notes

- Downloaded files are saved to your local disk (default: `data/raw/`), not temp memory.
- Running the script again will skip existing files unless you pass `--force`.
