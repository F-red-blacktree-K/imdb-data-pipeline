# IMDb Data Pipeline

A reproducible IMDb data pipeline focused on dataset-based analysis, with crawler-based analysis as an independent secondary track.

## Project Overview

This project has two tracks:

1. Dataset Pipeline (primary)
- `download -> clean_merge -> analyze_core -> (next) load_sqlite -> export reports`
- Primary unbiased analysis based on official IMDb datasets.

2. Crawl Pipeline (secondary)
- `crawl_topn -> clean_crawl -> analyze_topn` (independent)
- Targeted enrichment / high-rating subgroup study.

Dataset analysis is the primary unbiased analysis.
Crawler analysis is a targeted enrichment/high-rating subgroup study.

## Tech Stack

- Python 3
- pandas, matplotlib
- requests, tqdm, beautifulsoup4
- sqlite3 (planned next step)
- Git

## Project Structure

- `download_datasets.py`: Download IMDb dataset files (`basics`, `ratings`) with skip/force behavior.
- `clean_merge.py`: EDA summary + clean + merge `basics` and `ratings` by `tconst`.
- `analyze_core.py`: Generate core analysis CSV reports and PNG charts.
- `crawl_ldjson.py`: Crawl IMDb title pages and extract `application/ld+json` (secondary track).
- `DATA_DICTIONARY.md`: Field definitions and output semantics.

## Quick Start

```bash
pip install -r requirements.txt
python download_datasets.py
python clean_merge.py --force
python analyze_core.py
```

## Outputs

- Core merged dataset:
- `data/processed/movies_merged.csv`

- EDA / run summary:
- `data/reports/eda_summary.json`
- `data/reports/merged_preview.csv`
- `data/reports/run_summary.json`

- Analysis outputs:
- `data/analysis/*.csv`
- `data/analysis/*.png`

- Crawl outputs (secondary track):
- `data/staging/crawl_ldjson.jsonl`
- `data/reports/crawl_summary.json`

## Notes

- `--force`: rerun and overwrite existing outputs.
- Without `--force`, scripts can skip existing outputs to save time.
- Generated data files under `data/processed`, `data/analysis`, `data/reports`, `data/staging` are local artifacts and should not be committed.

## Next Steps

- Add SQLite loading (`load_sqlite.py`) and SQL-based report export.
- Add robustness checks (full vs high-confidence subsets) for all core analyses.