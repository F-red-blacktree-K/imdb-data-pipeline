# IMDb Data Pipeline

A reproducible IMDb data pipeline focused on dataset-based analysis, with crawler-based analysis as an independent secondary track.

## Project Overview

This project has two tracks:

1. Dataset Pipeline (primary)
- `download -> clean_merge -> analyze_core -> (next) load_sqlite -> export reports`
- Primary unbiased analysis based on official IMDb datasets.

2. Crawl Pipeline (secondary)
- `crawl_top1000 -> clean_crawl -> analyze_top1000` (independent)
- Targeted analysis for high-rated titles to study why they are well-received.

Dataset analysis is the primary unbiased analysis.
Crawler analysis is a targeted high-rating subgroup study.

## Tech Stack

- Python 3
- pandas, matplotlib
- requests, tqdm, beautifulsoup4
- sqlite3 (planned next step)
- Git

## Project Structure

- `download_datasets.py`: Download IMDb dataset files (`basics`, `ratings`) with skip/force behavior.
- `clean_merge.py`: EDA summary + clean + merge `basics` and `ratings` by `tconst`.
- `analyze_core.py`: Generate core analysis outputs, including full-sample vs high-confidence subset robustness comparisons.
- `crawl_ldjson.py`: Crawl IMDb title pages and extract `application/ld+json` (secondary track).
- `DATA_DICTIONARY.md`: Field definitions and output semantics.
- `INTERPRETATION.md`: How to read each chart and apply robustness flags.

## Quick Start

```bash
pip install -r requirements.txt
python download_datasets.py
python clean_merge.py --force
python analyze_core.py
```

## Outputs

Generated locally (not committed):

- Core merged dataset:
- `data/processed/movies_merged.csv`

- EDA / run summary:
- `data/reports/eda_summary.json`
- `data/reports/merged_preview.csv`
- `data/reports/run_summary.json`

- Analysis CSV outputs:
- `data/analysis/genre_rating_summary.csv`
- `data/analysis/decade_summary.csv`
- `data/analysis/runtime_bin_summary.csv`
- `data/analysis/votes_bin_summary.csv`
- `data/analysis/decade_genre_top_count.csv`
- `data/analysis/decade_genre_top_rating.csv`
- `data/analysis/subset_comparison.csv`
- `data/analysis/robustness_summary.csv`

- Analysis chart outputs:
- `data/analysis/chart_genre_rating_compare.png`
- `data/analysis/chart_decade_rating_compare.png`
- `data/analysis/chart_runtime_rating_compare.png`
- `data/analysis/chart_votes_bin_rating_compare.png`
- `data/analysis/chart_decade_genre_heatmap.png`
- `data/analysis/chart_subset_comparison.png`

## Notes

- `--force`: rerun and overwrite existing outputs.
- Without `--force`, scripts can skip existing outputs to save time.
- High-confidence subset in current analysis defaults to `numVotes >= 1000`.
- Generated data files under `data/processed`, `data/analysis`, `data/reports`, `data/staging` are local artifacts and should not be committed.

## Next Steps

- Build the crawler phase for Top 1000 high-rated titles.
- Clean and structure crawler outputs for secondary-track analysis.
- Analyze why top-rated titles are well-received (genre patterns, era effects, runtime and vote behavior).
- Load finalized outputs into SQLite and export SQL-based reports.