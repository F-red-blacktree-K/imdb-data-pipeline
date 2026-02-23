# IMDb Data Pipeline

A reproducible IMDb data pipeline with a primary dataset track and a secondary crawler track.

## Project Overview

This project has two independent tracks:

1. Dataset Pipeline (primary)
- `download_datasets.py -> clean_merge.py -> analyze_core.py -> (next) load_sqlite -> export reports`
- Primary unbiased analysis based on official IMDb datasets.

2. Crawl Pipeline (secondary)
- `crawl_top1000.py -> clean_crawl.py -> analyze_top1000.py`
- Targeted analysis for high-rated titles to study why they are well-received.

Dataset analysis is the primary unbiased analysis.
Crawler analysis is a targeted high-rating subgroup study.

## Tech Stack

- Python 3
- pandas, matplotlib
- requests, tqdm, beautifulsoup4
- sqlite3 (planned)
- Git

## Project Structure

- `download_datasets.py`: Download IMDb dataset files (`basics`, `ratings`) with skip/force behavior.
- `clean_merge.py`: EDA summary + clean + merge `basics` and `ratings` by `tconst`.
- `analyze_core.py`: Core analysis outputs, including full-sample vs high-confidence robustness checks.
- `crawl_top1000.py`: Crawl IMDb Top1000 candidate list via GraphQL API.
- `clean_crawl.py`: Clean and standardize crawler output for secondary-track analysis.
- `analyze_top1000.py`: Advanced Top1000 winner-profile analysis.
- `run_pipeline.py`: One-command orchestrator for Stage1/Stage2/all.
- `DATA_DICTIONARY.md`: Field definitions and output semantics.
- `INTERPRETATION.md`: Chart interpretation guide for Stage 1 and Stage 2.

## Quick Start (Primary Track)

```bash
pip install -r requirements.txt
python download_datasets.py
python clean_merge.py --force
python analyze_core.py
```

## Quick Start (Secondary Track)

```bash
python crawl_top1000.py --force
python clean_crawl.py --force
python analyze_top1000.py
```


## One-Command Pipeline

```bash
python run_pipeline.py
```

Optional modes:

```bash
python run_pipeline.py --stage stage1
python run_pipeline.py --stage stage2
python run_pipeline.py --stage stage2 --enrich-stage2
```

## Outputs

Generated locally (not committed):

- Primary merged dataset:
  - `data/processed/stage1/movies_merged.csv`
- Primary reports:
  - `data/reports/stage1/eda_summary.json`
  - `data/reports/stage1/merged_preview.csv`
  - `data/reports/stage1/run_summary.json`
- Primary analysis:
  - `data/analysis/stage1/genre_rating_summary.csv`
  - `data/analysis/stage1/decade_summary.csv`
  - `data/analysis/stage1/runtime_bin_summary.csv`
  - `data/analysis/stage1/votes_bin_summary.csv`
  - `data/analysis/stage1/decade_genre_top_count.csv`
  - `data/analysis/stage1/decade_genre_top_rating.csv`
  - `data/analysis/stage1/subset_comparison.csv`
  - `data/analysis/stage1/robustness_summary.csv`

- Secondary crawl artifacts:
  - `data/staging/stage2/top1000_ids.csv`
  - `data/processed/stage2/top1000_clean.csv`
  - `data/reports/stage2/top1000_fetch_summary.json`
  - `data/reports/stage2/top1000_clean_summary.json`
  - `data/reports/stage2/top1000_analysis_summary.json`

- Secondary analysis:
  - `data/analysis/stage2/top1000_enriched.csv`
  - `data/analysis/stage2/top1000_genre_structure.csv`
  - `data/analysis/stage2/top1000_genre_penetration.csv`
  - `data/analysis/stage2/top1000_decade_genre_hotspots.csv`
  - `data/analysis/stage2/top1000_votes_rating_bins.csv`
  - `data/analysis/stage2/top1000_runtime_rating_bins.csv`
  - `data/analysis/stage2/top1000_core_winner_profile.csv`
  - `data/analysis/stage2/top1000_strategy_recommendation.csv`

## Notes

- `--force`: rerun and overwrite existing outputs.
- Without `--force`, scripts can skip existing outputs to save time.
- High-confidence subset in primary analysis defaults to `numVotes >= 1000`.
- Generated files under `data/processed`, `data/analysis`, `data/reports`, `data/staging` are local artifacts and should not be committed.

## Next Steps

- Add SQLite loading and SQL report export as the next production step.
- Load finalized outputs into SQLite and export SQL-based reports.

