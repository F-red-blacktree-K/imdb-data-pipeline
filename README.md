# IMDb Data Pipeline

A reproducible IMDb data pipeline with a primary dataset track and a secondary crawler track.

## Project Overview

This project has two independent tracks:

1. Dataset Pipeline (primary)
- `download_datasets.py -> clean_merge.py -> analyze_core.py`
- Primary unbiased analysis based on official IMDb datasets.

2. Crawl Pipeline (secondary)
- `crawl_top1000.py -> clean_crawl.py -> analyze_top1000.py`
- Targeted analysis for high-rated titles to study why they are well-received.

After ET (extract/transform), the project now includes L (load):
- `load_sqlite.py -> query_reports.py`
- Load clean + analysis outputs into SQLite and export SQL report CSVs.

Dataset analysis is the primary unbiased analysis.
Crawler analysis is a targeted high-rating subgroup study.

## Tech Stack

- Python 3
- pandas, numpy, matplotlib
- requests, tqdm, beautifulsoup4
- SQLite (`sqlite3`)
- Git

## Project Structure

- `download_datasets.py`: Download IMDb dataset files (`basics`, `ratings`) with skip/force behavior.
- `clean_merge.py`: EDA summary + clean + merge `basics` and `ratings` by `tconst`.
- `analyze_core.py`: Stage1 analysis outputs, including full-sample vs high-confidence checks.
- `crawl_top1000.py`: Crawl IMDb Top1000 candidate list via GraphQL API.
- `clean_crawl.py`: Clean and standardize crawler output for Stage2 analysis.
- `analyze_top1000.py`: Advanced Top1000 winner-profile analysis.
- `load_sqlite.py`: Load Stage1/Stage2 clean + mart CSVs into SQLite.
- `query_reports.py`: Run fixed SQL query suite and export report CSVs.
- `run_pipeline.py`: One-command orchestrator for Stage1/Stage2/all + SQLite load + SQL reports.
- `DATA_DICTIONARY.md`: Field definitions and output semantics.`n- `DB_SCHEMA.md`: Human-readable SQLite table/column/index guide.
- `INTERPRETATION.md`: Chart interpretation guide for Stage1 and Stage2.

## Quick Start (Stage1)

```bash
pip install -r requirements.txt
python download_datasets.py
python clean_merge.py --force
python analyze_core.py
```

## Quick Start (Stage2)

```bash
python crawl_top1000.py --force
python clean_crawl.py --force
python analyze_top1000.py
```

## Load to SQLite + SQL Reports

```bash
python load_sqlite.py --stage all
python query_reports.py --suite all
```

Optional:

```bash
python load_sqlite.py --stage stage1
python load_sqlite.py --stage stage2
python load_sqlite.py --stage all --if-exists append
python load_sqlite.py --stage all --no-with-marts

python query_reports.py --suite stage1
python query_reports.py --suite stage2
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
python run_pipeline.py --skip-sql
```

## Outputs

Generated locally (not committed):

- Stage1 clean + analysis:
  - `data/processed/stage1/movies_merged.csv`
  - `data/analysis/stage1/*.csv`
  - `data/analysis/stage1/*.png`
  - `data/reports/stage1/*.json`

- Stage2 clean + analysis:
  - `data/staging/stage2/top1000_ids.csv`
  - `data/processed/stage2/top1000_clean.csv`
  - `data/analysis/stage2/*.csv`
  - `data/analysis/stage2/*.png`
  - `data/reports/stage2/*.json`

- SQLite load + SQL reports:
  - `data/db/imdb_pipeline.db`
  - `data/reports/load_summary.json`
  - `data/reports/sql/*.csv`
  - `data/reports/sql/query_reports_summary.json`

## Notes

- `--force`: rerun and overwrite existing outputs.
- Without `--force`, scripts can skip existing outputs to save time.
- `load_sqlite.py` checks required source CSVs and fails with a clear prerequisite hint if missing.
- Generated files under `data/` are local artifacts and should not be committed.

## Next Steps

- Add MySQL version of load/query modules after SQLite interview demo is stable.
- Add dashboard layer (BI/notebook) on top of `data/db/imdb_pipeline.db`.
