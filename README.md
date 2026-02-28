# IMDb Data Pipeline

Reproducible IMDb ETL pipeline with two analysis tracks and SQLite reporting.

## What This Project Does

- Stage1 (dataset-first): `download_datasets.py -> clean_merge.py -> analyze_core.py`
- Stage2 (crawler-first): `crawl_top1000.py -> clean_crawl.py -> analyze_top1000.py`
- Load & report: `load_sqlite.py -> query_reports.py`

## Quick Start

```bash
pip install -r requirements.txt
python run_pipeline.py
```

## Common Runs

### Full pipeline (recommended)

```bash
python run_pipeline.py
```

### Stage1 only

```bash
python run_pipeline.py --stage stage1
```

### Stage2 only (pure Stage2)

```bash
python run_pipeline.py --stage stage2
```

### Stage2 with Stage1 enrichment (optional)

```bash
python run_pipeline.py --stage stage2 --enrich-stage2
```

### Skip SQLite + SQL export (optional)

```bash
python run_pipeline.py --skip-sql
```

## Important Outputs

- Stage1 merged data: `data/processed/stage1/movies_merged.csv`
- Stage1 marts: `data/analysis/stage1/genre_rating_summary.csv`, `decade_summary.csv`, `runtime_bin_summary.csv`, `votes_bin_summary.csv`
- Stage2 cleaned data: `data/processed/stage2/top1000_clean.csv`
- Stage2 core outputs: `data/analysis/stage2/top1000_enriched.csv`, `top1000_genre_structure.csv`, `top1000_votes_rating_bins.csv`, `top1000_core_winner_profile.csv`, `top1000_strategy_recommendation.csv`
- SQLite DB: `data/db/imdb_pipeline.db`
- SQL reports: `data/reports/sql/*.csv`

## Project Files (Core)

- `run_pipeline.py`: one-command orchestrator
- `load_sqlite.py`: load Stage1/Stage2 tables into SQLite
- `query_reports.py`: export fixed SQL query reports
- `sql/schema.sql`: SQLite schema reference
- `sql/queries.sql`: SQL query reference examples
- `DATA_DICTIONARY.md`: field definitions
- `DB_SCHEMA.md`: table/index guide
- `INTERPRETATION.md`: chart interpretation guide


## Next Step (Engineering Focus)

Current features are complete. Next, prioritize code and performance optimization: improve speed, stability, resource efficiency, and maintainability; reduce duplicated logic and I/O; improve readability and modularity; keep outputs unchanged.
