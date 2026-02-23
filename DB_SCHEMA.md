# Database Schema (SQLite)

This document describes the SQLite schema for this project:
- Database file: `data/db/imdb_pipeline.db`
- Loader: `load_sqlite.py`
- SQL report runner: `query_reports.py`

## Why only one `.db` file?

SQLite is a single-file database engine.
- One `.db` file can contain many tables.
- In this project, `imdb_pipeline.db` currently contains **11 tables**.

## Table List

### Fact / Clean Tables
- `stage1_titles` (from `data/processed/stage1/movies_merged.csv`)
- `stage2_top1000` (from `data/processed/stage2/top1000_clean.csv`)

### Stage1 Mart Tables
- `mart_stage1_genre_rating`
- `mart_stage1_decade_rating`
- `mart_stage1_runtime_bins`
- `mart_stage1_votes_bins`

### Stage2 Mart Tables
- `mart_stage2_genre_structure`
- `mart_stage2_votes_bins`
- `mart_stage2_core_profile`
- `mart_stage2_decade_genre_hotspots`
- `mart_stage2_strategy`

## Key Columns (Human-readable)

### `stage1_titles`
- `tconst`: IMDb title id (join key)
- `titleType`: title type (movie, short, tvSeries, etc.)
- `primaryTitle`: main display title
- `originalTitle`: original title
- `isAdult`: adult flag (0/1)
- `startYear`, `endYear`: year fields
- `runtimeMinutes`: runtime
- `genres`: genre string
- `averageRating`, `numVotes`: rating and vote count

### `stage2_top1000`
- `rank`: rank inside the crawled Top1000 list
- `tconst`: IMDb title id
- `title`: **title name** (yes, Stage2 includes title)
- `titleType`: type in crawl result
- `year`: release/start year
- `runtimeMinutes`: runtime
- `genres`: genre string
- `averageRating`, `numVotes`: rating and vote count
- `title_url`: title page URL

## Primary Key / Indexes

This project uses indexes for fast lookup and rank operations:
- `idx_stage1_titles_tconst` on `stage1_titles(tconst)`
- `idx_stage2_top1000_rank` on `stage2_top1000(rank)`
- `idx_stage2_top1000_tconst` on `stage2_top1000(tconst)`

Notes:
- SQLite allows querying without indexes; indexes are for performance.
- Current implementation uses indexes as practical keys for access patterns.

## How to Inspect DB Quickly

### 1) List tables
```bash
python -c "import sqlite3; c=sqlite3.connect('data/db/imdb_pipeline.db'); print([r[0] for r in c.execute(\"select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name\")]); c.close()"
```

### 2) Check columns of one table
```bash
python -c "import sqlite3; c=sqlite3.connect('data/db/imdb_pipeline.db'); print(c.execute('pragma table_info(stage2_top1000)').fetchall()); c.close()"
```

### 3) Check row counts
```bash
python -c "import sqlite3; c=sqlite3.connect('data/db/imdb_pipeline.db'); print('stage1_titles', c.execute('select count(*) from stage1_titles').fetchone()[0]); print('stage2_top1000', c.execute('select count(*) from stage2_top1000').fetchone()[0]); c.close()"
```

### 4) Query title by `tconst`
```bash
python -c "import sqlite3; c=sqlite3.connect('data/db/imdb_pipeline.db'); print(c.execute(\"select tconst, primaryTitle, genres, averageRating, numVotes from stage1_titles where tconst='tt0000001'\").fetchall()); c.close()"
```

## Related Files

- SQL reference: `sql/schema.sql`, `sql/queries.sql`
- SQL CSV outputs: `data/reports/sql/*.csv`
- Load summary: `data/reports/load_summary.json`

## How to Read `load_summary.json`

Example:
```json
{
  "db_path": "data\\db\\imdb_pipeline.db",
  "stage": "all",
  "if_exists": "replace",
  "with_marts": true,
  "loaded_tables": {
    "mart_stage1_genre_rating": 28
  }
}
```

Field meanings:
- `db_path`: where the SQLite file is created.
- `stage`: which loading scope was used (`stage1`, `stage2`, or `all`).
- `if_exists`: what to do if table already exists.
  - `replace`: drop/recreate table
  - `append`: append rows
  - `fail`: stop with error
- `with_marts`: whether mart (aggregated analysis) tables are also loaded.
- `loaded_tables`: **row count per table written in this run**.

Important:
- Numbers like `28`, `16`, `7`, `11` are **row counts**, not column counts.
- Example: `"mart_stage1_genre_rating": 28` means table `mart_stage1_genre_rating` has 28 rows loaded.

## Why `python -c` Can Show Table Names

This command:
```bash
python -c "import sqlite3; c=sqlite3.connect('data/db/imdb_pipeline.db'); print([r[0] for r in c.execute(\"select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name\")]); c.close()"
```
works because:
- SQLite keeps metadata in `sqlite_master` (system catalog table).
- Querying `sqlite_master` with `type='table'` returns all user tables.

`-c` means:
- Run Python code directly from command line (one-liner script), no `.py` file needed.

If you prefer readable steps instead of one long line:
```bash
python
```
Then type:
```python
import sqlite3
c = sqlite3.connect('data/db/imdb_pipeline.db')
for row in c.execute("select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name"):
    print(row[0])
c.close()
```

## How to Read `query_reports_summary.json`

Example:
```json
{
  "db_path": "data\\db\\imdb_pipeline.db",
  "suite": "all",
  "exported_files": {
    "stage1_genre_top10": "data\\reports\\sql\\stage1_genre_top10.csv"
  }
}
```

Field meanings:
- `db_path`: which SQLite DB was queried.
- `suite`: which query set was executed (`stage1`, `stage2`, `all`).
- `exported_files`: mapping from query name to exported CSV path.

How to validate success:
- `exported_files` should not be empty.
- Every listed CSV path should exist.
- CSV files should contain header + data rows.

## SQL Report CSV Dictionary

This section explains the meaning of each exported SQL CSV in `data/reports/sql/`.

### Stage1 reports
- `stage1_genre_top10.csv`
  - Purpose: top genres by mean rating in Stage1 market-level data.
  - Columns: `genre`, `title_count`, `mean_rating`, `mean_votes`.

- `stage1_decade_trend.csv`
  - Purpose: mean rating trend by decade.
  - Columns: `decade`, `title_count`, `mean_rating`, `mean_votes`.

- `stage1_runtime_bins.csv`
  - Purpose: rating/votes by runtime bucket.
  - Columns: `runtime_bin`, `title_count`, `mean_rating`, `mean_votes`.

### Stage2 reports
- `stage2_genre_structure.csv`
  - Purpose: genre composition of Top1000.
  - Columns: `genre`, `title_count`, `mean_rating`, `mean_votes`.

- `stage2_votes_bins.csv`
  - Purpose: rating behavior across vote-size buckets in Top1000.
  - Columns: `votes_bin`, `title_count`, `mean_rating`, `median_rating`, `mean_votes`.

- `stage2_core_profile.csv`
  - Purpose: profile comparison between `core_winners` and `others`.
  - Columns: `group`, `title_count`, `mean_rating`, `mean_votes`, `mean_runtime`, `mean_year`.

- `stage2_decade_genre_hotspots.csv`
  - Purpose: per-decade dominant genre in Top1000.
  - Columns: `decade`, `top_genre_by_count`, `top_genre_count`, `top_genre_mean_rating`.

### Cross-stage report
- `cross_stage_genre_bias.csv`
  - Purpose: compare Top1000 vs full-market genre concentration.
  - Columns: `genre`, `top1000_count`, `full_count`, `top1000_to_full_ratio`.
  - Interpretation: higher ratio means the genre is more concentrated in Top1000 relative to full market.

## Common Confusions and Quick Answers

- "I only see one `.db` file. Does that mean one table?"
  - No. One SQLite file can contain many tables.

- "Why can't VS Code open `imdb_pipeline.db` as text?"
  - It is a binary database file, not a text file.

- "Where should I read structure quickly?"
  - Use this file (`DB_SCHEMA.md`) for structure/meaning.
  - Use `query_reports_summary.json` and SQL CSV files for output verification.
