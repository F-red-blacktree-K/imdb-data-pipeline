# Data Dictionary

This document defines the key columns used in the current dataset-based pipeline.

## Source Files

- Raw basics: `data/raw/title.basics.tsv.gz`
- Raw ratings: `data/raw/title.ratings.tsv.gz`
- Merged output: `data/processed/movies_merged.csv`
- EDA summary: `data/reports/eda_summary.json`
- Run summary: `data/reports/run_summary.json`

## Raw: title.basics

Columns:
- `tconst`: IMDb title unique identifier (for example `tt0000001`)
- `titleType`: title category (`movie`, `short`, `tvSeries`, etc.)
- `primaryTitle`: main display title
- `originalTitle`: original/local title
- `isAdult`: adult-content flag (`0`/`1`)
- `startYear`: release/start year
- `endYear`: end year (mostly for series; often missing for movies)
- `runtimeMinutes`: runtime in minutes
- `genres`: comma-separated genres

## Raw: title.ratings

Columns:
- `tconst`: IMDb title unique identifier
- `averageRating`: average user rating score
- `numVotes`: number of votes contributing to the rating

## Processed: movies_merged.csv

Produced by `clean_merge.py` by cleaning types and merging on `tconst`.

Current output columns:
- `tconst`
- `titleType`
- `primaryTitle`
- `originalTitle`
- `isAdult`
- `startYear`
- `endYear`
- `runtimeMinutes`
- `genres`
- `averageRating`
- `numVotes`

## Missing Values Policy

- Missing values are kept as `NaN` when unknown/not applicable.
- Missing values are not automatically replaced with `0` to avoid distorting analysis.
- Text columns remain text for reporting/analytics (ML encoding is a separate step).