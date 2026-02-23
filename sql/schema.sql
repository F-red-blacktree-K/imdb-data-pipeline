-- SQLite schema reference for IMDb pipeline
-- Generated/loaded by load_sqlite.py

-- Core clean tables
-- stage1_titles (from data/processed/stage1/movies_merged.csv)
-- stage2_top1000 (from data/processed/stage2/top1000_clean.csv)

-- Stage1 marts
-- mart_stage1_genre_rating
-- mart_stage1_decade_rating
-- mart_stage1_runtime_bins
-- mart_stage1_votes_bins

-- Stage2 marts
-- mart_stage2_genre_structure
-- mart_stage2_votes_bins
-- mart_stage2_core_profile
-- mart_stage2_decade_genre_hotspots
-- mart_stage2_strategy

-- Suggested indexes (created automatically by load_sqlite.py):
-- idx_stage1_titles_tconst ON stage1_titles(tconst)
-- idx_stage2_top1000_rank ON stage2_top1000(rank)
-- idx_stage2_top1000_tconst ON stage2_top1000(tconst)
