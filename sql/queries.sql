-- SQL report query reference for query_reports.py

-- Stage1
SELECT genre, title_count, mean_rating, mean_votes
FROM mart_stage1_genre_rating
ORDER BY mean_rating DESC
LIMIT 10;

SELECT decade, title_count, mean_rating, mean_votes
FROM mart_stage1_decade_rating
ORDER BY decade;

SELECT runtime_bin, title_count, mean_rating, mean_votes
FROM mart_stage1_runtime_bins;

-- Stage2
SELECT genre, title_count, mean_rating, mean_votes
FROM mart_stage2_genre_structure
ORDER BY title_count DESC;

SELECT votes_bin, title_count, mean_rating, median_rating, mean_votes
FROM mart_stage2_votes_bins;

SELECT "group", title_count, mean_rating, mean_votes, mean_runtime, mean_year
FROM mart_stage2_core_profile;

SELECT decade, top_genre_by_count, top_genre_count, top_genre_mean_rating
FROM mart_stage2_decade_genre_hotspots
ORDER BY decade;

-- Cross-stage
WITH s1 AS (
    SELECT genre, title_count AS full_count
    FROM mart_stage1_genre_rating
),
s2 AS (
    SELECT genre, title_count AS top1000_count
    FROM mart_stage2_genre_structure
)
SELECT
    s2.genre,
    s2.top1000_count,
    COALESCE(s1.full_count, 0) AS full_count,
    CASE
        WHEN COALESCE(s1.full_count, 0) = 0 THEN NULL
        ELSE CAST(s2.top1000_count AS REAL) / CAST(s1.full_count AS REAL)
    END AS top1000_to_full_ratio
FROM s2
LEFT JOIN s1 ON s1.genre = s2.genre
ORDER BY top1000_to_full_ratio DESC;
