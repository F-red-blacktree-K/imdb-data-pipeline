import argparse
import sqlite3
from pathlib import Path

import pandas as pd


QUERIES = {
    "stage1_genre_top10": """
        SELECT genre, title_count, mean_rating, mean_votes
        FROM mart_stage1_genre_rating
        ORDER BY mean_rating DESC
        LIMIT 10;
    """,
    "stage1_decade_trend": """
        SELECT decade, title_count, mean_rating, mean_votes
        FROM mart_stage1_decade_rating
        ORDER BY decade;
    """,
    "stage1_runtime_bins": """
        SELECT runtime_bin, title_count, mean_rating, mean_votes
        FROM mart_stage1_runtime_bins
        ORDER BY CASE runtime_bin
            WHEN '0-60' THEN 1
            WHEN '61-90' THEN 2
            WHEN '91-120' THEN 3
            WHEN '121-150' THEN 4
            WHEN '151-180' THEN 5
            WHEN '181-240' THEN 6
            WHEN '241+' THEN 7
            ELSE 999 END;
    """,
    "stage2_genre_structure": """
        SELECT genre, title_count, mean_rating, mean_votes
        FROM mart_stage2_genre_structure
        ORDER BY title_count DESC;
    """,
    "stage2_votes_bins": """
        SELECT votes_bin, title_count, mean_rating, median_rating, mean_votes
        FROM mart_stage2_votes_bins
        ORDER BY CASE votes_bin
            WHEN '0-1K' THEN 1
            WHEN '1K-10K' THEN 2
            WHEN '10K-50K' THEN 3
            WHEN '50K-100K' THEN 4
            WHEN '100K-500K' THEN 5
            WHEN '500K+' THEN 6
            ELSE 999 END;
    """,
    "stage2_core_profile": """
        SELECT "group", title_count, mean_rating, mean_votes, mean_runtime, mean_year
        FROM mart_stage2_core_profile
        ORDER BY CASE "group" WHEN 'core_winners' THEN 1 ELSE 2 END;
    """,
    "stage2_decade_genre_hotspots": """
        SELECT decade, top_genre_by_count, top_genre_count, top_genre_mean_rating
        FROM mart_stage2_decade_genre_hotspots
        ORDER BY decade;
    """,
    "cross_stage_genre_bias": """
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
    """,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run fixed SQL report queries and export CSV results."
    )
    parser.add_argument(
        "--db-path",
        default="data/db/imdb_pipeline.db",
        help="SQLite database file path.",
    )
    parser.add_argument(
        "--suite",
        choices=["all", "stage1", "stage2"],
        default="all",
        help="Which query suite to run.",
    )
    parser.add_argument(
        "--out-dir",
        default="data/reports/sql",
        help="Output directory for SQL query CSV results.",
    )
    return parser.parse_args()


def required_tables(suite: str) -> set[str]:
    stage1_tables = {
        "mart_stage1_genre_rating",
        "mart_stage1_decade_rating",
        "mart_stage1_runtime_bins",
    }
    stage2_tables = {
        "mart_stage2_genre_structure",
        "mart_stage2_votes_bins",
        "mart_stage2_core_profile",
        "mart_stage2_decade_genre_hotspots",
    }
    if suite == "stage1":
        return stage1_tables
    if suite == "stage2":
        return stage2_tables
    return stage1_tables | stage2_tables


def selected_query_names(suite: str) -> list[str]:
    stage1 = [
        "stage1_genre_top10",
        "stage1_decade_trend",
        "stage1_runtime_bins",
    ]
    stage2 = [
        "stage2_genre_structure",
        "stage2_votes_bins",
        "stage2_core_profile",
        "stage2_decade_genre_hotspots",
    ]
    cross = ["cross_stage_genre_bias"]
    if suite == "stage1":
        return stage1
    if suite == "stage2":
        return stage2
    return stage1 + stage2 + cross


def existing_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {r[0] for r in rows}


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(
            f"SQLite DB not found: {db_path}\n"
            "Run `python load_sqlite.py --stage all` first."
        )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        tables = existing_tables(conn)
        missing = sorted(required_tables(args.suite) - tables)
        if missing:
            raise SystemExit(
                "Missing required DB tables: "
                + ", ".join(missing)
                + "\nRun `python load_sqlite.py --stage all` (or matching stage) first."
            )

        exported: dict[str, str] = {}
        for name in selected_query_names(args.suite):
            df = pd.read_sql_query(QUERIES[name], conn)
            out_csv = out_dir / f"{name}.csv"
            df.to_csv(out_csv, index=False)
            exported[name] = str(out_csv)
            print(f"[EXPORTED] {name} -> {out_csv}")

    summary = {
        "db_path": str(db_path),
        "suite": args.suite,
        "exported_files": exported,
    }
    pd.Series(summary).to_json(
        out_dir / "query_reports_summary.json", force_ascii=False, indent=2
    )
    print(f"\nSaved SQL query summary: {out_dir / 'query_reports_summary.json'}")


if __name__ == "__main__":
    main()
