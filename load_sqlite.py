import argparse
import sqlite3
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Stage1/Stage2 clean + mart CSVs into SQLite."
    )
    parser.add_argument(
        "--db-path",
        default="data/db/imdb_pipeline.db",
        help="SQLite database file path.",
    )
    parser.add_argument(
        "--stage",
        choices=["all", "stage1", "stage2"],
        default="all",
        help="Which stage tables to load.",
    )
    parser.add_argument(
        "--if-exists",
        choices=["replace", "append", "fail"],
        default="replace",
        help="Behavior when destination table already exists.",
    )
    parser.add_argument(
        "--with-marts",
        dest="with_marts",
        action="store_true",
        default=True,
        help="Load analysis mart CSV tables as well.",
    )
    parser.add_argument(
        "--no-with-marts",
        dest="with_marts",
        action="store_false",
        help="Load only clean/fact tables.",
    )
    return parser.parse_args()


def require_csv(csv_path: Path, hint: str) -> Path:
    if not csv_path.exists():
        raise SystemExit(
            f"Missing required CSV: {csv_path}\n"
            f"Run prerequisite first: {hint}"
        )
    return csv_path


def read_csv_with_type_normalization(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    numeric_candidates = [
        "rank",
        "year",
        "startYear",
        "endYear",
        "runtimeMinutes",
        "averageRating",
        "numVotes",
        "mean_rating",
        "median_rating",
        "mean_votes",
        "title_count",
        "decade",
        "top_genre_count",
        "top_genre_mean_rating",
        "mean_runtime",
        "mean_year",
        "value",
    ]
    for col in numeric_candidates:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def stage1_tables(with_marts: bool) -> list[tuple[str, Path, str]]:
    tables = [
        (
            "stage1_titles",
            Path("data/processed/stage1/movies_merged.csv"),
            "python download_datasets.py && python clean_merge.py --force",
        )
    ]
    if with_marts:
        tables.extend(
            [
                (
                    "mart_stage1_genre_rating",
                    Path("data/analysis/stage1/genre_rating_summary.csv"),
                    "python analyze_core.py",
                ),
                (
                    "mart_stage1_decade_rating",
                    Path("data/analysis/stage1/decade_summary.csv"),
                    "python analyze_core.py",
                ),
                (
                    "mart_stage1_runtime_bins",
                    Path("data/analysis/stage1/runtime_bin_summary.csv"),
                    "python analyze_core.py",
                ),
                (
                    "mart_stage1_votes_bins",
                    Path("data/analysis/stage1/votes_bin_summary.csv"),
                    "python analyze_core.py",
                ),
            ]
        )
    return tables


def stage2_tables(with_marts: bool) -> list[tuple[str, Path, str]]:
    tables = [
        (
            "stage2_top1000",
            Path("data/processed/stage2/top1000_clean.csv"),
            "python crawl_top1000.py --force && python clean_crawl.py --force",
        )
    ]
    if with_marts:
        tables.extend(
            [
                (
                    "mart_stage2_genre_structure",
                    Path("data/analysis/stage2/top1000_genre_structure.csv"),
                    "python analyze_top1000.py",
                ),
                (
                    "mart_stage2_votes_bins",
                    Path("data/analysis/stage2/top1000_votes_rating_bins.csv"),
                    "python analyze_top1000.py",
                ),
                (
                    "mart_stage2_core_profile",
                    Path("data/analysis/stage2/top1000_core_winner_profile.csv"),
                    "python analyze_top1000.py",
                ),
                (
                    "mart_stage2_decade_genre_hotspots",
                    Path("data/analysis/stage2/top1000_decade_genre_hotspots.csv"),
                    "python analyze_top1000.py",
                ),
                (
                    "mart_stage2_strategy",
                    Path("data/analysis/stage2/top1000_strategy_recommendation.csv"),
                    "python analyze_top1000.py",
                ),
            ]
        )
    return tables


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    report_dir = Path("data/reports")
    report_dir.mkdir(parents=True, exist_ok=True)

    selected_tables: list[tuple[str, Path, str]] = []
    if args.stage in ("all", "stage1"):
        selected_tables.extend(stage1_tables(args.with_marts))
    if args.stage in ("all", "stage2"):
        selected_tables.extend(stage2_tables(args.with_marts))

    loaded_rows: dict[str, int] = {}
    with sqlite3.connect(db_path, timeout=120) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        for table_name, csv_path, hint in selected_tables:
            require_csv(csv_path, hint)
            df = read_csv_with_type_normalization(csv_path)
            df.to_sql(table_name, conn, if_exists=args.if_exists, index=False)
            loaded_rows[table_name] = int(len(df))
            print(f"[LOADED] {table_name}: {len(df):,} rows <- {csv_path}")

        if "stage1_titles" in loaded_rows:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_stage1_titles_tconst "
                "ON stage1_titles(tconst)"
            )
        if "stage2_top1000" in loaded_rows:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_stage2_top1000_rank "
                "ON stage2_top1000(rank)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_stage2_top1000_tconst "
                "ON stage2_top1000(tconst)"
            )

    summary = {
        "db_path": str(db_path),
        "stage": args.stage,
        "if_exists": args.if_exists,
        "with_marts": args.with_marts,
        "loaded_tables": loaded_rows,
    }
    pd.Series(summary).to_json(
        report_dir / "load_summary.json", force_ascii=False, indent=2
    )
    print(f"\nSaved load summary: {report_dir / 'load_summary.json'}")


if __name__ == "__main__":
    main()
