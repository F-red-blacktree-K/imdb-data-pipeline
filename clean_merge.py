import argparse
import json
import time
from pathlib import Path

from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile, clean, and merge IMDb basics + ratings by tconst"
    )
    parser.add_argument("--basics", default="data/raw/title.basics.tsv.gz", help="Path to title.basics.tsv.gz")
    parser.add_argument("--ratings", default="data/raw/title.ratings.tsv.gz", help="Path to title.ratings.tsv.gz")
    parser.add_argument("--out", default="data/processed/movies_merged.csv", help="Output merged CSV path")
    parser.add_argument("--report-dir", default="data/reports", help="Directory for EDA/report output files")
    parser.add_argument("--join-type", choices=["left", "inner"], default="left", help="Join type for basics + ratings")
    parser.add_argument(
        "--max-missing-fields",
        type=int,
        default=0,
        help="Drop rows with missing field count greater than this value (0 = disabled)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Keep first N rows after merge (0 means no limit)")
    parser.add_argument("--preview", type=int, default=5, help="Print first N merged rows (0 means no preview)")
    parser.add_argument("--force", action="store_true", help="Overwrite output file if it already exists")
    return parser.parse_args()


def build_eda_summary(name: str, df, sample_rows: int = 3) -> dict:
    missing = (df.isna().mean() * 100).sort_values(ascending=False)
    return {
        "name": name,
        "shape": [int(df.shape[0]), int(df.shape[1])],
        "columns": list(df.columns),
        "missing_top10_percent": {str(k): float(round(v, 2)) for k, v in missing.head(10).items()},
        "sample_rows": df.head(sample_rows).to_dict(orient="records"),
    }


def print_eda_summary(summary: dict) -> None:
    print(f"\n=== EDA: {summary['name']} ===")
    print(f"shape: {tuple(summary['shape'])}")
    print(f"columns: {summary['columns']}")
    print("top missing ratio (%):")
    for k, v in summary["missing_top10_percent"].items():
        print(f"  {k}: {v}")


def load_data(pd, basics_path: Path, ratings_path: Path):
    basics = pd.read_csv(basics_path, sep="\t", compression="gzip", na_values="\\N", low_memory=False)
    ratings = pd.read_csv(ratings_path, sep="\t", compression="gzip", na_values="\\N", low_memory=False)
    return basics, ratings


def clean_basics(pd, df):
    keep_cols = [
        "tconst",
        "titleType",
        "primaryTitle",
        "originalTitle",
        "isAdult",
        "startYear",
        "endYear",
        "runtimeMinutes",
        "genres",
    ]
    basics = df[keep_cols].copy()
    basics["isAdult"] = pd.to_numeric(basics["isAdult"], errors="coerce")
    basics["startYear"] = pd.to_numeric(basics["startYear"], errors="coerce")
    basics["endYear"] = pd.to_numeric(basics["endYear"], errors="coerce")
    basics["runtimeMinutes"] = pd.to_numeric(basics["runtimeMinutes"], errors="coerce")
    basics = basics.dropna(subset=["tconst"])  # key must exist
    return basics


def clean_ratings(pd, df):
    ratings = df[["tconst", "averageRating", "numVotes"]].copy()
    ratings["averageRating"] = pd.to_numeric(ratings["averageRating"], errors="coerce")
    ratings["numVotes"] = pd.to_numeric(ratings["numVotes"], errors="coerce")
    ratings = ratings.dropna(subset=["tconst"])  # key must exist
    return ratings


def main() -> None:
    args = parse_args()

    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "pandas is required for clean_merge.py. Install dependencies first: pip install -r requirements.txt"
        ) from exc

    basics_path = Path(args.basics)
    ratings_path = Path(args.ratings)
    out_path = Path(args.out)
    report_dir = Path(args.report_dir)

    if not basics_path.exists():
        raise FileNotFoundError(f"Missing basics file: {basics_path}")
    if not ratings_path.exists():
        raise FileNotFoundError(f"Missing ratings file: {ratings_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and not args.force:
        print(f"Skip merge (output exists): {out_path}")
        return

    start = time.time()
    with tqdm(total=4, desc="Pipeline", unit="step") as stage:
        stage.set_postfix_str("load data")
        basics_raw, ratings_raw = load_data(pd, basics_path, ratings_path)
        stage.update(1)

        stage.set_postfix_str("EDA summary")
        basics_eda = build_eda_summary("basics_raw", basics_raw)
        ratings_eda = build_eda_summary("ratings_raw", ratings_raw)
        stage.update(1)

        stage.set_postfix_str("clean + merge")
        basics = clean_basics(pd, basics_raw)
        ratings = clean_ratings(pd, ratings_raw)
        merged = basics.merge(ratings, on="tconst", how=args.join_type)
        merged = merged[
            [
                "tconst",
                "titleType",
                "primaryTitle",
                "originalTitle",
                "isAdult",
                "startYear",
                "endYear",
                "runtimeMinutes",
                "genres",
                "averageRating",
                "numVotes",
            ]
        ]

        if args.max_missing_fields > 0:
            before = len(merged)
            missing_count = merged.isna().sum(axis=1)
            merged = merged[missing_count <= args.max_missing_fields]
            print(
                f"Rows dropped by missing threshold: {before - len(merged):,} "
                f"(max_missing_fields={args.max_missing_fields})"
            )

        if args.limit > 0:
            merged = merged.head(args.limit)
        stage.update(1)

        stage.set_postfix_str("save outputs")
        merged.to_csv(out_path, index=False, encoding="utf-8")

        eda_path = report_dir / "eda_summary.json"
        preview_path = report_dir / "merged_preview.csv"
        summary_path = report_dir / "run_summary.json"

        with eda_path.open("w", encoding="utf-8") as f:
            json.dump({"basics": basics_eda, "ratings": ratings_eda}, f, ensure_ascii=False, indent=2)

        merged.head(max(args.preview, 5)).to_csv(preview_path, index=False, encoding="utf-8")

        with summary_path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "rows_after_clean_basics": int(len(basics)),
                    "rows_after_clean_ratings": int(len(ratings)),
                    "rows_after_merge": int(len(merged)),
                    "join_type": args.join_type,
                    "max_missing_fields": int(args.max_missing_fields),
                    "elapsed_seconds": round(time.time() - start, 2),
                    "output_csv": str(out_path),
                    "eda_summary": str(eda_path),
                    "preview_csv": str(preview_path),
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        stage.update(1)

    print_eda_summary(basics_eda)
    print_eda_summary(ratings_eda)
    print(f"\nMerged rows written: {len(merged):,}")
    print(f"Saved merged CSV: {out_path}")
    print(f"Saved EDA JSON: {eda_path}")
    print(f"Saved preview CSV: {preview_path}")
    print(f"Saved run summary: {summary_path}")

    if args.preview > 0:
        print(f"\nPreview first {min(args.preview, len(merged))} rows:")
        print(merged.head(args.preview).to_string(index=False))


if __name__ == "__main__":
    main()
