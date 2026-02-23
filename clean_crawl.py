import argparse
import json
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean and standardize Top1000 crawler output for downstream analysis"
    )
    parser.add_argument("--input", default="data/staging/stage2/top1000_ids.csv", help="Raw crawler CSV")
    parser.add_argument("--output", default="data/processed/stage2/top1000_clean.csv", help="Cleaned output CSV")
    parser.add_argument("--summary", default="data/reports/stage2/top1000_clean_summary.json", help="Cleaning summary JSON")
    parser.add_argument("--preview", type=int, default=5, help="Print first N rows")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    summary_path = Path(args.summary)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise SystemExit(f"Input not found: {input_path}")

    if output_path.exists() and not args.force:
        print(f"Skip clean (output exists): {output_path}")
        return

    raw = pd.read_csv(input_path)

    required = ["rank", "tconst", "title", "titleType", "year", "runtimeMinutes", "genres", "averageRating", "numVotes", "title_url"]
    for col in required:
        if col not in raw.columns:
            raw[col] = pd.NA

    df = raw[required].copy()

    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["runtimeMinutes"] = pd.to_numeric(df["runtimeMinutes"], errors="coerce")
    df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce")
    df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce")

    before_rows = len(df)

    df = df.dropna(subset=["tconst"]).copy()
    df["tconst"] = df["tconst"].astype(str).str.strip()
    df = df[df["tconst"].str.match(r"^tt\d+$", na=False)]

    # Keep best row by smallest rank if duplicates appear.
    df = df.sort_values(["rank", "tconst"], na_position="last")
    df = df.drop_duplicates(subset=["tconst"], keep="first")

    # Fill lightweight defaults for readability.
    df["title"] = df["title"].fillna("").astype(str).str.strip()

    # Re-rank after cleaning.
    df = df.reset_index(drop=True)
    df["rank"] = df.index + 1

    df.to_csv(output_path, index=False, encoding="utf-8")

    summary = {
        "input_csv": str(input_path),
        "output_csv": str(output_path),
        "rows_before": int(before_rows),
        "rows_after": int(len(df)),
        "dropped_rows": int(before_rows - len(df)),
        "null_counts_after": {k: int(v) for k, v in df.isna().sum().to_dict().items()},
    }
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"Saved: {output_path}")
    print(f"Saved summary: {summary_path}")

    n = min(max(args.preview, 0), len(df))
    if n > 0:
        print(f"Preview first {n} rows:")
        print(df.head(n).to_string(index=False))


if __name__ == "__main__":
    main()

