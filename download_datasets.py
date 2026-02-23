import argparse
import csv
import gzip
import json
from pathlib import Path
from typing import Dict, Iterable, List

import requests
from tqdm import tqdm

IMDB_DATASETS: Dict[str, str] = {
    "basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
    "ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
}


def download_file(url: str, output_path: Path, chunk_size: int = 1024 * 256) -> None:
    """Download a URL to a local path with a progress bar."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))

        with output_path.open("wb") as f, tqdm(
            total=total if total > 0 else None,
            unit="B",
            unit_scale=True,
            desc=f"Downloading {output_path.name}",
        ) as pbar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                pbar.update(len(chunk))

    print(f"Saved: {output_path}")


def preview_tsv_gz(file_path: Path, rows: int = 5) -> List[dict]:
    """Read first N rows from a .tsv.gz file."""
    preview_rows: List[dict] = []
    with gzip.open(file_path, mode="rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for idx, row in enumerate(reader):
            if idx >= rows:
                break
            preview_rows.append(row)
    return preview_rows


def resolve_targets(dataset: str) -> Iterable[tuple[str, str]]:
    """Resolve selected target datasets from CLI argument."""
    if dataset == "all":
        return IMDB_DATASETS.items()
    return [(dataset, IMDB_DATASETS[dataset])]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download IMDb official datasets and optionally preview rows"
    )
    parser.add_argument(
        "--dataset",
        choices=["basics", "ratings", "all"],
        default="all",
        help="Which dataset to download",
    )
    parser.add_argument(
        "--out-dir",
        default="data/raw/datasets",
        help="Directory for downloaded files",
    )
    parser.add_argument(
        "--preview",
        type=int,
        default=0,
        help="Print first N rows after download (0 means no preview)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if file already exists",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)

    for name, url in resolve_targets(args.dataset):
        output_path = out_dir / Path(url).name
        print(f"\nTarget [{name}] -> {output_path}")

        if output_path.exists() and not args.force:
            print(f"Skip download (exists): {output_path}")
        else:
            download_file(url, output_path)

        if args.preview > 0:
            print(f"Preview [{name}] first {args.preview} rows:")
            data = preview_tsv_gz(output_path, rows=args.preview)
            print(json.dumps(data, ensure_ascii=False, indent=2))

    print("\nDone.")


if __name__ == "__main__":
    main()
