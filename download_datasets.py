import argparse
import csv
import gzip
import json
from pathlib import Path
from typing import Dict, Iterable, List
import requests
from tqdm import tqdm

# 這個專案要用的 IMDb 官方資料集網址。
IMDB_DATASETS: Dict[str, str] = {
    "basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
    "ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
}


def download_file(url: str, output_path: Path, chunk_size: int = 1024 * 256) -> None:
    """下載指定 URL 到本機，並顯示進度條。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))

        # output_path.open("wb"):
        # w = write 寫入模式、b = binary 二進位模式（.gz 壓縮檔必須用二進位寫入）。
        # tqdm(...):
        # 建立進度條物件，total 是總大小（bytes），desc 是進度條顯示文字。
        with output_path.open("wb") as f, tqdm(
            total=total if total > 0 else None,
            unit="B",
            unit_scale=True,
            desc=f"下載中 {output_path.name}",
        ) as pbar:
            # iter_content 會把下載內容切成一塊一塊（chunk）讀取，避免一次吃太多記憶體。
            for chunk in response.iter_content(chunk_size=chunk_size):
                # 有些 chunk 可能是空值，直接跳過。
                if not chunk:
                    continue
                # 把這一塊資料寫進檔案。
                f.write(chunk)
                # 把這一塊的大小更新到進度條。
                pbar.update(len(chunk))

    print(f"已儲存: {output_path}")


def preview_tsv_gz(file_path: Path, rows: int = 5) -> List[dict]:
    """讀取 .tsv.gz 的前 N 筆資料，方便快速檢查格式。"""
    preview_rows: List[dict] = []
    with gzip.open(file_path, mode="rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for idx, row in enumerate(reader):
            if idx >= rows:
                break
            preview_rows.append(row)
    return preview_rows


def resolve_targets(dataset: str) -> Iterable[tuple[str, str]]:
    """根據命令列參數決定要下載哪個資料集。"""
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
        default="data/raw",
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
        print(f"\n目標 [{name}] -> {output_path}")

        # output_path.exists() 是 pathlib.Path 的官方方法，用來檢查路徑（檔案）是否存在。
        # args.force 來自 --force 參數：
        # - 不加 --force => args.force 為 False
        # - 有加 --force => args.force 為 True
        # 這個判斷式意思是：
        # 如果檔案已存在，而且你「沒有」要求強制重抓，就跳過下載。
        if output_path.exists() and not args.force:
            print(f"略過下載（檔案已存在）: {output_path}")
        else:
            download_file(url, output_path)

        if args.preview > 0:
            print(f"預覽 [{name}] 前 {args.preview} 筆:")
            data = preview_tsv_gz(output_path, rows=args.preview)
            print(json.dumps(data, ensure_ascii=False, indent=2))

    print("\n完成。")


if __name__ == "__main__":
    main()

