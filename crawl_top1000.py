import argparse
import copy
import csv
import json
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

TT_PATTERN = re.compile(r"tt\d+")

DEFAULT_ENDPOINT = "https://caching.graphql.imdb.com/"
DEFAULT_OPERATION = "AdvancedTitleSearch"
DEFAULT_SHA256 = "9fc7c8867ff66c1e1aa0f39d0fd4869c64db97cddda14fea1c048ca4b568f06a"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch IMDb Top-N titles via GraphQL API (no browser automation)."
    )
    parser.add_argument("--seed-url", default="", help="Optional full Request URL copied from DevTools")

    parser.add_argument("--min-rating", type=float, default=8.0, help="Minimum user rating filter")
    parser.add_argument("--max-rating", type=float, default=10.0, help="Maximum user rating filter")
    parser.add_argument("--locale", default="zh-TW", help="Locale")
    parser.add_argument("--sort-by", default="POPULARITY", help="Sort field")
    parser.add_argument("--sort-order", default="ASC", help="Sort order")

    parser.add_argument("--limit", type=int, default=1000, help="How many titles to collect")
    parser.add_argument("--page-size", type=int, default=50, help="Rows per API call")
    parser.add_argument("--sleep", type=float, default=0.25, help="Sleep seconds between API calls")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout seconds")

    parser.add_argument("--out-csv", default="data/staging/stage2/top1000_ids.csv", help="Output CSV path")
    parser.add_argument("--summary", default="data/reports/stage2/top1000_fetch_summary.json", help="Summary JSON path")
    parser.add_argument("--sample-json", default="data/reports/stage2/top1000_api_sample.json", help="Save first API page JSON sample")
    parser.add_argument("--schema-json", default="data/reports/stage2/top1000_api_schema_hint.json", help="Save simple JSON key-path hint")
    parser.add_argument("--preview", type=int, default=3, help="Print first N rows")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    return parser.parse_args()


def parse_seed_url(seed_url: str) -> tuple[str, dict, str, str]:
    parsed = urlparse(seed_url)
    query = parse_qs(parsed.query)

    operation_name = query.get("operationName", [DEFAULT_OPERATION])[0]
    variables_raw = query.get("variables", ["{}"])[0]
    extensions_raw = query.get("extensions", ["{}"])[0]

    variables = json.loads(variables_raw)
    extensions = json.loads(extensions_raw)
    sha256_hash = extensions.get("persistedQuery", {}).get("sha256Hash") or DEFAULT_SHA256

    endpoint = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    return operation_name, variables, sha256_hash, endpoint


def build_default_variables(args: argparse.Namespace) -> dict:
    return {
        "first": args.page_size,
        "locale": args.locale,
        "sortBy": args.sort_by,
        "sortOrder": args.sort_order,
        "userRatingsConstraint": {
            "aggregateRatingRange": {
                "min": args.min_rating,
                "max": args.max_rating,
            }
        },
    }


def build_variables_for_page(base_variables: dict, after: str | None, page_size: int) -> dict:
    variables = copy.deepcopy(base_variables)
    variables["first"] = page_size
    if after:
        variables["after"] = after
    else:
        variables.pop("after", None)
    return variables


def iter_title_ids(obj):
    if isinstance(obj, dict):
        for v in obj.values():
            yield from iter_title_ids(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_title_ids(item)
    elif isinstance(obj, str) and TT_PATTERN.fullmatch(obj):
        yield obj


def walk_key_paths(obj, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else k
            paths.add(p)
            paths.update(walk_key_paths(v, p))
    elif isinstance(obj, list):
        for idx, item in enumerate(obj[:3]):
            p = f"{prefix}[{idx}]"
            paths.update(walk_key_paths(item, p))
    return paths


def deep_find_first_by_key(obj, target_keys: set[str]):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in target_keys and v not in (None, ""):
                return v
            found = deep_find_first_by_key(v, target_keys)
            if found not in (None, ""):
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = deep_find_first_by_key(item, target_keys)
            if found not in (None, ""):
                return found
    return None


def find_connection_with_edges(data: dict) -> dict | None:
    stack = [data]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            edges = node.get("edges")
            if isinstance(edges, list):
                return node
            for v in node.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(node, list):
            for item in node:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return None


def extract_node_fields(node: dict) -> dict:
    # Current GraphQL response wraps real title payload under node["title"].
    title_obj = node.get("title") if isinstance(node.get("title"), dict) else node

    tconst = None
    if isinstance(title_obj, dict) and isinstance(title_obj.get("id"), str) and TT_PATTERN.fullmatch(title_obj["id"]):
        tconst = title_obj["id"]
    if not tconst:
        for tid in iter_title_ids(title_obj):
            tconst = tid
            break

    title = ""
    title_text_obj = title_obj.get("titleText") if isinstance(title_obj, dict) else None
    if isinstance(title_text_obj, dict) and isinstance(title_text_obj.get("text"), str):
        title = title_text_obj["text"]

    year = None
    release_year = title_obj.get("releaseYear") if isinstance(title_obj, dict) else None
    if isinstance(release_year, dict):
        y = release_year.get("year")
        if isinstance(y, int):
            year = y

    rating = None
    votes = None
    ratings_summary = title_obj.get("ratingsSummary") if isinstance(title_obj, dict) else None
    if isinstance(ratings_summary, dict):
        r = ratings_summary.get("aggregateRating")
        v = ratings_summary.get("voteCount")
        if isinstance(r, (int, float)):
            rating = float(r)
        if isinstance(v, int):
            votes = v

    runtime_minutes = None
    runtime = title_obj.get("runtime") if isinstance(title_obj, dict) else None
    if isinstance(runtime, dict):
        sec = runtime.get("seconds")
        if isinstance(sec, (int, float)):
            runtime_minutes = int(round(float(sec) / 60.0))

    title_type = ""
    tt = title_obj.get("titleType") if isinstance(title_obj, dict) else None
    if isinstance(tt, dict):
        t_text = tt.get("text")
        if isinstance(t_text, str):
            title_type = t_text

    genres = ""
    tg = title_obj.get("titleGenres") if isinstance(title_obj, dict) else None
    if isinstance(tg, dict) and isinstance(tg.get("genres"), list):
        names = []
        for g in tg.get("genres", []):
            if not isinstance(g, dict):
                continue
            gd = g.get("genre")
            if isinstance(gd, dict) and isinstance(gd.get("text"), str):
                names.append(gd.get("text"))
        if names:
            genres = ",".join(names)

    return {
        "tconst": tconst or "",
        "title": title,
        "titleType": title_type,
        "year": year,
        "runtimeMinutes": runtime_minutes,
        "genres": genres,
        "averageRating": rating,
        "numVotes": votes,
    }


def extract_page_items(payload: dict) -> tuple[list[dict], str | None, bool | None, list[str]]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return [], None, None, []

    connection = find_connection_with_edges(data)
    if not connection:
        return [], None, None, []

    edges = connection.get("edges") or []
    page_info = connection.get("pageInfo") or {}
    end_cursor = page_info.get("endCursor")
    has_next_page = page_info.get("hasNextPage")

    rows: list[dict] = []
    sample_paths: list[str] = []

    for edge in edges:
        if not isinstance(edge, dict):
            continue
        cursor = edge.get("cursor")
        node = edge.get("node") if isinstance(edge.get("node"), dict) else edge

        fields = extract_node_fields(node)
        if not fields["tconst"]:
            continue

        if not sample_paths:
            sample_paths = sorted(walk_key_paths(node))

        fields["cursor"] = cursor or ""
        rows.append(fields)

    if not end_cursor and rows:
        end_cursor = rows[-1]["cursor"] or None

    return rows, end_cursor, has_next_page, sample_paths


def fetch_page(
    session: requests.Session,
    endpoint: str,
    operation_name: str,
    sha256_hash: str,
    variables: dict,
    timeout: float,
) -> dict:
    params = {
        "operationName": operation_name,
        "variables": json.dumps(variables, ensure_ascii=False, separators=(",", ":")),
        "extensions": json.dumps(
            {
                "persistedQuery": {
                    "sha256Hash": sha256_hash,
                    "version": 1,
                }
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    }

    headers = {
        "accept": "application/graphql+json, application/json",
        "content-type": "application/json",
        "referer": "https://www.imdb.com/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
    }

    response = session.get(endpoint, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def main() -> None:
    args = parse_args()

    out_csv = Path(args.out_csv)
    summary_path = Path(args.summary)
    sample_json_path = Path(args.sample_json)
    schema_json_path = Path(args.schema_json)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    sample_json_path.parent.mkdir(parents=True, exist_ok=True)
    schema_json_path.parent.mkdir(parents=True, exist_ok=True)

    if out_csv.exists() and not args.force:
        print(f"Skip fetch (output exists): {out_csv}")
        return

    if args.seed_url:
        operation_name, base_variables, sha256_hash, endpoint = parse_seed_url(args.seed_url)
    else:
        operation_name = DEFAULT_OPERATION
        base_variables = build_default_variables(args)
        sha256_hash = DEFAULT_SHA256
        endpoint = DEFAULT_ENDPOINT

    collected: list[dict] = []
    seen: set[str] = set()
    after: str | None = None
    pages_fetched = 0
    first_payload_saved = False
    node_schema_paths: list[str] = []

    with requests.Session() as session:
        while len(collected) < args.limit:
            variables = build_variables_for_page(base_variables, after, args.page_size)
            payload = fetch_page(
                session=session,
                endpoint=endpoint,
                operation_name=operation_name,
                sha256_hash=sha256_hash,
                variables=variables,
                timeout=args.timeout,
            )

            if not first_payload_saved:
                with sample_json_path.open("w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                first_payload_saved = True

            rows, end_cursor, has_next_page, sample_paths = extract_page_items(payload)
            if sample_paths and not node_schema_paths:
                node_schema_paths = sample_paths

            pages_fetched += 1

            if not rows:
                print(f"Stop: page {pages_fetched} returned no rows")
                break

            new_count = 0
            for row in rows:
                tid = row["tconst"]
                if tid in seen:
                    continue
                seen.add(tid)
                collected.append(row)
                new_count += 1
                if len(collected) >= args.limit:
                    break

            print(f"page={pages_fetched} fetched={len(rows)} new={new_count} total={len(collected)}")

            if len(collected) >= args.limit:
                break

            if end_cursor:
                after = end_cursor
            else:
                print("Stop: no next cursor")
                break

            if has_next_page is False:
                print("Stop: hasNextPage=False")
                break

            if args.sleep > 0:
                time.sleep(args.sleep)

    output_rows = []
    for i, row in enumerate(collected[: args.limit], start=1):
        output_rows.append(
            {
                "rank": i,
                "tconst": row["tconst"],
                "title": row.get("title", ""),
                "year": row.get("year", ""),
                "titleType": row.get("titleType", ""),
                "runtimeMinutes": row.get("runtimeMinutes", ""),
                "genres": row.get("genres", ""),
                "averageRating": row.get("averageRating", ""),
                "numVotes": row.get("numVotes", ""),
                "title_url": f"https://www.imdb.com/title/{row['tconst']}/",
            }
        )

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["rank", "tconst", "title", "titleType", "year", "runtimeMinutes", "genres", "averageRating", "numVotes", "title_url"],
        )
        writer.writeheader()
        writer.writerows(output_rows)

    schema_payload = {
        "note": "These are sampled key-paths from one node in the first API page.",
        "sampled_node_key_paths": node_schema_paths,
    }
    with schema_json_path.open("w", encoding="utf-8") as f:
        json.dump(schema_payload, f, ensure_ascii=False, indent=2)

    summary = {
        "endpoint": endpoint,
        "operation_name": operation_name,
        "sha256_hash": sha256_hash,
        "limit_requested": args.limit,
        "page_size": args.page_size,
        "pages_fetched": pages_fetched,
        "rows_written": len(output_rows),
        "output_csv": str(out_csv),
        "sample_json": str(sample_json_path),
        "schema_json": str(schema_json_path),
        "seed_url_used": bool(args.seed_url),
    }
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"Saved: {out_csv}")
    print(f"Saved summary: {summary_path}")
    print(f"Saved sample JSON: {sample_json_path}")
    print(f"Saved schema hint: {schema_json_path}")

    preview_n = min(max(args.preview, 0), len(output_rows))
    if preview_n > 0:
        print(f"Preview first {preview_n} rows:")
        for idx, row in enumerate(output_rows[:preview_n], start=1):
            print(f"[{idx}] {row}")


if __name__ == "__main__":
    main()

