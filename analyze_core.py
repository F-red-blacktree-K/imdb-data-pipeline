import argparse
from pathlib import Path

import pandas as pd

RUNTIME_BINS = [0, 60, 90, 120, 150, 180, 240, 9999]
RUNTIME_LABELS = ["0-60", "61-90", "91-120", "121-150", "151-180", "181-240", "241+"]
VOTE_BINS = [0, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 999999999]
VOTE_LABELS = ["0-10", "11-50", "51-100", "101-500", "501-1k", "1k-5k", "5k-10k", "10k-50k", "50k-100k", "100k-500k", "500k+"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate concise analysis + robustness outputs from merged IMDb data")
    parser.add_argument("--input-csv", default="data/processed/movies_merged.csv", help="Input merged CSV")
    parser.add_argument("--out-dir", default="data/analysis", help="Output directory")
    parser.add_argument("--high-vote-threshold", type=int, default=1000, help="High-confidence threshold")
    parser.add_argument("--min-genre-titles", type=int, default=30, help="Min title count for genre comparisons")
    parser.add_argument("--min-decade-genre-titles", type=int, default=20, help="Min rows for decade-genre top-rating")
    parser.add_argument("--rating-diff-threshold", type=float, default=0.2, help="Threshold for changed_flag")
    parser.add_argument("--corr-diff-threshold", type=float, default=0.1, help="Correlation threshold for changed_flag")
    parser.add_argument("--rank-shift-threshold", type=int, default=3, help="Top-rank shift threshold for changed_flag")
    parser.add_argument("--keep-legacy-output", action="store_true", help="Do not delete old legacy output files")
    return parser.parse_args()


def save_df(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def to_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["startYear", "endYear", "runtimeMinutes", "averageRating", "numVotes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def explode_genres(df: pd.DataFrame, include_decade: bool = False) -> pd.DataFrame:
    cols = ["genres", "averageRating", "numVotes"]
    if include_decade:
        cols.insert(0, "startYear")

    g = df[cols].dropna(subset=["genres", "averageRating"]).copy()
    if include_decade:
        g = g.dropna(subset=["startYear"])
        g["decade"] = (g["startYear"] // 10 * 10).astype("Int64")

    rows = []
    for _, row in g.iterrows():
        for genre in str(row["genres"]).split(","):
            genre = genre.strip()
            if not genre:
                continue
            item = {
                "genre": genre,
                "averageRating": row["averageRating"],
                "numVotes": row["numVotes"],
            }
            if include_decade:
                item["decade"] = row["decade"]
            rows.append(item)

    if not rows:
        return pd.DataFrame(columns=["decade", "genre", "averageRating", "numVotes"] if include_decade else ["genre", "averageRating", "numVotes"])

    return pd.DataFrame(rows)


def build_genre_summary(df: pd.DataFrame) -> pd.DataFrame:
    e = explode_genres(df, include_decade=False)
    if e.empty:
        return pd.DataFrame(columns=["genre", "title_count", "mean_rating", "mean_votes"])

    return (
        e.groupby("genre", as_index=False)
        .agg(title_count=("averageRating", "count"), mean_rating=("averageRating", "mean"), mean_votes=("numVotes", "mean"))
        .sort_values(["mean_rating", "title_count"], ascending=[False, False])
    )


def build_decade_summary(df: pd.DataFrame) -> pd.DataFrame:
    d = df.dropna(subset=["startYear", "averageRating"]).copy()
    if d.empty:
        return pd.DataFrame(columns=["decade", "title_count", "mean_rating", "mean_votes"])

    d["decade"] = (d["startYear"] // 10 * 10).astype("Int64")
    return (
        d.groupby("decade", as_index=False)
        .agg(title_count=("startYear", "count"), mean_rating=("averageRating", "mean"), mean_votes=("numVotes", "mean"))
        .sort_values("decade")
    )


def build_runtime_bin_summary(df: pd.DataFrame) -> pd.DataFrame:
    r = df[["runtimeMinutes", "averageRating", "numVotes"]].dropna(subset=["runtimeMinutes", "averageRating"]).copy()
    if r.empty:
        return pd.DataFrame(columns=["runtime_bin", "title_count", "mean_rating", "mean_votes"])

    r["runtime_bin"] = pd.cut(r["runtimeMinutes"], bins=RUNTIME_BINS, labels=RUNTIME_LABELS, include_lowest=True)
    out = (
        r.groupby("runtime_bin", as_index=False, observed=False)
        .agg(title_count=("averageRating", "count"), mean_rating=("averageRating", "mean"), mean_votes=("numVotes", "mean"))
        .dropna(subset=["runtime_bin"])
    )
    out["runtime_bin"] = out["runtime_bin"].astype(str)
    return out


def build_votes_bin_summary(df: pd.DataFrame) -> pd.DataFrame:
    v = df[["numVotes", "averageRating"]].dropna(subset=["numVotes", "averageRating"]).copy()
    if v.empty:
        return pd.DataFrame(columns=["votes_bin", "title_count", "mean_rating"])

    v["votes_bin"] = pd.cut(v["numVotes"], bins=VOTE_BINS, labels=VOTE_LABELS, include_lowest=True)
    out = (
        v.groupby("votes_bin", as_index=False, observed=False)
        .agg(title_count=("averageRating", "count"), mean_rating=("averageRating", "mean"))
        .dropna(subset=["votes_bin"])
    )
    out["votes_bin"] = out["votes_bin"].astype(str)
    return out


def build_decade_genre_outputs(df: pd.DataFrame, min_titles: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    dg = explode_genres(df, include_decade=True)
    if dg.empty:
        empty = pd.DataFrame(columns=["decade", "genre", "title_count", "mean_rating"])
        return empty, empty

    grouped = (
        dg.groupby(["decade", "genre"], as_index=False)
        .agg(title_count=("averageRating", "count"), mean_rating=("averageRating", "mean"))
        .sort_values(["decade", "title_count", "mean_rating"], ascending=[True, False, False])
    )

    top_count = grouped.sort_values(["decade", "title_count", "mean_rating"], ascending=[True, False, False]).groupby("decade", as_index=False).first()

    rated = grouped[grouped["title_count"] >= min_titles].copy()
    if rated.empty:
        top_rating = pd.DataFrame(columns=["decade", "genre", "title_count", "mean_rating"])
    else:
        top_rating = rated.sort_values(["decade", "mean_rating", "title_count"], ascending=[True, False, False]).groupby("decade", as_index=False).first()

    return top_count, top_rating


def safe_float(x):
    return None if pd.isna(x) else float(x)


def max_rank_shift(full: pd.DataFrame, hc: pd.DataFrame, top_n: int = 10) -> float | None:
    if full.empty or hc.empty:
        return None
    rf = full.head(top_n).reset_index(drop=True)[["genre"]].copy()
    rh = hc.head(top_n).reset_index(drop=True)[["genre"]].copy()
    rf["rank_full"] = rf.index + 1
    rh["rank_hc"] = rh.index + 1
    joined = rf.merge(rh, on="genre", how="inner")
    if joined.empty:
        return None
    return float((joined["rank_full"] - joined["rank_hc"]).abs().max())


def plot_compare_line(plt, merged: pd.DataFrame, x_col: str, y_full: str, y_hc: str, title: str, xlabel: str, ylabel: str, out_path: Path) -> None:
    if merged.empty:
        return
    x = merged[x_col].astype(str)
    plt.figure(figsize=(11, 6))
    plt.plot(x, merged[y_full], marker="o", color="#1f77b4", label="Full")
    plt.plot(x, merged[y_hc], marker="o", color="#d62728", label="High-Confidence")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_genre_compare_bar(plt, merged: pd.DataFrame, out_path: Path) -> None:
    if merged.empty:
        return
    d = merged.sort_values("mean_rating_full", ascending=False).head(12).copy()
    y = range(len(d))
    plt.figure(figsize=(12, 7))
    plt.barh([i + 0.2 for i in y], d["mean_rating_full"], height=0.38, color="#1f77b4", label="Full")
    plt.barh([i - 0.2 for i in y], d["mean_rating_hc"], height=0.38, color="#d62728", label="High-Confidence")
    plt.yticks(y, d["genre"])
    plt.gca().invert_yaxis()
    plt.title("Genre Mean Rating: Full vs High-Confidence")
    plt.xlabel("Mean Rating")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_decade_genre_heatmap(plt, top_count: pd.DataFrame, out_path: Path) -> None:
    if top_count.empty:
        return

    decades = top_count["decade"].astype(str).tolist()
    labels = top_count["genre"].tolist()
    counts = top_count["title_count"].tolist()

    # one-column heat stripe + text labels: easier to read than full matrix
    import numpy as np

    arr = np.array(counts).reshape(-1, 1)
    plt.figure(figsize=(8, 8))
    img = plt.imshow(arr, aspect="auto", cmap="YlGnBu")
    plt.colorbar(img, label="Title Count")
    plt.yticks(range(len(decades)), decades)
    plt.xticks([0], ["Top Genre by Count"])
    for i, label in enumerate(labels):
        plt.text(0, i, f" {label}", va="center", ha="left", color="black", fontsize=9)
    plt.title("Decade x Genre Hotspot (Most Frequent Genre per Decade)")
    plt.ylabel("Decade")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_subset_comparison(plt, subset: pd.DataFrame, out_path: Path) -> None:
    if subset.empty:
        return

    labels = subset["subset"].tolist()
    ratings = subset["mean_rating"].tolist()
    votes = subset["mean_votes"].tolist()

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    axes[0].bar(labels, ratings, color="#1f77b4")
    axes[0].set_title("Mean Rating")
    axes[0].set_ylabel("Rating")

    axes[1].bar(labels, votes, color="#d62728")
    axes[1].set_title("Mean Votes")
    axes[1].set_ylabel("Votes")

    fig.suptitle("Subset Comparison: Full vs High-Confidence")
    fig.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close(fig)


def cleanup_legacy_files(out_dir: Path) -> None:
    old_files = [
        "chart_top_genres.png",
        "chart_decade_rating.png",
        "chart_runtime_vs_rating.png",
        "genre_rating_summary_filtered.csv",
        "decade_summary_filtered.csv",
        "genre_comparison.csv",
        "decade_comparison.csv",
        "runtime_bin_comparison.csv",
        "votes_bin_comparison.csv",
        "decade_genre_summary.csv",
    ]
    for name in old_files:
        p = out_dir / name
        if p.exists():
            p.unlink()


def main() -> None:
    args = parse_args()

    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as exc:
        raise SystemExit("matplotlib is required. Install dependencies: pip install -r requirements.txt") from exc

    in_path = Path(args.input_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(f"Missing input CSV: {in_path}")

    df = to_numeric_columns(pd.read_csv(in_path))
    hc = df[df["numVotes"].fillna(0) >= args.high_vote_threshold].copy()

    genre_full = build_genre_summary(df)
    genre_hc = build_genre_summary(hc)
    decade_full = build_decade_summary(df)
    decade_hc = build_decade_summary(hc)
    runtime_full = build_runtime_bin_summary(df)
    runtime_hc = build_runtime_bin_summary(hc)
    votes_full = build_votes_bin_summary(df)
    votes_hc = build_votes_bin_summary(hc)
    decade_genre_top_count, decade_genre_top_rating = build_decade_genre_outputs(df, args.min_decade_genre_titles)

    # comparisons for plotting + robustness
    genre_cmp = (
        genre_full[genre_full["title_count"] >= args.min_genre_titles][["genre", "mean_rating"]]
        .merge(
            genre_hc[genre_hc["title_count"] >= args.min_genre_titles][["genre", "mean_rating"]],
            on="genre",
            how="inner",
            suffixes=("_full", "_hc"),
        )
    )
    genre_cmp["abs_diff"] = (genre_cmp["mean_rating_full"] - genre_cmp["mean_rating_hc"]).abs()

    decade_cmp = (
        decade_full[["decade", "mean_rating"]]
        .merge(decade_hc[["decade", "mean_rating"]], on="decade", how="inner", suffixes=("_full", "_hc"))
        .sort_values("decade")
    )
    decade_cmp["abs_diff"] = (decade_cmp["mean_rating_full"] - decade_cmp["mean_rating_hc"]).abs()

    runtime_cmp = (
        runtime_full[["runtime_bin", "mean_rating"]]
        .merge(runtime_hc[["runtime_bin", "mean_rating"]], on="runtime_bin", how="inner", suffixes=("_full", "_hc"))
    )
    runtime_cmp["abs_diff"] = (runtime_cmp["mean_rating_full"] - runtime_cmp["mean_rating_hc"]).abs()

    votes_cmp = (
        votes_full[["votes_bin", "mean_rating"]]
        .merge(votes_hc[["votes_bin", "mean_rating"]], on="votes_bin", how="inner", suffixes=("_full", "_hc"))
    )
    votes_cmp["abs_diff"] = (votes_cmp["mean_rating_full"] - votes_cmp["mean_rating_hc"]).abs()

    # core csv outputs only
    save_df(genre_full, out_dir / "genre_rating_summary.csv")
    save_df(decade_full, out_dir / "decade_summary.csv")
    save_df(runtime_full, out_dir / "runtime_bin_summary.csv")
    save_df(votes_full, out_dir / "votes_bin_summary.csv")
    save_df(decade_genre_top_count, out_dir / "decade_genre_top_count.csv")
    save_df(decade_genre_top_rating, out_dir / "decade_genre_top_rating.csv")

    subset_summary = pd.DataFrame([
        {
            "subset": "full",
            "rows": int(len(df)),
            "mean_rating": float(df["averageRating"].mean(skipna=True)),
            "mean_votes": float(df["numVotes"].mean(skipna=True)),
        },
        {
            "subset": f"numVotes>={args.high_vote_threshold}",
            "rows": int(len(hc)),
            "mean_rating": float(hc["averageRating"].mean(skipna=True)),
            "mean_votes": float(hc["numVotes"].mean(skipna=True)),
        },
    ])
    save_df(subset_summary, out_dir / "subset_comparison.csv")

    corr_full_df = df[["runtimeMinutes", "averageRating"]].dropna()
    corr_hc_df = hc[["runtimeMinutes", "averageRating"]].dropna()
    corr_full = corr_full_df["runtimeMinutes"].corr(corr_full_df["averageRating"]) if len(corr_full_df) > 1 else None
    corr_hc = corr_hc_df["runtimeMinutes"].corr(corr_hc_df["averageRating"]) if len(corr_hc_df) > 1 else None

    rank_shift = max_rank_shift(
        genre_full[genre_full["title_count"] >= args.min_genre_titles].sort_values("mean_rating", ascending=False),
        genre_hc[genre_hc["title_count"] >= args.min_genre_titles].sort_values("mean_rating", ascending=False),
        top_n=10,
    )

    robustness = pd.DataFrame([
        {
            "metric": "overall_mean_rating_diff",
            "full_value": float(df["averageRating"].mean(skipna=True)),
            "hc_value": float(hc["averageRating"].mean(skipna=True)) if len(hc) > 0 else None,
            "abs_diff": abs(float(df["averageRating"].mean(skipna=True)) - float(hc["averageRating"].mean(skipna=True))) if len(hc) > 0 else None,
            "threshold": args.rating_diff_threshold,
            "changed_flag": bool(len(hc) > 0 and abs(float(df["averageRating"].mean(skipna=True)) - float(hc["averageRating"].mean(skipna=True))) > args.rating_diff_threshold),
        },
        {
            "metric": "genre_mean_abs_diff",
            "full_value": safe_float(genre_cmp["mean_rating_full"].mean()) if not genre_cmp.empty else None,
            "hc_value": safe_float(genre_cmp["mean_rating_hc"].mean()) if not genre_cmp.empty else None,
            "abs_diff": safe_float(genre_cmp["abs_diff"].mean()) if not genre_cmp.empty else None,
            "threshold": args.rating_diff_threshold,
            "changed_flag": bool((not genre_cmp.empty) and genre_cmp["abs_diff"].mean() > args.rating_diff_threshold),
        },
        {
            "metric": "decade_mean_abs_diff",
            "full_value": safe_float(decade_cmp["mean_rating_full"].mean()) if not decade_cmp.empty else None,
            "hc_value": safe_float(decade_cmp["mean_rating_hc"].mean()) if not decade_cmp.empty else None,
            "abs_diff": safe_float(decade_cmp["abs_diff"].mean()) if not decade_cmp.empty else None,
            "threshold": args.rating_diff_threshold,
            "changed_flag": bool((not decade_cmp.empty) and decade_cmp["abs_diff"].mean() > args.rating_diff_threshold),
        },
        {
            "metric": "runtime_bin_mean_abs_diff",
            "full_value": safe_float(runtime_cmp["mean_rating_full"].mean()) if not runtime_cmp.empty else None,
            "hc_value": safe_float(runtime_cmp["mean_rating_hc"].mean()) if not runtime_cmp.empty else None,
            "abs_diff": safe_float(runtime_cmp["abs_diff"].mean()) if not runtime_cmp.empty else None,
            "threshold": args.rating_diff_threshold,
            "changed_flag": bool((not runtime_cmp.empty) and runtime_cmp["abs_diff"].mean() > args.rating_diff_threshold),
        },
        {
            "metric": "votes_bin_mean_abs_diff",
            "full_value": safe_float(votes_cmp["mean_rating_full"].mean()) if not votes_cmp.empty else None,
            "hc_value": safe_float(votes_cmp["mean_rating_hc"].mean()) if not votes_cmp.empty else None,
            "abs_diff": safe_float(votes_cmp["abs_diff"].mean()) if not votes_cmp.empty else None,
            "threshold": 0.15,
            "changed_flag": bool((not votes_cmp.empty) and votes_cmp["abs_diff"].mean() > 0.15),
        },
        {
            "metric": "runtime_rating_corr_diff",
            "full_value": corr_full,
            "hc_value": corr_hc,
            "abs_diff": abs(corr_full - corr_hc) if (corr_full is not None and corr_hc is not None) else None,
            "threshold": args.corr_diff_threshold,
            "changed_flag": bool(corr_full is not None and corr_hc is not None and abs(corr_full - corr_hc) > args.corr_diff_threshold),
        },
        {
            "metric": "genre_top10_rank_shift_max",
            "full_value": 0,
            "hc_value": 0,
            "abs_diff": rank_shift,
            "threshold": args.rank_shift_threshold,
            "changed_flag": bool(rank_shift is not None and rank_shift > args.rank_shift_threshold),
        },
    ])
    save_df(robustness, out_dir / "robustness_summary.csv")

    # charts (concise set)
    plot_genre_compare_bar(plt, genre_cmp, out_dir / "chart_genre_rating_compare.png")
    plot_compare_line(plt, decade_cmp, "decade", "mean_rating_full", "mean_rating_hc", f"Mean Rating by Decade: Full vs HC (numVotes>={args.high_vote_threshold})", "Decade", "Mean Rating", out_dir / "chart_decade_rating_compare.png")
    plot_compare_line(plt, runtime_cmp, "runtime_bin", "mean_rating_full", "mean_rating_hc", f"Mean Rating by Runtime Bin: Full vs HC (numVotes>={args.high_vote_threshold})", "Runtime Bin", "Mean Rating", out_dir / "chart_runtime_rating_compare.png")
    plot_compare_line(plt, votes_cmp, "votes_bin", "mean_rating_full", "mean_rating_hc", f"Mean Rating by Votes Bin: Full vs HC (numVotes>={args.high_vote_threshold})", "Votes Bin", "Mean Rating", out_dir / "chart_votes_bin_rating_compare.png")
    plot_decade_genre_heatmap(plt, decade_genre_top_count, out_dir / "chart_decade_genre_heatmap.png")
    plot_subset_comparison(plt, subset_summary, out_dir / "chart_subset_comparison.png")

    if not args.keep_legacy_output:
        cleanup_legacy_files(out_dir)

    print(f"Saved analysis directory: {out_dir}")
    print("Generated CSV files:")
    for f in [
        "genre_rating_summary.csv",
        "decade_summary.csv",
        "runtime_bin_summary.csv",
        "votes_bin_summary.csv",
        "decade_genre_top_count.csv",
        "decade_genre_top_rating.csv",
        "subset_comparison.csv",
        "robustness_summary.csv",
    ]:
        print(f"- {out_dir / f}")

    print("Generated chart files:")
    for f in [
        "chart_genre_rating_compare.png",
        "chart_decade_rating_compare.png",
        "chart_runtime_rating_compare.png",
        "chart_votes_bin_rating_compare.png",
        "chart_decade_genre_heatmap.png",
        "chart_subset_comparison.png",
    ]:
        print(f"- {out_dir / f}")


if __name__ == "__main__":
    main()