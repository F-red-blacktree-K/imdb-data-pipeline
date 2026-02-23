import argparse
import json
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


VOTE_BINS = [0, 1_000, 10_000, 50_000, 100_000, 500_000, float("inf")]
VOTE_LABELS = ["0-1K", "1K-10K", "10K-50K", "50K-100K", "100K-500K", "500K+"]
RUNTIME_BINS = [0, 60, 90, 120, 150, 180, 240, float("inf")]
RUNTIME_LABELS = ["0-60", "61-90", "91-120", "121-150", "151-180", "181-240", "241+"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Advanced analytics for IMDb Top1000 winner profile"
    )
    parser.add_argument("--top1000-csv", default="data/processed/top1000_clean.csv")
    parser.add_argument("--full-csv", default="data/processed/movies_merged.csv")
    parser.add_argument("--out-dir", default="data/analysis")
    parser.add_argument("--reports-dir", default="data/reports")
    parser.add_argument(
        "--enrich-from-stage1",
        action="store_true",
        help="Optionally enrich Stage2 with Stage1 full dataset for penetration analysis.",
    )
    return parser.parse_args()


def ensure_numeric(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def split_genres(df: pd.DataFrame) -> pd.DataFrame:
    if "genres" not in df.columns:
        return pd.DataFrame(columns=["tconst", "genre", "averageRating", "numVotes", "decade"])
    tmp = df[["tconst", "genres", "averageRating", "numVotes", "decade"]].copy()
    tmp["genres"] = tmp["genres"].fillna("Unknown")
    tmp["genre"] = tmp["genres"].astype(str).str.split(",")
    tmp = tmp.explode("genre")
    tmp["genre"] = tmp["genre"].fillna("Unknown").str.strip()
    return tmp


def plot_genre_structure(df: pd.DataFrame, out_path: Path) -> None:
    top = df.sort_values("title_count", ascending=False).head(15).copy()
    fig, ax = plt.subplots(figsize=(12, 7))
    ax.barh(top["genre"], top["title_count"], color="#1f77b4")
    ax.invert_yaxis()
    ax.set_title("Top1000 Genre Structure (by Count)")
    ax.set_xlabel("Title Count")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def plot_genre_penetration(df: pd.DataFrame, out_path: Path) -> None:
    top = df.sort_values("penetration_ratio", ascending=False).head(15).copy()
    fig, ax = plt.subplots(figsize=(12, 7))
    ax.barh(top["genre"], top["penetration_ratio"], color="#d62728")
    ax.invert_yaxis()
    ax.set_title("Genre Penetration into Top1000")
    ax.set_xlabel("Penetration Ratio (top1000_count / full_count)")
    x_max = float(top["penetration_ratio"].max()) if not top.empty else 1.0
    if x_max <= 0:
        x_max = 1.0
    ax.set_xlim(0, x_max * 1.18)
    for i, v in enumerate(top["penetration_ratio"]):
        ax.text(v + (x_max * 0.01), i, f"{v:.3f}", va="center", ha="left", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def plot_decade_genre_trends(pivot: pd.DataFrame, out_path: Path) -> None:
    if pivot.empty:
        return
    by_total = pivot.sum(axis=0).sort_values(ascending=False)
    chosen = by_total.head(6).index.tolist()
    view = pivot[chosen].copy()

    fig, ax = plt.subplots(figsize=(14, 8))
    x = view.index.astype(int)
    for col in view.columns:
        ax.plot(x, view[col].values, marker="o", linewidth=2, label=col)

    ax.set_title("Top1000: Genre Trend by Decade (Top 6 Genres)")
    ax.set_xlabel("Decade")
    ax.set_ylabel("Title Count")
    ax.set_xticks(x)
    ax.tick_params(axis="x", rotation=30)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), title="Genre")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def plot_votes_rating(df: pd.DataFrame, out_path: Path) -> None:
    fig, ax1 = plt.subplots(figsize=(11, 6))
    x = np.arange(len(df))
    ax1.plot(x, df["mean_rating"], marker="o", color="#1f77b4", label="Mean Rating")
    ax1.set_ylabel("Mean Rating", color="#1f77b4")
    ax1.set_xlabel("Votes Bin")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df["votes_bin"], rotation=30, ha="right")

    ax2 = ax1.twinx()
    ax2.bar(x, df["title_count"], alpha=0.25, color="#ff7f0e", label="Title Count")
    ax2.set_ylabel("Title Count", color="#ff7f0e")

    ax1.set_title("Top1000: Votes Bin vs Mean Rating (+ sample size)")
    l1 = ax1.legend(loc="upper left")
    l2 = ax2.legend(loc="upper right")
    ax1.add_artist(l1)
    ax2.add_artist(l2)

    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def plot_decade_rating(df: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 6))
    x = df["decade"].astype(int)
    ax.plot(x, df["mean_rating"], marker="o", linewidth=2, color="#1f77b4")
    ax.set_title("Top1000: Mean Rating by Decade")
    ax.set_xlabel("Decade")
    ax.set_ylabel("Mean Rating")
    ax.set_xticks(x)
    ax.tick_params(axis="x", rotation=30)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def plot_core_profile(df: pd.DataFrame, out_path: Path) -> None:
    metrics = df["metric"].unique().tolist()
    label_map = {
        "averageRating": "Mean Rating",
        "numVotes": "Mean Votes",
        "runtimeMinutes": "Mean Runtime",
    }
    n = len(metrics)
    fig, axes = plt.subplots(1, n, figsize=(5 * n, 5))
    if n == 1:
        axes = [axes]

    for i, m in enumerate(metrics):
        sub = df[df["metric"] == m]
        axes[i].bar(sub["group"], sub["value"], color=["#1f77b4", "#d62728"])
        axes[i].set_title(label_map.get(m, m))
        axes[i].set_xticks(range(len(sub["group"])))
        axes[i].set_xticklabels(["Others", "Core Winners"], rotation=12, ha="center")

    fig.suptitle("Core Winners vs Others")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def main() -> None:
    args = parse_args()

    top_csv = Path(args.top1000_csv)
    full_csv = Path(args.full_csv)
    out_dir = Path(args.out_dir)
    reports_dir = Path(args.reports_dir)

    out_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    if not top_csv.exists():
        raise SystemExit(
            f"Input not found: {top_csv}. Run `python crawl_top1000.py --force` and `python clean_crawl.py --force` first."
        )

    top = pd.read_csv(top_csv)
    ensure_numeric(top, ["rank", "year", "averageRating", "numVotes"])

    merged = top.copy()
    for col in ["title", "titleType", "runtimeMinutes", "genres", "averageRating", "numVotes", "title_url"]:
        if col not in merged.columns:
            merged[col] = pd.NA
    merged["startYear"] = pd.to_numeric(merged.get("year"), errors="coerce")

    full_loaded = False
    if args.enrich_from_stage1:
        if not full_csv.exists():
            raise SystemExit(
                f"Missing required Stage1 file for enrichment: {full_csv}.\n"
                "Run Stage1 first (`python download_datasets.py` + `python clean_merge.py --force`)\n"
                "or rerun without `--enrich-from-stage1` to use pure Stage2 analysis."
            )
        full = pd.read_csv(full_csv)
        ensure_numeric(full, ["startYear", "runtimeMinutes", "averageRating", "numVotes"])
        full_small = full[
            ["tconst", "titleType", "primaryTitle", "startYear", "runtimeMinutes", "genres", "averageRating", "numVotes"]
        ].drop_duplicates(subset=["tconst"]).copy()

        merged = merged.merge(full_small, on="tconst", how="left", suffixes=("_top", "_full"))

        if "title" in merged.columns and "primaryTitle" in merged.columns:
            merged["title"] = merged["title"].fillna("")
            merged["title"] = np.where(
                merged["title"].astype(str).str.len() > 0,
                merged["title"],
                merged["primaryTitle"].fillna(""),
            )
        if "startYear_full" in merged.columns:
            merged["startYear"] = merged["startYear_full"].fillna(merged["startYear"])
        if "averageRating_top" in merged.columns and "averageRating_full" in merged.columns:
            merged["averageRating"] = merged["averageRating_top"].fillna(merged["averageRating_full"])
        if "numVotes_top" in merged.columns and "numVotes_full" in merged.columns:
            merged["numVotes"] = merged["numVotes_top"].fillna(merged["numVotes_full"])
        if "titleType_full" in merged.columns:
            merged["titleType"] = merged["titleType_full"].fillna(merged.get("titleType"))
        if "runtimeMinutes_full" in merged.columns:
            merged["runtimeMinutes"] = merged["runtimeMinutes_full"].fillna(merged.get("runtimeMinutes"))
        if "genres_full" in merged.columns:
            merged["genres"] = merged["genres_full"].fillna(merged.get("genres"))

        full_loaded = True

    keep_cols = ["rank", "tconst", "title", "titleType", "startYear", "runtimeMinutes", "genres", "averageRating", "numVotes", "title_url"]
    merged = merged[keep_cols].copy()
    merged = merged.dropna(subset=["tconst"]).drop_duplicates(subset=["tconst"]).reset_index(drop=True)
    ensure_numeric(merged, ["rank", "startYear", "runtimeMinutes", "averageRating", "numVotes"])
    merged["decade"] = (np.floor(merged["startYear"] / 10) * 10).astype("Int64")

    generated_csv: list[str] = []
    generated_png: list[str] = []
    skipped: list[str] = []

    save_csv(merged, out_dir / "top1000_enriched.csv")
    generated_csv.append("top1000_enriched.csv")

    votes_df = merged.dropna(subset=["numVotes", "averageRating"]).copy()
    votes_df["votes_bin"] = pd.cut(votes_df["numVotes"], bins=VOTE_BINS, labels=VOTE_LABELS, include_lowest=True)
    votes_summary = (
        votes_df.groupby("votes_bin", observed=False)
        .agg(title_count=("tconst", "nunique"), mean_rating=("averageRating", "mean"), median_rating=("averageRating", "median"), mean_votes=("numVotes", "mean"))
        .reset_index()
    )
    save_csv(votes_summary, out_dir / "top1000_votes_rating_bins.csv")
    plot_votes_rating(votes_summary, out_dir / "chart_top1000_votes_rating.png")
    generated_csv.append("top1000_votes_rating_bins.csv")
    generated_png.append("chart_top1000_votes_rating.png")

    decade_df = merged.dropna(subset=["decade", "averageRating"]).copy()
    if decade_df.empty:
        skipped.append("decade_rating (startYear/year unavailable)")
    else:
        decade_rating = (
            decade_df.groupby("decade")
            .agg(title_count=("tconst", "nunique"), mean_rating=("averageRating", "mean"), mean_votes=("numVotes", "mean"))
            .reset_index()
            .sort_values("decade")
        )
        save_csv(decade_rating, out_dir / "top1000_decade_rating.csv")
        plot_decade_rating(decade_rating, out_dir / "chart_top1000_decade_rating.png")
        generated_csv.append("top1000_decade_rating.csv")
        generated_png.append("chart_top1000_decade_rating.png")

    q_rating = merged["averageRating"].quantile(0.75)
    q_votes = merged["numVotes"].quantile(0.75)
    merged["core_winner"] = (merged["averageRating"] >= q_rating) & (merged["numVotes"] >= q_votes)
    core_profile = (
        merged.groupby("core_winner")
        .agg(title_count=("tconst", "nunique"), mean_rating=("averageRating", "mean"), mean_votes=("numVotes", "mean"), mean_runtime=("runtimeMinutes", "mean"), mean_year=("startYear", "mean"))
        .reset_index()
    )
    core_profile["group"] = np.where(core_profile["core_winner"], "core_winners", "others")
    save_csv(core_profile, out_dir / "top1000_core_winner_profile.csv")
    generated_csv.append("top1000_core_winner_profile.csv")

    metrics = [
        {"metric": "averageRating", "col": "mean_rating"},
        {"metric": "numVotes", "col": "mean_votes"},
    ]
    if core_profile["mean_runtime"].notna().any():
        metrics.append({"metric": "runtimeMinutes", "col": "mean_runtime"})
    else:
        skipped.append("core_runtime_panel (runtimeMinutes unavailable)")

    core_plot_rows = []
    for m in metrics:
        for r in core_profile.itertuples():
            core_plot_rows.append({"group": r.group, "metric": m["metric"], "value": getattr(r, m["col"])})
    core_plot = pd.DataFrame(core_plot_rows)
    save_csv(core_plot, out_dir / "top1000_core_profile_plot.csv")
    plot_core_profile(core_plot, out_dir / "chart_top1000_core_profile.png")
    generated_csv.append("top1000_core_profile_plot.csv")
    generated_png.append("chart_top1000_core_profile.png")

    runtime_df = merged.dropna(subset=["runtimeMinutes", "averageRating"]).copy()
    if runtime_df.empty:
        skipped.append("runtime_bin_analysis (runtimeMinutes unavailable)")
    else:
        runtime_df["runtime_bin"] = pd.cut(runtime_df["runtimeMinutes"], bins=RUNTIME_BINS, labels=RUNTIME_LABELS, include_lowest=True)
        runtime_summary = (
            runtime_df.groupby("runtime_bin", observed=False)
            .agg(title_count=("tconst", "nunique"), mean_rating=("averageRating", "mean"), mean_votes=("numVotes", "mean"))
            .reset_index()
        )
        save_csv(runtime_summary, out_dir / "top1000_runtime_rating_bins.csv")
        generated_csv.append("top1000_runtime_rating_bins.csv")

    top_genres = split_genres(merged)
    valid_genres = top_genres[~top_genres["genre"].isin(["", "Unknown"])].copy()
    if valid_genres.empty:
        skipped.append("genre_structure / genre_trend (genres unavailable)")
    else:
        genre_structure = (
            valid_genres.groupby("genre", dropna=False)
            .agg(title_count=("tconst", "nunique"), mean_rating=("averageRating", "mean"), mean_votes=("numVotes", "mean"))
            .reset_index()
            .sort_values("title_count", ascending=False)
        )
        save_csv(genre_structure, out_dir / "top1000_genre_structure.csv")
        plot_genre_structure(genre_structure, out_dir / "chart_top1000_genre_structure.png")
        generated_csv.append("top1000_genre_structure.csv")
        generated_png.append("chart_top1000_genre_structure.png")

        tg2 = valid_genres.dropna(subset=["decade"]).copy()
        trend_data = (
            tg2.groupby(["decade", "genre"], dropna=False)
            .agg(title_count=("tconst", "nunique"))
            .reset_index()
            .pivot(index="decade", columns="genre", values="title_count")
            .fillna(0)
            .sort_index()
        )
        if not trend_data.empty:
            plot_decade_genre_trends(trend_data, out_dir / "chart_top1000_decade_genre_trend.png")
            generated_png.append("chart_top1000_decade_genre_trend.png")

        decade_genre_count = (
            tg2.groupby(["decade", "genre"], dropna=False)
            .agg(title_count=("tconst", "nunique"), mean_rating=("averageRating", "mean"))
            .reset_index()
        )
        top_by_count = decade_genre_count.sort_values(["decade", "title_count", "mean_rating"], ascending=[True, False, False]).drop_duplicates("decade")
        top_by_count = top_by_count.rename(columns={"genre": "top_genre_by_count", "title_count": "top_genre_count", "mean_rating": "top_genre_mean_rating"})
        save_csv(top_by_count, out_dir / "top1000_decade_genre_hotspots.csv")
        generated_csv.append("top1000_decade_genre_hotspots.csv")

    if full_loaded:
        full = pd.read_csv(full_csv)
        ensure_numeric(full, ["startYear", "runtimeMinutes", "averageRating", "numVotes"])
        full_genres = split_genres(full.assign(decade=(np.floor(pd.to_numeric(full["startYear"], errors="coerce") / 10) * 10).astype("Int64")))
        full_valid = full_genres[~full_genres["genre"].isin(["", "Unknown"])].copy()

        if not full_valid.empty and not valid_genres.empty:
            full_counts = full_valid.groupby("genre", dropna=False).agg(full_count=("tconst", "nunique")).reset_index()
            top_counts = valid_genres.groupby("genre", dropna=False).agg(top1000_count=("tconst", "nunique")).reset_index()
            pen = top_counts.merge(full_counts, on="genre", how="left")
            pen["penetration_ratio"] = pen["top1000_count"] / pen["full_count"].replace(0, np.nan)
            pen["top1000_share"] = pen["top1000_count"] / max(1, merged["tconst"].nunique())
            pen = pen.sort_values("penetration_ratio", ascending=False)
            save_csv(pen, out_dir / "top1000_genre_penetration.csv")
            plot_genre_penetration(pen, out_dir / "chart_top1000_genre_penetration.png")
            generated_csv.append("top1000_genre_penetration.csv")
            generated_png.append("chart_top1000_genre_penetration.png")
    else:
        skipped.append("genre_penetration (enable with --enrich-from-stage1)")

    strategy_rows = []
    gp_path = out_dir / "top1000_genre_penetration.csv"
    if gp_path.exists():
        gp = pd.read_csv(gp_path)
        top3 = gp.sort_values("penetration_ratio", ascending=False).head(3)["genre"].tolist()
        strategy_rows.append({"recommendation_key": "target_genres_by_penetration", "value": ", ".join(top3), "reason": "Genres with highest conversion into Top1000 given market base."})

    rt_path = out_dir / "top1000_runtime_rating_bins.csv"
    if rt_path.exists():
        rt = pd.read_csv(rt_path)
        rt = rt[rt["title_count"] >= 20].sort_values("mean_rating", ascending=False)
        if not rt.empty:
            strategy_rows.append({"recommendation_key": "preferred_runtime_bin", "value": str(rt.iloc[0]["runtime_bin"]), "reason": "Runtime bin with strongest rating under minimum sample threshold."})

    strategy_rows.append({"recommendation_key": "high_consensus_vote_threshold", "value": str(int(q_votes)) if not math.isnan(q_votes) else "N/A", "reason": "Top1000 upper-quartile vote cutoff used to define high-consensus winners."})
    strategy = pd.DataFrame(strategy_rows)
    save_csv(strategy, out_dir / "top1000_strategy_recommendation.csv")
    generated_csv.append("top1000_strategy_recommendation.csv")

    summary = {
        "input_top1000": str(top_csv),
        "input_full": str(full_csv) if args.enrich_from_stage1 else None,
        "enrich_from_stage1": bool(args.enrich_from_stage1),
        "full_dataset_loaded": full_loaded,
        "rows_top1000": int(len(top)),
        "rows_enriched": int(len(merged)),
        "core_rating_threshold_p75": round(float(q_rating), 4) if pd.notna(q_rating) else None,
        "core_votes_threshold_p75": int(q_votes) if pd.notna(q_votes) else None,
        "generated_csv": generated_csv,
        "generated_png": generated_png,
        "skipped": skipped,
        "outputs_dir": str(out_dir),
    }
    with (reports_dir / "top1000_analysis_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("Generated CSV files:")
    for name in generated_csv:
        print(f"- {out_dir / name}")

    print("Generated chart files:")
    for name in generated_png:
        print(f"- {out_dir / name}")

    if skipped:
        print("Skipped items:")
        for s in skipped:
            print(f"- {s}")


if __name__ == "__main__":
    main()



