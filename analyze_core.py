import argparse
from pathlib import Path

import pandas as pd
from matplotlib.ticker import FormatStrFormatter, MultipleLocator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate interpretable IMDb analysis reports from merged data")
    parser.add_argument("--input-csv", default="data/processed/movies_merged.csv", help="Input merged CSV")
    parser.add_argument("--out-dir", default="data/analysis", help="Output directory for reports/charts")
    parser.add_argument("--high-vote-threshold", type=int, default=1000, help="Threshold for high-confidence subset")
    parser.add_argument("--min-genre-titles", type=int, default=30, help="Minimum title count for genre ranking chart")
    parser.add_argument("--min-decade-titles", type=int, default=50, help="Minimum title count for decade chart")
    return parser.parse_args()


def save_df(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def build_genre_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    gdf = df[["tconst", "genres", "averageRating", "numVotes"]].dropna(subset=["genres", "averageRating"])
    for _, row in gdf.iterrows():
        for g in str(row["genres"]).split(","):
            g = g.strip()
            if g:
                rows.append({"genre": g, "averageRating": row["averageRating"], "numVotes": row["numVotes"]})

    if not rows:
        return pd.DataFrame(columns=["genre", "title_count", "mean_rating", "mean_votes"])

    return (
        pd.DataFrame(rows)
        .groupby("genre", as_index=False)
        .agg(
            title_count=("averageRating", "count"),
            mean_rating=("averageRating", "mean"),
            mean_votes=("numVotes", "mean"),
        )
        .sort_values(["mean_rating", "title_count"], ascending=[False, False])
    )


def build_decade_summary(df: pd.DataFrame) -> pd.DataFrame:
    decade_df = df.dropna(subset=["startYear", "averageRating"]).copy()
    if decade_df.empty:
        return pd.DataFrame(columns=["decade", "title_count", "mean_rating", "mean_votes"])

    decade_df["decade"] = (decade_df["startYear"] // 10 * 10).astype("Int64")
    return (
        decade_df.groupby("decade", as_index=False)
        .agg(
            title_count=("tconst", "count"),
            mean_rating=("averageRating", "mean"),
            mean_votes=("numVotes", "mean"),
        )
        .sort_values("decade")
    )


def build_runtime_bin_summary(df: pd.DataFrame) -> pd.DataFrame:
    runtime_df = df[["runtimeMinutes", "averageRating", "numVotes"]].dropna(subset=["runtimeMinutes", "averageRating"])
    if runtime_df.empty:
        return pd.DataFrame(columns=["runtime_bin", "title_count", "mean_rating", "mean_votes"])

    bins = [0, 60, 90, 120, 150, 180, 240, 9999]
    labels = ["0-60", "61-90", "91-120", "121-150", "151-180", "181-240", "241+"]
    runtime_df["runtime_bin"] = pd.cut(runtime_df["runtimeMinutes"], bins=bins, labels=labels, include_lowest=True)

    return (
        runtime_df.groupby("runtime_bin", as_index=False, observed=False)
        .agg(
            title_count=("averageRating", "count"),
            mean_rating=("averageRating", "mean"),
            mean_votes=("numVotes", "mean"),
        )
        .dropna(subset=["runtime_bin"])
    )


def main() -> None:
    args = parse_args()

    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "matplotlib is required for analyze_core.py. Install dependencies first: pip install -r requirements.txt"
        ) from exc

    in_path = Path(args.input_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(f"Missing input CSV: {in_path}")

    df = pd.read_csv(in_path)
    for col in ["startYear", "endYear", "runtimeMinutes", "averageRating", "numVotes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 1) Genre analysis: what genres tend to score higher?
    genre_summary = build_genre_summary(df)
    save_df(genre_summary, out_dir / "genre_rating_summary.csv")
    genre_filtered = genre_summary[genre_summary["title_count"] >= args.min_genre_titles].copy()
    save_df(genre_filtered, out_dir / "genre_rating_summary_filtered.csv")

    # 2) Decade analysis: how ratings/popularity change over time?
    decade_summary = build_decade_summary(df)
    save_df(decade_summary, out_dir / "decade_summary.csv")
    decade_filtered = decade_summary[decade_summary["title_count"] >= args.min_decade_titles].copy()
    save_df(decade_filtered, out_dir / "decade_summary_filtered.csv")

    # 3) Runtime analysis: relation between movie length and rating
    runtime_bin_summary = build_runtime_bin_summary(df)
    save_df(runtime_bin_summary, out_dir / "runtime_bin_summary.csv")

    corr_df = df[["runtimeMinutes", "averageRating"]].dropna()
    corr = corr_df["runtimeMinutes"].corr(corr_df["averageRating"]) if len(corr_df) > 1 else None
    corr_summary = pd.DataFrame([{"metric": "pearson_runtime_vs_rating", "value": corr}])
    save_df(corr_summary, out_dir / "runtime_rating_correlation.csv")

    # 4) Subset comparison: overall vs high-vote subset
    full_stats = {
        "subset": "full",
        "rows": int(len(df)),
        "mean_rating": float(df["averageRating"].mean(skipna=True)),
        "mean_votes": float(df["numVotes"].mean(skipna=True)),
    }
    high = df[df["numVotes"].fillna(0) >= args.high_vote_threshold]
    high_stats = {
        "subset": f"numVotes>={args.high_vote_threshold}",
        "rows": int(len(high)),
        "mean_rating": float(high["averageRating"].mean(skipna=True)),
        "mean_votes": float(high["numVotes"].mean(skipna=True)),
    }
    subset_summary = pd.DataFrame([full_stats, high_stats])
    save_df(subset_summary, out_dir / "subset_comparison.csv")

    # Chart A: top genres (only sufficiently large genres)
    plot_genre = genre_filtered.sort_values("mean_rating", ascending=False).head(15)
    if plot_genre.empty:
        plot_genre = genre_summary.sort_values("title_count", ascending=False).head(15)

    plt.figure(figsize=(11, 6))
    plt.barh(plot_genre["genre"], plot_genre["mean_rating"])
    ax = plt.gca()
    ax.xaxis.set_major_locator(MultipleLocator(0.5))
    ax.xaxis.set_major_formatter(FormatStrFormatter("%.1f"))
    plt.gca().invert_yaxis()
    plt.title("Top Genres by Mean Rating")
    plt.xlabel("Mean Rating")
    plt.tight_layout()
    plt.savefig(out_dir / "chart_top_genres.png", dpi=150)
    plt.close()

    # Chart B: decade rating + sample size (dual axis)
    if len(decade_filtered) >= 2:
        x = decade_filtered["decade"].astype(str)
        y_rating = decade_filtered["mean_rating"]
        y_count = decade_filtered["title_count"]

        fig, ax1 = plt.subplots(figsize=(12, 6))
        ax1.plot(x, y_rating, marker="o", color="#1f77b4")
        ax1.set_xlabel("Decade")
        ax1.set_ylabel("Mean Rating", color="#1f77b4")
        ax1.tick_params(axis="y", labelcolor="#1f77b4")
        ax1.set_xticks(range(len(x)))
        ax1.set_xticklabels(x, rotation=45)

        ax2 = ax1.twinx()
        ax2.bar(range(len(x)), y_count, alpha=0.25, color="#ff7f0e")
        ax2.set_ylabel("Title Count", color="#ff7f0e")
        ax2.tick_params(axis="y", labelcolor="#ff7f0e")

        plt.title("Mean Rating by Decade (with Sample Size)")
        fig.tight_layout()
        plt.savefig(out_dir / "chart_decade_rating.png", dpi=150)
        plt.close(fig)
    else:
        # Keep a file output but make the reason explicit in the figure.
        plt.figure(figsize=(10, 4))
        plt.axis("off")
        plt.text(
            0.02,
            0.5,
            "Not enough decade groups to plot trend.\n"
            f"Current groups meeting threshold ({args.min_decade_titles}): {len(decade_filtered)}\n"
            "Tip: rerun clean_merge.py without --limit, then rerun analysis.",
            fontsize=11,
        )
        plt.tight_layout()
        plt.savefig(out_dir / "chart_decade_rating.png", dpi=150)
        plt.close()

    # Chart C: runtime bins are easier to interpret than dense scatter
    runtime_plot = runtime_bin_summary[runtime_bin_summary["title_count"] > 0].copy()
    plt.figure(figsize=(11, 6))
    plt.bar(runtime_plot["runtime_bin"].astype(str), runtime_plot["mean_rating"])
    plt.title("Mean Rating by Runtime Bin")
    plt.xlabel("Runtime Bin (minutes)")
    plt.ylabel("Mean Rating")
    plt.tight_layout()
    plt.savefig(out_dir / "chart_runtime_vs_rating.png", dpi=150)
    plt.close()

    print(f"Saved analysis directory: {out_dir}")
    print("Generated CSV files:")
    for f in [
        "genre_rating_summary.csv",
        "genre_rating_summary_filtered.csv",
        "decade_summary.csv",
        "decade_summary_filtered.csv",
        "runtime_bin_summary.csv",
        "runtime_rating_correlation.csv",
        "subset_comparison.csv",
    ]:
        print(f"- {out_dir / f}")

    print("Generated chart files:")
    for f in [
        "chart_top_genres.png",
        "chart_decade_rating.png",
        "chart_runtime_vs_rating.png",
    ]:
        print(f"- {out_dir / f}")


if __name__ == "__main__":
    main()