# Analysis Interpretation Guide

This document standardizes how analysis outputs are interpreted for reporting and interview discussion.

## Interpretation Policy

- **Stage 1 (Dataset-wide)** focuses on market-level patterns.
- **Stage 2 (Top1000)** focuses on winner-profile diagnostics.
- For robustness comparisons:
  - `Full`: all cleaned records.
  - `High-Confidence`: higher-vote subset.
  - If the gap is material, prioritize **High-Confidence** for decision-oriented conclusions.

---

## Stage 1: Dataset-wide Analysis

### `chart_genre_rating_compare.png`
- **Analytical objective**: Compare mean rating by genre under Full vs High-Confidence scope.
- **Business question**: Are genre-level conclusions stable after removing low-consensus titles?
- **Interpretation**: Small gaps indicate stable ranking; large gaps suggest sensitivity to low-vote noise.

### `chart_decade_rating_compare.png`
- **Analytical objective**: Evaluate long-run rating trend by decade.
- **Business question**: Is decade trend structurally consistent across data-quality tiers?
- **Interpretation**: Early decades may diverge due to sparse legacy voting; recent decades are typically more stable.

### `chart_runtime_rating_compare.png`
- **Analytical objective**: Measure rating behavior across runtime bins.
- **Business question**: Does runtime preference remain consistent after quality control?
- **Interpretation**: Mid-runtime bins are often stable; extreme bins are sample-sensitive.

### `chart_votes_bin_rating_compare.png`
- **Analytical objective**: Relate vote-scale segments to mean rating.
- **Business question**: Are high scores supported by broad audience consensus?
- **Interpretation**: Convergence in high-vote bins implies stronger reliability.

### `chart_decade_genre_heatmap.png`
- **Analytical objective**: Show decade-level dominant genre intensity.
- **Business question**: Which genre clusters dominate each era at market scope?
- **Interpretation**: Darker cells imply higher concentration; use as a macro-structure map.

### `chart_subset_comparison.png`
- **Analytical objective**: Summarize Full vs High-Confidence aggregate differences.
- **Business question**: How much does consensus filtering change global averages?
- **Interpretation**: Similar mean rating with very different vote depth means similar central tendency but different confidence strength.

---

## Stage 2: Top1000 Winner-Profile Analysis

### `chart_top1000_genre_structure.png`
- **Analytical objective**: Quantify Top1000 genre composition by count.
- **Business question**: Is Top1000 dominated by a narrow genre set?
- **Interpretation**: Strong concentration indicates winner clustering.

### `chart_top1000_decade_genre_trend.png`
- **Analytical objective**: Track top-genre count trajectories across decades.
- **Business question**: How does audience preference shift over time among winner genres?
- **Interpretation**: Persistent upward lines suggest durable demand; crossovers suggest preference rotation.

### `chart_top1000_votes_rating.png`
- **Analytical objective**: Jointly inspect mean rating and sample size across vote bins.
- **Business question**: Are top-rated segments also high-consensus segments?
- **Interpretation**: High rating with large sample size is a stronger signal than high rating in thin bins.

### `chart_top1000_core_profile.png`
- **Analytical objective**: Compare `Core Winners` vs `Others` on mean rating, vote volume, and runtime.
- **Business question**: What differentiates stable winners from the rest?
- **Interpretation**: If vote-volume gap is largest, the strongest discriminator is **consensus depth**, not rating alone.
- **Important nuance**: This is **association**, not causation.

### `chart_top1000_genre_penetration.png` *(optional; requires Stage1 enrichment)*
- **Analytical objective**: Measure genre penetration ratio (`top1000_count / full_count`).
- **Business question**: Which genres most efficiently convert into winners relative to market base?
- **Interpretation**: Better than raw share because it controls for base-size effects.
- **Run condition**: Generated only with `python analyze_top1000.py --enrich-from-stage1`.

---

## Recommended Reporting Sequence

1. Validate stability using Stage 1 robustness outputs.
2. Present Stage 2 winner-profile diagnostics.
3. Convert findings into strategy using `top1000_strategy_recommendation.csv`.

## Executive Summary

> We separate market-level structure (Stage 1) from winner-level structure (Stage 2).
> Stage 1 provides robust population-level signals, while Stage 2 provides targeted winner-profile diagnostics for strategy recommendations.
