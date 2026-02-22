# Interpretation Guide

This file explains how to read the analysis charts and how to use robustness checks.

## Core Rule

- Blue = `Full` (all cleaned records, includes low-vote titles)
- Red = `High-Confidence` (`numVotes >= 1000`)
- If `changed_flag = False`: result is robust, Full is acceptable
- If `changed_flag = True`: result is quality-sensitive, prefer High-Confidence for conclusions

## Chart-by-Chart

### `chart_genre_rating_compare.png`
Genre mean rating comparison (Full vs High-Confidence).
- Close blue/red bars: stable genre conclusion
- Large gap: low-vote noise likely affects this genre

### `chart_decade_rating_compare.png`
Mean rating by decade (Full vs High-Confidence).
- Early decades often show larger Full-vs-HC gaps due to low-vote/legacy sampling effects
- If blue/red diverge, use red trend for final interpretation

### `chart_runtime_rating_compare.png`
Mean rating by runtime bins (Full vs High-Confidence).
- Middle bins usually more stable
- Extreme bins (very short/very long) are often more quality-sensitive

### `chart_votes_bin_rating_compare.png`
Mean rating by vote-count bins.
- `501-1K` is usually threshold-sensitive
- Higher-vote bins (1K+) often align better across Full/HC and are more reliable

### `chart_decade_genre_heatmap.png`
Decade x top-genre hotspot.
- Cell text = most frequent genre in that decade
- Darker color = higher title count for that decade's top genre

### `chart_subset_comparison.png`
Full vs High-Confidence summary.
- Left panel: mean rating
- Right panel: mean votes
- Typical reading: ratings may be close, but HC has far stronger consensus depth

## How to Decide Final Conclusion

1. Check `robustness_summary.csv` first.
2. For metrics with `changed_flag = False`, Full conclusion is generally acceptable.
3. For metrics with `changed_flag = True`, report High-Confidence result as primary and Full as context.

## Interview Short Answer

"I run Full vs High-Confidence comparisons and use `robustness_summary.csv` (`changed_flag`) to test stability. If the gap is large, I prioritize High-Confidence conclusions and treat Full as market-context only."