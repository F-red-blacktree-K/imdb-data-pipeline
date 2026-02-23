import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run IMDb pipeline by stage (Stage1, Stage2, or all)."
    )
    parser.add_argument(
        "--stage",
        choices=["all", "stage1", "stage2"],
        default="all",
        help="Which stage(s) to run.",
    )
    parser.add_argument(
        "--enrich-stage2",
        action="store_true",
        help="Run Stage2 analysis with Stage1 enrichment (--enrich-from-stage1).",
    )
    return parser.parse_args()


def run_step(step_name: str, cmd: list[str]) -> None:
    print(f"\n[START] {step_name}")
    print("[CMD]", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise SystemExit(f"[FAIL] {step_name} (exit code={result.returncode})")
    print(f"[OK] {step_name}")


def main() -> None:
    args = parse_args()

    root = Path(__file__).resolve().parent
    py = sys.executable

    stage1 = [
        ("Stage1: Download datasets", [py, str(root / "download_datasets.py")]),
        ("Stage1: Clean and merge", [py, str(root / "clean_merge.py"), "--force"]),
        ("Stage1: Analyze core", [py, str(root / "analyze_core.py")]),
    ]

    stage2_analyze_cmd = [py, str(root / "analyze_top1000.py")]
    if args.enrich_stage2:
        stage2_analyze_cmd.append("--enrich-from-stage1")

    stage2 = [
        ("Stage2: Crawl Top1000", [py, str(root / "crawl_top1000.py"), "--force"]),
        ("Stage2: Clean crawl", [py, str(root / "clean_crawl.py"), "--force"]),
        ("Stage2: Analyze Top1000", stage2_analyze_cmd),
    ]

    if args.stage in ("all", "stage1"):
        for name, cmd in stage1:
            run_step(name, cmd)

    if args.stage in ("all", "stage2"):
        for name, cmd in stage2:
            run_step(name, cmd)

    print("\n[DONE] Pipeline finished successfully.")


if __name__ == "__main__":
    main()
