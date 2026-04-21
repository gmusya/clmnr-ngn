#!/usr/bin/env python3
"""
Compare two result directories and write diffs for mismatched files.

Usage example:
  python3 script/check_results_diff.py \
    --expected-dir results-main/gmusya/abc1234 \
    --actual-dir results-main/dub-otrezkov/def5678 \
    --diff-dir results-main/diffs/gmusya-abc1234_vs_dub-def5678
"""

from __future__ import annotations

import argparse
import difflib
from pathlib import Path
from typing import Dict, Iterable, List, Set


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare expected and actual result folders and write file diffs."
    )
    parser.add_argument(
        "--expected-dir",
        required=True,
        help="Directory with expected (correct) files.",
    )
    parser.add_argument(
        "--actual-dir",
        required=True,
        help="Directory with files to verify.",
    )
    parser.add_argument(
        "--diff-dir",
        required=True,
        help="Output directory where mismatch diffs will be stored.",
    )
    return parser.parse_args()


def collect_files(root: Path) -> Set[Path]:
    # Compare only per-query CSV artifacts (ignore logs and summary csv files).
    return {
        p.relative_to(root)
        for p in root.rglob("*.csv")
        if p.is_file() and p.name != "query_times.csv"
    }


def read_text_lines(path: Path) -> List[str]:
    # Results are expected to be text (csv/log/txt). Keep decoding permissive.
    return path.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)


def build_missing_message(rel_path: Path, side: str) -> str:
    return f"File is missing on {side} side: {rel_path}\n"


def write_diff_file(diff_root: Path, rel_path: Path, content: str) -> Path:
    out_path = diff_root / f"{rel_path}.diff"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    return out_path


def compare_dirs(expected_dir: Path, actual_dir: Path, diff_dir: Path) -> Dict[str, List[str]]:
    expected_files = collect_files(expected_dir)
    actual_files = collect_files(actual_dir)
    all_rel_paths = sorted(expected_files | actual_files)

    diff_paths: List[str] = []
    mismatches: List[str] = []

    for rel_path in all_rel_paths:
        expected_path = expected_dir / rel_path
        actual_path = actual_dir / rel_path

        if rel_path not in expected_files:
            text = build_missing_message(rel_path, "expected")
            diff_file = write_diff_file(diff_dir, rel_path, text)
            diff_paths.append(str(diff_file))
            mismatches.append(str(rel_path))
            continue

        if rel_path not in actual_files:
            text = build_missing_message(rel_path, "actual")
            diff_file = write_diff_file(diff_dir, rel_path, text)
            diff_paths.append(str(diff_file))
            mismatches.append(str(rel_path))
            continue

        if expected_path.read_bytes() == actual_path.read_bytes():
            continue

        expected_lines = read_text_lines(expected_path)
        actual_lines = read_text_lines(actual_path)
        diff_lines = difflib.unified_diff(
            expected_lines,
            actual_lines,
            fromfile=f"expected/{rel_path}",
            tofile=f"actual/{rel_path}",
            lineterm="",
        )
        diff_text = "".join(diff_lines)
        if not diff_text:
            diff_text = (
                f"Binary or undecodable content differs for file: {rel_path}\n"
            )
        diff_file = write_diff_file(diff_dir, rel_path, diff_text)
        diff_paths.append(str(diff_file))
        mismatches.append(str(rel_path))

    return {"mismatches": mismatches, "diff_files": diff_paths}


def main() -> int:
    args = parse_args()
    expected_dir = Path(args.expected_dir).resolve()
    actual_dir = Path(args.actual_dir).resolve()
    diff_dir = Path(args.diff_dir).resolve()

    if not expected_dir.is_dir():
        raise NotADirectoryError(f"expected directory not found: {expected_dir}")
    if not actual_dir.is_dir():
        raise NotADirectoryError(f"actual directory not found: {actual_dir}")

    result = compare_dirs(expected_dir, actual_dir, diff_dir)
    mismatches = result["mismatches"]
    diff_files = result["diff_files"]

    print(f"Compared directories:")
    print(f"  expected: {expected_dir}")
    print(f"  actual:   {actual_dir}")
    print(f"  checked files: {len(collect_files(expected_dir) | collect_files(actual_dir))}")
    print(f"  mismatches:    {len(mismatches)}")
    if diff_files:
        print("Diff files:")
        for path in diff_files:
            print(f"  {path}")

    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
