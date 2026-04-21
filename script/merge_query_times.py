#!/usr/bin/env python3
"""
Merge multiple query_times.csv files into a single comparison table.

Expected source layout:
  <results_root>/<github_user>/<commit_hash>/query_times.csv

Output format:
  query,<github_user>/<commit_hash>,<github_user>/<commit_hash>,...
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge multiple query_times.csv files into one table."
    )
    parser.add_argument(
        "--results-root",
        default="results-main",
        help="Root directory that contains <user>/<commit>/query_times.csv trees.",
    )
    parser.add_argument(
        "--output",
        default="results-main/query_times_merged.csv",
        help="Path to output merged CSV file.",
    )
    parser.add_argument(
        "--expected-results-dir",
        default=None,
        help=(
            "Directory with expected query_<N>.csv files. "
            "When provided, mismatched answers are marked as WA instead of time."
        ),
    )
    return parser.parse_args()


def discover_query_time_files(results_root: Path) -> List[Tuple[str, Path]]:
    files: List[Tuple[str, Path]] = []
    for file_path in results_root.rglob("query_times.csv"):
        rel = file_path.relative_to(results_root)
        if len(rel.parts) < 3:
            # We expect at least <user>/<commit>/query_times.csv
            continue
        user = rel.parts[-3]
        commit = rel.parts[-2]
        run_name = f"{user}/{commit}"
        files.append((run_name, file_path))

    # Deterministic output order
    files.sort(key=lambda x: x[0])
    return files


def read_query_times(csv_path: Path) -> Dict[str, str]:
    result: Dict[str, str] = {}
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if "query" not in reader.fieldnames or "time_ms" not in reader.fieldnames:
            raise ValueError(
                f"{csv_path}: expected columns 'query' and 'time_ms', got {reader.fieldnames}"
            )

        for row in reader:
            query = (row.get("query") or "").strip()
            time_ms = (row.get("time_ms") or "").strip()
            if not query:
                continue
            result[query] = time_ms
    return result


def expected_query_file(expected_results_dir: Path, query: str) -> Path:
    return expected_results_dir / f"query_{query}.csv"


def actual_query_file(run_dir: Path, query: str) -> Path:
    return run_dir / f"query_{query}.csv"


def apply_wa_verdicts(
    run_data: Dict[str, Dict[str, str]],
    run_dirs: Dict[str, Path],
    expected_results_dir: Path,
) -> None:
    for run_name, per_query_times in run_data.items():
        run_dir = run_dirs[run_name]
        for query in list(per_query_times.keys()):
            expected_path = expected_query_file(expected_results_dir, query)
            actual_path = actual_query_file(run_dir, query)
            if not expected_path.is_file() or not actual_path.is_file():
                per_query_times[query] = "WA"
                continue
            if expected_path.read_bytes() != actual_path.read_bytes():
                per_query_times[query] = "WA"


def merge_runs(run_data: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    all_queries = sorted(
        {query for per_run in run_data.values() for query in per_run.keys()},
        key=lambda q: int(q) if q.isdigit() else q,
    )
    run_names = sorted(run_data.keys())

    rows: List[Dict[str, str]] = []
    for query in all_queries:
        row: Dict[str, str] = {"query": query}
        for run_name in run_names:
            row[run_name] = run_data[run_name].get(query, "")
        rows.append(row)
    return rows


def write_merged_csv(
    output_path: Path, run_names: List[str], rows: List[Dict[str, str]]
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["query", *run_names]
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    results_root = Path(args.results_root).resolve()
    output_path = Path(args.output).resolve()
    expected_results_dir = (
        Path(args.expected_results_dir).resolve()
        if args.expected_results_dir is not None
        else None
    )

    if not results_root.exists():
        raise FileNotFoundError(f"results root not found: {results_root}")

    discovered = discover_query_time_files(results_root)
    if not discovered:
        raise FileNotFoundError(
            f"no query_times.csv found under results root: {results_root}"
        )

    run_data: Dict[str, Dict[str, str]] = {}
    run_dirs: Dict[str, Path] = {}
    for run_name, csv_path in discovered:
        if run_name in run_data:
            raise ValueError(
                f"duplicate run name '{run_name}' detected while scanning {results_root}"
            )
        run_data[run_name] = read_query_times(csv_path)
        run_dirs[run_name] = csv_path.parent

    if expected_results_dir is not None:
        if not expected_results_dir.is_dir():
            raise NotADirectoryError(
                f"expected results directory not found: {expected_results_dir}"
            )
        apply_wa_verdicts(run_data, run_dirs, expected_results_dir)

    run_names = sorted(run_data.keys())
    rows = merge_runs(run_data)
    write_merged_csv(output_path, run_names, rows)

    print(f"Merged {len(run_names)} files into {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
