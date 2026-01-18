#!/usr/bin/env python3
"""
Compare benchmark results between columnar-engine and DuckDB.
Usage: ./compare_bench.py [columnar_log] [duckdb_log]

If no arguments provided, runs both benchmarks and compares.
"""

import sys
import re
import subprocess
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent

def parse_columnar_log(log_content: str) -> dict[int, float]:
    """Parse columnar-engine log output for query timings."""
    results = {}
    # Pattern: Q0 completed in 123 ms
    pattern = r'Q(\d+)\s+completed\s+in\s+(\d+)\s*ms'
    for match in re.finditer(pattern, log_content):
        query_num = int(match.group(1))
        time_ms = float(match.group(2))
        results[query_num] = time_ms
    return results

def parse_duckdb_log(log_content: str) -> dict[int, float]:
    """Parse DuckDB timer output for query timings."""
    results = {}
    # Try format 1: Q0 completed in 123 ms (same as columnar)
    pattern1 = r'Q(\d+)\s+completed\s+in\s+(\d+)\s*ms'
    for match in re.finditer(pattern1, log_content):
        query_num = int(match.group(1))
        time_ms = float(match.group(2))
        results[query_num] = time_ms
    
    if results:
        return results
    
    # Try format 2: Run Time (s): real 0.123 user 0.100 sys 0.020
    times = re.findall(r'Run Time \(s\): real ([\d.]+)', log_content)
    for i, time_str in enumerate(times):
        results[i] = float(time_str) * 1000  # Convert to ms
    return results

def format_speedup(speedup: float) -> str:
    """Format speedup ratio with color indication."""
    if speedup >= 1.0:
        return f"\033[32m{speedup:.2f}x faster\033[0m"  # Green
    else:
        return f"\033[31m{1/speedup:.2f}x slower\033[0m"  # Red

def print_comparison(columnar_results: dict, duckdb_results: dict, use_color: bool = True):
    """Print comparison table."""
    all_queries = sorted(set(columnar_results.keys()) | set(duckdb_results.keys()))
    
    print("\n" + "=" * 80)
    print("BENCHMARK COMPARISON: columnar-engine vs DuckDB (single-thread)")
    print("=" * 80)
    print(f"{'Query':<8} {'columnar-engine':>18} {'DuckDB (1 thread)':>18} {'Ratio':>20}")
    print("-" * 80)
    
    total_columnar = 0.0
    total_duckdb = 0.0
    
    for q in all_queries:
        col_time = columnar_results.get(q)
        duck_time = duckdb_results.get(q)
        
        col_str = f"{col_time:.0f} ms" if col_time is not None else "N/A"
        duck_str = f"{duck_time:.0f} ms" if duck_time is not None else "N/A"
        
        if col_time is not None and duck_time is not None and duck_time > 0:
            total_columnar += col_time
            total_duckdb += duck_time
            ratio = duck_time / col_time
            if use_color:
                ratio_str = format_speedup(ratio)
            else:
                ratio_str = f"{ratio:.2f}x" + (" faster" if ratio >= 1 else " slower")
        else:
            ratio_str = "N/A"
        
        print(f"Q{q:<7} {col_str:>18} {duck_str:>18} {ratio_str:>20}")
    
    print("-" * 80)
    
    if total_columnar > 0 and total_duckdb > 0:
        total_ratio = total_duckdb / total_columnar
        if use_color:
            total_ratio_str = format_speedup(total_ratio)
        else:
            total_ratio_str = f"{total_ratio:.2f}x"
        print(f"{'TOTAL':<8} {total_columnar:>15.0f} ms {total_duckdb:>15.0f} ms {total_ratio_str:>20}")
        
        # Geometric mean of ratios
        ratios = []
        for q in all_queries:
            if q in columnar_results and q in duckdb_results:
                col_time = columnar_results[q]
                duck_time = duckdb_results[q]
                if col_time > 0 and duck_time > 0:
                    ratios.append(duck_time / col_time)
        
        if ratios:
            import math
            geomean = math.exp(sum(math.log(r) for r in ratios) / len(ratios))
            if use_color:
                geomean_str = format_speedup(geomean)
            else:
                geomean_str = f"{geomean:.2f}x"
            print(f"{'GEOMEAN':<8} {'':>18} {'':>18} {geomean_str:>20}")
    
    print("=" * 80)

def main():
    if len(sys.argv) >= 3:
        # Read from provided log files
        with open(sys.argv[1], 'r') as f:
            columnar_log = f.read()
        with open(sys.argv[2], 'r') as f:
            duckdb_log = f.read()
    else:
        print("Usage: ./compare_bench.py <columnar_log> <duckdb_log>")
        print("\nExample workflow:")
        print("  1. Run your engine and save output:")
        print("     ./build/build/Release/clickbench/ngn-clickbench-run \\")
        print("       --input /path/to/hits.clmnr \\")
        print("       --schema hits.schema \\")
        print("       --output_dir output/ 2>&1 | tee columnar_bench.log")
        print("")
        print("  2. Run DuckDB benchmark:")
        print("     ./script/bench_duckdb.sh")
        print("")
        print("  3. Compare:")
        print("     ./script/compare_bench.py columnar_bench.log output-duckdb/full_output.txt")
        return 1
    
    columnar_results = parse_columnar_log(columnar_log)
    duckdb_results = parse_duckdb_log(duckdb_log)
    
    if not columnar_results:
        print("Warning: No results parsed from columnar-engine log")
    if not duckdb_results:
        print("Warning: No results parsed from DuckDB log")
    
    print_comparison(columnar_results, duckdb_results)
    
    # Also save to CSV for further analysis
    results_dir = PROJECT_DIR / "benchmark-results"
    results_dir.mkdir(exist_ok=True)
    output_csv = results_dir / "benchmark_comparison.csv"
    with open(output_csv, 'w') as f:
        f.write("Query,columnar_engine_ms,duckdb_ms,ratio\n")
        all_queries = sorted(set(columnar_results.keys()) | set(duckdb_results.keys()))
        for q in all_queries:
            col_time = columnar_results.get(q, "")
            duck_time = duckdb_results.get(q, "")
            if col_time and duck_time and col_time > 0:
                ratio = duck_time / col_time
            else:
                ratio = ""
            f.write(f"Q{q},{col_time},{duck_time},{ratio}\n")
    
    print(f"\nCSV saved to: {output_csv}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
