#!/usr/bin/env python3
"""Generate benchmark visualization charts from CSV time-series data."""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path


def load_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    df["elapsed_min"] = df["elapsed_sec"] / 60.0
    return df


def plot_throughput(df: pd.DataFrame, output_dir: Path):
    fig, ax = plt.subplots(figsize=(16, 6))

    for op in df["operation"].unique():
        op_df = df[df["operation"] == op]
        ax.plot(op_df["elapsed_min"], op_df["throughput_ops_sec"], label=op, alpha=0.8)

    ax.set_xlabel("Elapsed Time (minutes)")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_title("Throughput Over Time by Operation")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)

    phases = df.groupby("phase")["elapsed_min"].agg(["min", "max"])
    colors = plt.cm.Set3.colors
    for i, (phase, row) in enumerate(phases.iterrows()):
        ax.axvspan(row["min"], row["max"], alpha=0.1, color=colors[i % len(colors)], label=f"Phase: {phase}")

    plt.tight_layout()
    plt.savefig(output_dir / "throughput.png", dpi=150)
    plt.close()


def plot_latency_percentiles(df: pd.DataFrame, output_dir: Path):
    for op in df["operation"].unique():
        op_df = df[df["operation"] == op]
        if op_df.empty:
            continue

        fig, ax = plt.subplots(figsize=(16, 6))

        ax.plot(op_df["elapsed_min"], op_df["p50_ms"], label="P50", linewidth=2)
        ax.plot(op_df["elapsed_min"], op_df["p95_ms"], label="P95", linewidth=1.5)
        ax.plot(op_df["elapsed_min"], op_df["p99_ms"], label="P99", linewidth=1)
        ax.plot(op_df["elapsed_min"], op_df["p999_ms"], label="P99.9", linewidth=0.8, linestyle="--")

        ax.set_xlabel("Elapsed Time (minutes)")
        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"Latency Percentiles - {op}")
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.set_yscale("log")

        plt.tight_layout()
        safe_name = op.replace(" ", "_")
        plt.savefig(output_dir / f"latency_{safe_name}.png", dpi=150)
        plt.close()


def plot_error_rate(df: pd.DataFrame, output_dir: Path):
    fig, ax = plt.subplots(figsize=(16, 4))

    for op in df["operation"].unique():
        op_df = df[df["operation"] == op]
        total = op_df["total_count"]
        errors = op_df["error_count"]
        rate = (errors / total.replace(0, 1)) * 100
        ax.plot(op_df["elapsed_min"], rate, label=op, alpha=0.8)

    ax.set_xlabel("Elapsed Time (minutes)")
    ax.set_ylabel("Error Rate (%)")
    ax.set_title("Error Rate Over Time")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / "error_rate.png", dpi=150)
    plt.close()


def plot_data_volume(df: pd.DataFrame, output_dir: Path):
    total_df = df.groupby("elapsed_min")["total_bytes_mb"].sum().reset_index()

    fig, ax = plt.subplots(figsize=(16, 4))
    ax.plot(total_df["elapsed_min"], total_df["total_bytes_mb"] / 1024, linewidth=2, color="green")
    ax.set_xlabel("Elapsed Time (minutes)")
    ax.set_ylabel("Total Data Volume (GB)")
    ax.set_title("Data Volume Growth Over Time")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / "data_volume.png", dpi=150)
    plt.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: python plot.py <timeseries.csv> [output_dir]")
        sys.exit(1)

    csv_path = sys.argv[1]
    output_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(csv_path).parent / "charts"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading data from {csv_path}...")
    df = load_data(csv_path)
    print(f"  {len(df)} rows, {df['operation'].nunique()} operations, {df['phase'].nunique()} phases")

    print("Generating throughput chart...")
    plot_throughput(df, output_dir)

    print("Generating latency percentile charts...")
    plot_latency_percentiles(df, output_dir)

    print("Generating error rate chart...")
    plot_error_rate(df, output_dir)

    print("Generating data volume chart...")
    plot_data_volume(df, output_dir)

    print(f"Charts saved to {output_dir}/")


if __name__ == "__main__":
    main()
