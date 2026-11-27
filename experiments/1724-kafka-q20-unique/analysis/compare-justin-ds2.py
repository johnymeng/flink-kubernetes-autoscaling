#!/usr/bin/env python3
"""Compare two exported experiments on shared plots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt

from plot_data import elapsed_minutes, load_series, run_start


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left")
    parser.add_argument("right")
    parser.add_argument("--output")
    args = parser.parse_args()

    left = Path(args.left).resolve()
    right = Path(args.right).resolve()

    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
    for path, color in [(left, "tab:blue"), (right, "tab:orange")]:
        metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))
        label = metadata["autoscaler"]
        throughput_t, throughput_v = load_series(path / "throughput_total_source.csv")
        busy_t, busy_v = load_series(path / "join_busy_pct.csv")
        bid_t, bid_v = load_series(path / "kafka_lag_bid.csv")
        auction_t, auction_v = load_series(path / "kafka_lag_auction.csv")
        start = run_start(metadata, throughput_t, busy_t, bid_t, auction_t)

        axes[0].plot([elapsed_minutes(t, start) for t in throughput_t], throughput_v, label=label, color=color)
        axes[1].plot([elapsed_minutes(t, start) for t in busy_t], busy_v, label=f"{label} busy%", color=color)
        if bid_v:
            axes[2].plot([elapsed_minutes(t, start) for t in bid_t], bid_v, label=f"{label} bid lag", color=color)
        if auction_v:
            axes[2].plot(
                [elapsed_minutes(t, start) for t in auction_t], auction_v,
                label=f"{label} auction lag", color=color, linestyle=":",
            )

    axes[0].set_title("Total Source Throughput")
    axes[0].set_ylabel("events/s")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    axes[1].set_title("Join Busy Time")
    axes[1].set_ylabel("busy %")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    axes[2].set_title("Kafka Lag")
    axes[2].set_ylabel("events")
    axes[2].set_xlabel("Elapsed time (min)")
    axes[2].legend()
    axes[2].grid(True, alpha=0.3)

    axes[2].set_xlim(left=0)
    for ax in axes:
        ax.set_ylim(bottom=0)

    fig.tight_layout()
    output = Path(args.output).resolve() if args.output else Path.cwd() / "compare_justin_ds2.png"
    fig.savefig(output, dpi=180)
    print(output)


if __name__ == "__main__":
    main()
