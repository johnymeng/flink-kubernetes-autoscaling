"""Render the three-panel Q8 resource figure from a quick_pair.py run.

Reuses the Q8 online-penalty figure drawing code and metric definitions. Both jobs
are aligned to the sealed input release, even when one gets a new Flink start time.
"""
import argparse
import csv
import hashlib
import importlib.util
import math
import json
from urllib.request import urlopen
from pathlib import Path

from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator, StrMethodFormatter

METRICS = ("source_records_per_second", "managed_memory_used_bytes",
           "managed_memory_allocated_bytes", "task_slots_used")
MIB = 1024 ** 2


def value(event, metric, divisor=1):
    result = event["values"][metric]
    return result / divisor if result is not None else float("nan")


def read_arm(arm_dir, case, window_seconds):
    rows = [json.loads(x) for x in (arm_dir / "resource-series.jsonl").read_text().splitlines()]
    plan = json.loads((arm_dir / "plan.json").read_text())
    manifest = json.loads((arm_dir / "manifest.json").read_text())
    jid = manifest["spec"]["flinkConfiguration"]["$internal.pipeline.job-id"]
    release = plan["release_ms"] / 1000
    if not rows or rows[0]["type"] != "started" or any(
            row["job_id"] != jid or row["release_epoch"] != release or row["case"] != case
            for row in rows):
        raise ValueError(f"Unmatched {case} resource series")
    marker = rows[-1] if rows[-1]["type"] in ("finished", "observed_until") else None
    samples = rows[1:-1] if marker else rows[1:]
    if (any(row["type"] != "sample" for row in samples)
            or (marker and marker["snapshot_count"] != len(samples))):
        raise ValueError(f"Missing sample receipts for {case}")
    finish = arm_dir / "finished.json"
    window = arm_dir / "window-ended.json"
    finish_epoch = None
    deadline = None
    if finish.is_file():
        data = json.loads(finish.read_text())
        end_ms = (data.get("job") or {}).get("end-time", -1)
        finish_epoch = end_ms / 1000 if end_ms > 0 else data["epoch"]
    if window.is_file():
        data = json.loads(window.read_text())
        deadline = data.get("deadline_epoch", data.get("epoch"))
    if window_seconds is not None:
        requested = release + window_seconds
        if deadline is not None and requested > deadline + .01:
            raise ValueError(f"Requested {case} window extends beyond its receipt")
        deadline = requested
    if finish_epoch is None and deadline is None:
        raise ValueError(f"No completion or observation window for {case}")
    end_epoch = min(x for x in (finish_epoch, deadline) if x is not None)
    ended_at_deadline = deadline is not None and end_epoch == deadline
    if ended_at_deadline:
        observed = bool(samples and samples[-1]["capture_epoch"] >= deadline)
        if marker and marker["type"] == "observed_until":
            observed |= marker["deadline_epoch"] >= deadline
        if not observed:
            raise ValueError(f"Insufficient observed coverage for {case} at {deadline}")
    elif marker is None or marker["type"] != "finished":
        raise ValueError(f"Incomplete finished-job series for {case}")
    samples = [row for row in samples if row["capture_epoch"] <= end_epoch]
    if not samples:
        raise ValueError(f"No eligible samples for {case}")
    previous = None
    for row in samples:
        row["elapsed_sec"] = row["capture_epoch"] - release
        if previous is not None and row["elapsed_sec"] <= previous:
            raise ValueError(f"Capture time did not increase in {case}")
        previous = row["elapsed_sec"]
    if any(not any(row["values"][key] is not None for row in samples) for key in METRICS):
        raise ValueError(f"A required Flink metric was unavailable for the entire {case} run")
    return samples, end_epoch - release, ended_at_deadline


RYAN_REVISION = "7d32b6abb0423453c4fbf3f44206abf5c2a81d9e"
RYAN_SHA256 = "1f1193c54e1055937121c081d9d7e485edf88c04ab444efaa4b444b482844bbe"
RYAN_URL = f"https://raw.githubusercontent.com/RyanL123/memory-aware-scaling/{RYAN_REVISION}/scripts/experiments/plot_run.py"
RYAN_METRICS = ("source_throughput_records_per_sec", "total_managed_memory_used_bytes",
                "total_managed_memory_total_bytes", "total_slots_used")


def load_ryan_plotter(path, output):
    if path is None:
        path = output / f"plot_run-{RYAN_REVISION[:7]}.py"
        if not path.exists():
            with urlopen(RYAN_URL, timeout=15) as response:
                payload = response.read()
            if hashlib.sha256(payload).hexdigest() != RYAN_SHA256:
                raise ValueError("Downloaded original Q8 plotter has an unexpected checksum")
            path.write_bytes(payload)
    if hashlib.sha256(path.read_bytes()).hexdigest() != RYAN_SHA256:
        raise ValueError(f"Expected the pinned original Q8 plotter {RYAN_REVISION}")
    spec = importlib.util.spec_from_file_location("ryan_q8_plot_run", path)
    upstream = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(upstream)
    # Preserve missing points in the plot. The original limits helper expects the
    # first value to be finite, which does not hold throughout a savepoint outage.
    original_limits = upstream.infer_axis_limits
    def finite_limits(column, values):
        return original_limits(column, [v for v in values if math.isfinite(v)])
    upstream.infer_axis_limits = finite_limits
    return upstream


def draw_ryan(arm, rows, upstream, controllers, output, label):
    mode = controllers[arm]["mode"]
    filename = "q8-" + mode
    series = {original: [value(row, key) for row in rows]
              for original, key in zip(RYAN_METRICS, METRICS)}
    with (output / (filename + "-samples.csv")).open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(("timestamp_epoch", *RYAN_METRICS))
        for index, row in enumerate(rows):
            writer.writerow((row["capture_epoch"], *(column[index] for column in series.values())))
    run_row = {"run_id": rows[0]["deployment"], "autoscaler": label,
               "run_commit": controllers[arm]["revision"][:12]}
    upstream.plot_run("q8", run_row, [row["elapsed_sec"] for row in rows], series,
                      output / (filename + "-ryan.png"))


def join_profile(folder, release, end_epoch):
    join_id = "11ffd1a2f5905feab358b0af9a75fe4e"
    progress = [json.loads(line) for line in (folder / "progress.jsonl").read_text().splitlines()]
    profiles = []
    for row in progress:
        if row.get("epoch", release) > end_epoch:
            continue
        parallelism = row.get("parallelism", {}).get(join_id)
        memory = row.get("join_managed_slot_bytes")
        native = row.get("native") or {}
        if not parallelism or not memory or len(native) != int(parallelism):
            continue
        current = (int(parallelism), memory / MIB)
        if not profiles or current != profiles[-1]:
            profiles.append(current)
    events = folder / "rescale-events.jsonl"
    spans = []
    starts = {}
    if events.is_file():
        for entry in (json.loads(line) for line in events.read_text().splitlines()):
            if entry["epoch"] > end_epoch:
                continue
            if entry.get("type") == "rescale_start":
                starts[entry["transaction"]] = entry["epoch"] - release
            elif entry.get("type") == "rescale_end" and entry["transaction"] in starts:
                spans.append((starts.pop(entry["transaction"]), entry["epoch"] - release))
    spans.extend((start, end_epoch - release) for start in starts.values())
    return profiles, spans


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pair", type=Path, help="Directory containing hardcoded/ and calibrated/")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--window-seconds", type=float,
                        help="Plot only the common first N seconds when both arms were observed through that time")
    parser.add_argument("--main-label", default="main · fixed costs",
                        help="Display the policy mode, including any empty-curve adapter")
    parser.add_argument("--ryan-plotter", type=Path,
                        help="Local checkout of the pinned Ryan scripts/experiments/plot_run.py")
    args = parser.parse_args()
    args.output.mkdir(exist_ok=True, parents=True)
    cases = {"hardcoded": args.main_label, "calibrated": "a4s-cache-miss-penalty · online costs"}
    pair_manifest = args.pair / "pair.json"
    pair_data = json.loads(pair_manifest.read_text()) if pair_manifest.exists() else {}
    offered = pair_data.get("offered_records_per_second")
    native = pair_data.get("scaling_path") == "native_operator"
    native_results = {}
    if native:
        result_path = args.pair / "native-results.json"
        if not result_path.is_file():
            result_path = args.pair.parent / "quick-results.json"
        native_results = json.loads(result_path.read_text())
    kafka = native and all(result.get("source_mode") == "kafka" for result in native_results.values())
    producer_rate = offered if kafka else None
    if kafka:
        # The producer also writes bids; the Q8 source throughput counts only persons and auctions.
        offered = None
    records = {}
    elapsed_finish = {}
    ended_at_deadline = {}
    for case in cases:
        records[case], elapsed_finish[case], ended_at_deadline[case] = read_arm(
            args.pair / case, case, args.window_seconds)
    upstream = load_ryan_plotter(args.ryan_plotter, args.output)
    if native:
        controllers = {arm: {"mode": mode, "revision": json.loads((args.pair / arm / "operator.json").read_text())["revision"]}
                       for arm, mode in (("hardcoded", "main"), ("calibrated", "branch"))}
    else:
        controllers = json.loads((args.pair.parent / "controllers.json").read_text())
    for case, rows in records.items():
        draw_ryan(case, rows, upstream, controllers, args.output, cases[case])
    with (args.output / "q8-resource-series.csv").open("w", newline="") as out:
        columns = ["case", "deployment", "job_id", "capture_epoch", "elapsed_sec",
                   "job_state", "job_start_epoch_ms", *METRICS]
        writer = csv.DictWriter(out, fieldnames=columns)
        writer.writeheader()
        for rows in records.values():
            for row in rows:
                writer.writerow({key: row.get(key) for key in columns[:-4]} | row["values"])

    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 10, "axes.labelcolor": "#334155",
        "text.color": "#172033", "xtick.color": "#475569", "ytick.color": "#475569",
        "axes.edgecolor": "#cbd5e1", "axes.spines.top": False, "axes.spines.right": False,
        "grid.color": "#e2e8f0", "grid.linewidth": 0.65, "axes.axisbelow": True,
        "pdf.fonttype": 42,
    })
    fig, axes = plt.subplots(3, 2, figsize=(11, 7.7), sharex=True, sharey="row")
    fig.subplots_adjust(left=0.11, bottom=0.125, right=0.975, top=0.81, wspace=0.13, hspace=0.24)
    fig.suptitle("Nexmark Q8 on the lab cluster", fontsize=19, fontweight="bold", x=0.11,
                 y=0.963, ha="left")
    release_times = [records[arm][0]["release_epoch"] for arm in cases]
    first_end = min(records[arm][0]["release_epoch"] + elapsed_finish[arm] for arm in cases)
    schedule = "Simultaneous release" if len(set(release_times)) == 1 else (
        "Concurrent jobs" if max(release_times) < first_end else "Sequential jobs")
    method = "operator-managed in-place scaling" if native else "including savepoint recovery"
    subtitle = f"{schedule} · identical paced input · {method}"
    if any(ended_at_deadline.values()):
        horizon = max(elapsed_finish[c] for c in cases if ended_at_deadline[c])
        subtitle = f"{schedule} · first {horizon:g} s from input release · {method}"
    if producer_rate:
        subtitle += f" · producer target {producer_rate:,.0f} mixed events/s"
    fig.text(0.11, 0.908, subtitle, fontsize=10.5, color="#475569")
    labels = ("Source throughput\n(records/s)", "Managed memory\n(MiB)", "Task slots\nused")
    if kafka:
        labels = ("Person + auction source\n(records/s)", labels[1], labels[2])
    for axis, label in zip(axes[:, 0], labels):
        axis.set_ylabel(label)
    for col, (case, rows) in enumerate(records.items()):
        a, b, c = axes[:, col]
        if native:
            result = native_results[case]
            applied = [step["observed_profile"] for step in result["transactions"] if step.get("observed_profile")]
            profiles = [(p["join_parallelism"], p["join_managed_slot_bytes"] / MIB)
                        for p in [result.get("initial_profile"), *applied] if p and p.get("join_managed_slot_bytes")]
            spans = [(step["in_place_ack_from_release_s"],
                      min(step["observed_from_release_s"] if step["observed_from_release_s"] is not None
                          else elapsed_finish[case], elapsed_finish[case])) for step in result["transactions"]
                     if step["in_place_ack_from_release_s"] < elapsed_finish[case]]
        else:
            profiles, spans = join_profile(args.pair / case, rows[0]["release_epoch"],
                                           rows[0]["release_epoch"] + elapsed_finish[case])
        profile_text = "  →  ".join(f"{k} × {m:g}" for k, m in profiles)
        title = cases[case] + ("\nJoin: " + profile_text + " MiB/slot" if profile_text else "")
        a.set_title(title, loc="left", fontsize=10, fontweight="bold", pad=6)
        for axis in (a, b, c):
            for left, right in spans:
                axis.axvspan(left, right, color="#cbd5e1", alpha=.28, linewidth=0, zorder=0)
            if native:
                for failure in result.get("failed_in_place_requests", ()):
                    at = failure["native_failure_from_release_s"]
                    if 0 <= at <= elapsed_finish[case]:
                        axis.axvline(at, color="#a44224", linestyle="--", linewidth=1)
        t = [event["elapsed_sec"] for event in rows]
        a.plot(t, [value(event, METRICS[0]) for event in rows], linewidth=1.75, marker="o",
               markersize=2.5, color="#0369a1")
        if offered is not None:
            a.axhline(offered, color="#9a3412", linestyle="--", linewidth=1.2)
            a.annotate(f"Offered {offered:,} records/s", xy=(.98, offered),
                       xycoords=a.get_yaxis_transform(), xytext=(0, 3), textcoords="offset points",
                       va="bottom", ha="right", fontsize=8, color="#81330f")
        b.step(t, [value(event, METRICS[2], MIB) for event in rows], where="post", linewidth=1.7,
               color="#16734e", linestyle="--", label="Allocated")
        b.step(t, [value(event, METRICS[1], MIB) for event in rows], where="post", linewidth=1.7,
               color="#b43338", label="Used")
        c.step(t, [value(event, METRICS[3]) for event in rows], where="post", linewidth=1.7,
               color="#554399")
        for axis in (a, b, c):
            axis.grid(True)
            axis.set_xlim(left=0)
            axis.set_ylim(bottom=0)
            axis.tick_params(length=3)
        if elapsed_finish[case] is not None:
            c.axvline(elapsed_finish[case], color="#64748b", linestyle=":", linewidth=1)
        c.set_xlabel("Seconds since scheduled producer start" if kafka else "Seconds since input release")

    for panel, keys, divisor in ((0, (METRICS[0],), 1), (1, METRICS[1:3], MIB),
                                 (2, (METRICS[3],), 1)):
        observed = [row["values"][key] / divisor for rows in records.values() for row in rows
                    for key in keys if row["values"][key] is not None]
        if panel == 0 and offered is not None:
            observed.append(offered)
        highest = max(observed, default=0)
        axes[panel, 0].set_ylim(0, max(1, highest * 1.10))
    axes[0, 0].yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
    axes[2, 0].yaxis.set_major_locator(MaxNLocator(integer=True))
    axes[1, 0].legend(loc="best", frameon=True, framealpha=0.95, edgecolor="#e2e8f0")
    shading = "in-place acknowledgement to observed placement or cutoff" if native else "resize to verified restore or cutoff"
    caption = f"5 s REST samples. Gaps mark missing data. Shading spans {shading}. Memory sums TaskManager managed pools."
    if any(result.get("failed_in_place_requests") for result in native_results.values()):
        caption += "\nDashed red lines mark failed in-place updates."
    fig.text(0.11, 0.039, caption,
             fontsize=8.7, color="#475569")
    for format_ in ("png",):
        fig.savefig(args.output / f"q8-resource-comparison.{format_}", dpi=200, format=format_)
    plt.close(fig)



if __name__ == "__main__":
    main()
