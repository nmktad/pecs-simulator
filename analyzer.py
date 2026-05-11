import csv
import itertools
import math
import random
import statistics
from pathlib import Path

from client import Client
from event import Event, EventType
from gateway import Gateway
from message import Message
from scheduler import Scheduler
from server import Server


def reset_ids() -> None:
    Client.client_id_iter = itertools.count(1)
    Message.msg_id_iter = itertools.count()
    Server.server_id_iter = itertools.count()
    Event.event_id_iter = itertools.count()


def model_config(model_name: str) -> tuple[int, float]:
    configs = {
        "M/M/1": (1, float("inf")),
        "M/M/1/4": (1, 3),  # queue limit = capacity - servers = 4 - 1
        "M/M/1/8": (1, 7),  # queue limit = 8 - 1
        "M/M/3/8": (3, 5),  # queue limit = 8 - 3
    }
    if model_name not in configs:
        raise ValueError(f"Unknown model {model_name}")
    return configs[model_name]


def num_busy_servers(gateway: Gateway) -> int:
    return sum(1 for s in gateway.servers if s.is_busy())


def run_replication(
    model_name: str, lam: float, mu: float, sim_time: float, seed: int
) -> dict:
    random.seed(seed)
    reset_ids()

    num_servers, queue_limit = model_config(model_name)
    scheduler = Scheduler()
    gateway = Gateway(
        scheduler, num_of_servers=num_servers, queue_size=queue_limit, mu=mu
    )
    client = Client(scheduler, lam=lam)
    client.create_message(0.0)

    current_time = 0.0
    last_time = 0.0

    # for time-weighted averages
    area_nsys = 0.0
    area_nq = 0.0

    while (event := scheduler.get_event()) is not None:
        current_time = event.get_time()
        if current_time > sim_time:
            break

        # accumulate time-weighted area before processing event
        dt = current_time - last_time
        if dt > 0:
            nq = len(gateway.queue)
            nsys = nq + num_busy_servers(gateway)
            area_nq += nq * dt
            area_nsys += nsys * dt

        message = event.get_message()
        event_type = event.get_event_type()

        if event_type == EventType.SEND:
            scheduler.add_event(Event(message, EventType.RECV, current_time + 1.0))
            client.create_message(current_time)

        elif event_type == EventType.RECV:
            gateway.receive_message(message, current_time)

        elif event_type == EventType.DEPT:
            message.departure_time = current_time
            gateway.total_sojourn_time += message.get_sojourn_time()
            gateway.total_wait_time += message.get_wait_time()
            gateway.total_served += 1
            gateway.release_server(message, current_time)

        last_time = current_time

    # instant values at end of simulation
    instant_nq = len(gateway.queue)
    instant_nsys = instant_nq + num_busy_servers(gateway)
    completed = max(1, gateway.total_served)

    return {
        "model": model_name,
        "lambda": lam,
        "arrived": gateway.total_arrived,
        "dropped": gateway.total_dropped,
        "served": gateway.total_served,
        # instant
        "instant_nsys": instant_nsys,
        "instant_nq": instant_nq,
        # total
        "total_nsys": area_nsys,
        "total_nq": area_nq,
        "total_wait": gateway.total_wait_time,
        "total_sojourn": gateway.total_sojourn_time,
        # average
        "avg_nsys": area_nsys / sim_time,
        "avg_nq": area_nq / sim_time,
        "avg_wait": gateway.total_wait_time / completed,
        "avg_sojourn": gateway.total_sojourn_time / completed,
        # drop rate
        "drop_prob": (
            gateway.total_dropped / gateway.total_arrived
            if gateway.total_arrived > 0
            else 0.0
        ),
    }


def mean_ci95(samples: list[float]) -> tuple[float, float, float]:
    n = len(samples)
    if n == 0:
        return 0.0, 0.0, 0.0
    mean_v = statistics.fmean(samples)
    if n == 1:
        return mean_v, mean_v, mean_v
    std_v = statistics.stdev(samples)
    margin = 1.96 * std_v / math.sqrt(n)
    return mean_v, mean_v - margin, mean_v + margin


def aggregate_results(rows: list[dict]) -> list[dict]:
    grouped = {}
    for row in rows:
        key = (row["model"], row["lambda"])
        grouped.setdefault(key, []).append(row)

    metrics = [
        "instant_nsys",
        "instant_nq",
        "total_nsys",
        "total_nq",
        "total_wait",
        "total_sojourn",
        "avg_nsys",
        "avg_nq",
        "avg_wait",
        "avg_sojourn",
        "drop_prob",
    ]

    summary = []
    for (model, lam), group in sorted(grouped.items()):
        item = {
            "model": model,
            "lambda": lam,
            "replications": len(group),
            "arrived_mean": statistics.fmean(r["arrived"] for r in group),
            "served_mean": statistics.fmean(r["served"] for r in group),
            "dropped_mean": statistics.fmean(r["dropped"] for r in group),
        }
        for metric in metrics:
            samples = [r[metric] for r in group]
            mean_v, lo, hi = mean_ci95(samples)
            item[f"{metric}_mean"] = mean_v
            item[f"{metric}_ci95_low"] = lo
            item[f"{metric}_ci95_high"] = hi
        summary.append(item)
    return summary


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def plot_results(summary: list[dict], output_dir: Path) -> None:
    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError:
        print("matplotlib not installed, skipping plots")
        return

    metrics = [
        ("avg_wait", "Average Wait Time in Queue"),
        ("avg_sojourn", "Average Time in System"),
        ("avg_nq", "Average Number in Queue"),
        ("avg_nsys", "Average Number in System"),
        ("drop_prob", "Drop Probability"),
    ]

    models = sorted(set(r["model"] for r in summary))

    for metric_key, title in metrics:
        fig, ax = plt.subplots(figsize=(8, 5))
        for model in models:
            rows = sorted(
                [r for r in summary if r["model"] == model], key=lambda r: r["lambda"]
            )
            x = [r["lambda"] for r in rows]
            y = [r[f"{metric_key}_mean"] for r in rows]
            lo = [r[f"{metric_key}_ci95_low"] for r in rows]
            hi = [r[f"{metric_key}_ci95_high"] for r in rows]
            yerr = [[m - l for m, l in zip(y, lo)], [h - m for h, m in zip(hi, y)]]
            ax.errorbar(x, y, yerr=yerr, marker="o", capsize=4, label=model)

        ax.set_title(f"{title} vs Lambda (95% CI)")
        ax.set_xlabel("Lambda (arrivals/s)")
        ax.set_ylabel(title)
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(output_dir / f"{metric_key}.png", dpi=150)
        plt.close(fig)
        print(f"Saved plot: {metric_key}.png")


def main() -> None:
    replications = 30
    sim_time = 1000.0
    mu = 8.0
    seed = 42
    lambdas = [4.0, 6.0, 8.0, 12.0]
    models = ["M/M/1", "M/M/1/4", "M/M/1/8", "M/M/3/8"]

    output_dir = Path("results")
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_rows = []
    rep_id = 0

    for model in models:
        for lam in lambdas:
            print(f"Running {model} lambda={lam}...")
            for _ in range(replications):
                row = run_replication(
                    model_name=model,
                    lam=lam,
                    mu=mu,
                    sim_time=sim_time,
                    seed=seed + rep_id,
                )
                raw_rows.append(row)
                rep_id += 1

    summary = aggregate_results(raw_rows)
    write_csv(output_dir / "raw_replications.csv", raw_rows)
    write_csv(output_dir / "summary_ci95.csv", summary)
    plot_results(summary, output_dir)
    print(f"\nDone. Results in {output_dir}/")


if __name__ == "__main__":
    main()
