import argparse
import itertools
import math

import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

import analyzer
from client import Client
from event import Event, EventType
from gateway import Gateway
from message import Message
from my_queue import Queue
from scheduler import Scheduler
from server import Server


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class Engine:
    """
    Main simulation engine.

    Responsibilities:
    - Initialise all components (Scheduler, Gateway, Clients)
    - Drive the event loop
    - Collect per-run metrics
    - Aggregate results over multiple runs
    """

    # Predefined scenarios: name -> (num_servers, queue_capacity)
    # For M/M/s/K notation: K is the *total* system capacity (queue + servers).
    # Queue capacity = K - s.
    SCENARIOS = {
        "M/M/1": (1, float("inf")),  # unlimited queue
        "M/M/1/4": (1, 3),  # total capacity 4 → queue holds 3
        "M/M/1/8": (1, 7),  # total capacity 8 → queue holds 7
        "M/M/3/8": (3, 5),  # total capacity 8 → queue holds 5
    }

    def __init__(
        self, num_of_servers: int = 1, queue_size: float = float("inf"), mu: float = 8.0
    ) -> None:
        """
        Parameters
        ----------
        num_of_servers : int
            Number of parallel servers in the gateway.
        queue_size : float | int
            Maximum number of messages that can wait in the queue.
            Use float("inf") for an unlimited queue (M/M/1).
        mu : float
            Service rate (messages per second). Service times are
            Exp(mu) distributed.
        """
        self.num_of_servers = num_of_servers
        self.queue_size = queue_size
        self.mu = mu

        # These are (re)created by reset() before every run
        self.clients: list[Client] = []
        self.current_time = 0.0
        self.scheduler: Scheduler = Scheduler()
        self.gateway: Gateway = Gateway(
            self.scheduler,
            num_of_servers=self.num_of_servers,
            queue_size=self.queue_size,
            mu=self.mu,
        )

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Reset all state for a fresh simulation run."""
        # Reset class-level ID counters so IDs start from 0 each run
        Client.client_id_iter = itertools.count(1)
        Message.msg_id_iter = itertools.count()
        Server.server_id_iter = itertools.count()
        Event.event_id_iter = itertools.count()

        self.clients = []
        self.current_time = 0.0
        self.scheduler = Scheduler()
        self.gateway = Gateway(
            self.scheduler,
            num_of_servers=self.num_of_servers,
            queue_size=self.queue_size,
            mu=self.mu,
        )

    def create_clients(self, n_clients: int, lam: float) -> None:
        """Instantiate *n_clients* Client objects and register them."""
        self.clients.extend(
            Client(scheduler=self.scheduler, lam=lam) for _ in range(n_clients)
        )

    # ------------------------------------------------------------------
    # Trace
    # ------------------------------------------------------------------

    def trace(self, event: Event) -> None:
        msg = event.get_message()
        node = (
            msg.source if event.get_event_type() == EventType.SEND else msg.destination
        )
        print(
            f"{event.get_time():<10.3f} "
            f"{node:<6} "
            f"{event.get_event_type().value:<6} "
            f"{msg.source:<8} "
            f"{msg.destination:<6} "
            f"{msg.id}"
        )

    # ------------------------------------------------------------------
    # Event loop
    # ------------------------------------------------------------------

    def run_once(
        self,
        n_clients: int = 1,
        lam: float = 4.0,
        sim_time: float = 100.0,
        enable_trace: bool = False,
    ) -> dict[str, float]:
        """
        Execute a single simulation run.
        """
        self.reset()
        self.create_clients(n_clients, lam)

        # Seed one initial SEND event per client
        for client in self.clients:
            client.create_message(self.current_time)

        if enable_trace:
            print(
                f"{'time':<10} {'node':<6} {'event':<6} "
                f"{'source':<8} {'dest':<6} {'msgID'}"
            )
            print("-" * 50)

        # ---- main event loop ----------------------------------------
        while (event := self.scheduler.get_event()) is not None:
            self.current_time = event.get_time()

            if enable_trace:
                self.trace(event)

            message = event.get_message()
            event_type = event.get_event_type()

            if event_type == EventType.SEND:
                # Propagation delay: message arrives at gateway 1 s later
                recv_event = Event(message, EventType.RECV, self.current_time + 1.0)
                self.scheduler.add_event(recv_event)
                # Client schedules its next message
                if self.current_time <= sim_time:
                    client_id = message.get_source()
                    self.clients[client_id - 1].create_message(self.current_time)

            elif event_type == EventType.RECV:
                self.gateway.receive_message(message, self.current_time)

            elif event_type == EventType.DEPT:
                message.departure_time = self.current_time
                self.gateway.update_departure_metrics(message)
                self.gateway.release_server(message, self.current_time)

        # ---- collect metrics ----------------------------------------
        gw = self.gateway
        served = gw.total_served if gw.total_served > 0 else 1  # avoid /0

        total_service_time = sum(s.total_service_time for s in gw.servers)

        metrics = {
            # Average number of messages in the gateway (Little's law approx)
            "avg_msgs_gateway": (gw.total_sojourn_time / sim_time)
            if sim_time > 0
            else 0.0,
            # Average number of messages in the queue
            "avg_msgs_queue": (gw.total_wait_time / sim_time) if sim_time > 0 else 0.0,
            # Average sojourn time (total time in system)
            "avg_time_gateway": gw.total_sojourn_time / served,
            # Average waiting time in queue
            "avg_time_queue": gw.total_wait_time / served,
            # Average service time
            "avg_time_server": total_service_time / served,
            # Total dropped messages
            "avg_dropped": float(gw.total_dropped),
        }
        return metrics

    # ------------------------------------------------------------------
    # Multiple runs
    # ------------------------------------------------------------------

    def run_many(
        self,
        n_runs: int = 100,
        n_clients: int = 1,
        lam: float = 4.0,
        sim_time: float = 100.0,
    ) -> dict[str, dict[str, float]]:
        """
        Execute *n_runs* independent replications and return
        mean ± 95 % confidence interval for every metric.

        Returns
        -------
        dict  metric_name -> {"mean": float, "ci": float}
        """
        all_results = [
            self.run_once(n_clients=n_clients, lam=lam, sim_time=sim_time)
            for _ in range(n_runs)
        ]

        summary = {}
        metric_names = all_results[0].keys()

        for metric in metric_names:
            values = np.array([r[metric] for r in all_results])
            mean = values.mean()
            # 95 % CI using t-distribution (n-1 degrees of freedom)
            std = values.std(ddof=1)
            # t* ≈ 1.984 for n=100, but scipy not required — use 1.96 for large n
            t_star = 1.96
            ci = t_star * std / math.sqrt(n_runs)
            summary[metric] = {"mean": mean, "ci": ci}

        return summary

    # ------------------------------------------------------------------
    # Plotting
    # ------------------------------------------------------------------

    @staticmethod
    def plot_results(
        results: dict[float, dict[str, dict[str, float]]],
        scenario_name: str,
        lam_values: list[float],
    ) -> None:
        """
        Plot 6 separate metric figures vs λ with 95 % CI error bars.

        Parameters
        ----------
        results       : {lam: {metric: {"mean", "ci"}}}
        scenario_name : e.g. "M/M/1"
        lam_values    : list of λ values tested
        """
        metric_labels = {
            "avg_msgs_gateway": "Avg # messages in gateway",
            "avg_msgs_queue": "Avg # messages in queue",
            "avg_time_gateway": "Avg time in gateway (s)",
            "avg_time_queue": "Avg time in queue (s)",
            "avg_time_server": "Avg time in server (s)",
            "avg_dropped": "Avg # dropped messages",
        }

        scenario_slug = scenario_name.replace("/", "_")

        for metric, label in metric_labels.items():
            means = [results[lam][metric]["mean"] for lam in lam_values]
            cis = [results[lam][metric]["ci"] for lam in lam_values]

            fig, ax = plt.subplots(figsize=(8, 5))
            ax.errorbar(
                lam_values,
                means,
                yerr=cis,
                fmt="o-",
                capsize=5,
                color="steelblue",
                ecolor="tomato",
                linewidth=2,
                markersize=6,
            )
            ax.set_title(f"{label} — {scenario_name}", fontsize=11)
            ax.set_xlabel("λ (arrival rate)")
            ax.set_ylabel(label)
            ax.grid(True, linestyle="--", alpha=0.5)
            fig.tight_layout()

            fname = f"results_{scenario_slug}_{metric}.png"
            fig.savefig(fname, dpi=150)
            print(f"  → saved {fname}")
            plt.close(fig)

    # ------------------------------------------------------------------
    # Test methods (one per class, called from Engine)
    # ------------------------------------------------------------------

    def test_message(self) -> None:
        """Test the Message class."""
        print("\n=== test_message ===")
        m = Message(1, 0)
        m.arrival_time = 1.0
        m.service_start_time = 2.5
        m.departure_time = 4.0
        m.print_message()
        print(f"  wait time:    {m.get_wait_time():.3f}")
        print(f"  sojourn time: {m.get_sojourn_time():.3f}")
        print(f"  service time: {m.get_service_time():.3f}")

    def test_event(self) -> None:
        """Test the Event class."""
        print("\n=== test_event ===")
        for etype, t in [
            (EventType.SEND, 1.0),
            (EventType.RECV, 2.0),
            (EventType.DEPT, 3.0),
        ]:
            e = Event(Message(1, 0), etype, t)
            e.print_event()

    def test_scheduler(self) -> None:
        """Test Scheduler ordering."""
        print("\n=== test_scheduler ===")
        sched = Scheduler()
        # Insert out of order
        for t in [3.0, 1.0, 2.0]:
            sched.add_event(Event(Message(1, 0), EventType.SEND, t))
        print("Expected order: 1.0, 2.0, 3.0")
        while (e := sched.get_event()) is not None:
            e.print_event()

    def test_queue(self) -> None:
        """Test Queue with overflow."""
        print("\n=== test_queue ===")
        q = Queue(3)
        for i in range(4):
            m = Message(i, 0)
            ok = q.enqueue(m)
            print(f"  enqueue msg {i}: {'OK' if ok else 'DROPPED'}")
        print(f"  queue length: {len(q)}")
        while not q.is_empty():
            msg = q.dequeue()
            if msg is not None:
                msg.print_message()

    def test_client(self) -> None:
        """Test Client message generation."""
        print("\n=== test_client ===")
        sched = Scheduler()
        client = Client(sched, lam=4)
        client.create_message(0.0)
        e = sched.get_event()
        if e is None:
            raise RuntimeError("Expected a SEND event")
        print(f"  First SEND event time: {e.get_time():.3f}")
        e.print_event()

    def test_server(self) -> None:
        """Test Server service event creation."""
        print("\n=== test_server ===")
        sched = Scheduler()
        server = Server(sched, mu=8)
        m = Message(1, 0)
        server.set_busy(True, m)
        e = server.get_service(m, 1.0)
        print(f"  Server busy: {server.is_busy()}")
        e.print_event()
        server.set_busy(False, None)
        print(f"  Server busy after release: {server.is_busy()}")

    def test_gateway(self) -> None:
        """Test Gateway receive / depart cycle."""
        print("\n=== test_gateway ===")
        sched = Scheduler()
        gateway = Gateway(sched, num_of_servers=1, queue_size=3, mu=8)
        t = 0.0
        for i in range(5):
            gateway.receive_message(Message(i, 0), t)
            t += 0.1
        while (e := sched.get_event()) is not None:
            msg = e.get_message()
            msg.departure_time = e.get_time()
            gateway.update_departure_metrics(msg)
            gateway.release_server(msg, e.get_time())
        gateway.print_stats()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discrete-event simulator — NCA USEEJ7"
    )
    parser.add_argument(
        "--scenario",
        default="M/M/1",
        choices=list(Engine.SCENARIOS.keys()),
        help="Queueing model scenario (default: M/M/1)",
    )
    parser.add_argument(
        "--lam",
        type=float,
        nargs="+",
        default=[4, 6, 8, 12],
        help="Arrival rate(s) λ to simulate (default: 4 6 8 12)",
    )
    parser.add_argument(
        "--mu", type=float, default=8.0, help="Service rate μ (default: 8)"
    )
    parser.add_argument(
        "--sim-time",
        type=float,
        default=30.0,
        help="Simulated duration per run in seconds (default: 30)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=100,
        help="Number of independent replications (default: 100)",
    )
    parser.add_argument(
        "--clients", type=int, default=1, help="Number of client nodes (default: 1)"
    )
    parser.add_argument(
        "--trace", action="store_true", help="Print event trace for a single demo run"
    )
    parser.add_argument(
        "--test", action="store_true", help="Run unit tests for all classes and exit"
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Run analyzer workflow (CSV + plots) for selected scenario/lambda values",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    num_servers, queue_size = Engine.SCENARIOS[args.scenario]
    engine = Engine(num_of_servers=num_servers, queue_size=queue_size, mu=args.mu)

    # ---- unit tests ------------------------------------------------
    if args.test:
        engine.test_message()
        engine.test_event()
        engine.test_scheduler()
        engine.test_queue()
        engine.test_client()
        engine.test_server()
        engine.test_gateway()
        return

    if args.analyze:
        analyzer.run_analysis(
            replications=args.runs,
            sim_time=args.sim_time,
            mu=args.mu,
            seed=42,
            lambdas=args.lam,
            models=[args.scenario],
            output_dir=Path("results"),
        )
        return

    if args.trace:
        print(
            f"\n--- Trace: {args.scenario}, λ={args.lam[0]}, "
            f"sim_time={args.sim_time} ---"
        )
        engine.run_once(
            n_clients=args.clients,
            lam=args.lam[0],
            sim_time=args.sim_time,
            enable_trace=True,
        )
        return

    # ---- full simulation over all λ values -------------------------
    print(f"\nScenario : {args.scenario}")
    print(f"λ values : {args.lam}")
    print(f"μ        : {args.mu}")
    print(f"Runs     : {args.runs}")
    print(f"Sim time : {args.sim_time} s\n")

    all_results: dict[float, dict[str, dict[str, float]]] = {}

    for lam in args.lam:
        print(f"  Running λ={lam} × {args.runs} replications …", end=" ", flush=True)
        all_results[lam] = engine.run_many(
            n_runs=args.runs,
            n_clients=args.clients,
            lam=lam,
            sim_time=args.sim_time,
        )
        print("done")
        for metric, stats in all_results[lam].items():
            print(f"    {metric:<22} mean={stats['mean']:.4f} ± {stats['ci']:.4f}")

    Engine.plot_results(all_results, args.scenario, args.lam)


if __name__ == "__main__":
    main()
