# PECS Queueing Simulator

Discrete-event simulation project for queueing models (`M/M/1`, `M/M/1/K`, `M/M/s/K`) with:
- event-level simulation engine,
- replication/aggregation workflow,
- confidence intervals,
- optional plotting.

## Project Structure

- `main.py` - CLI simulation runner (single-run trace, test/demo methods, batch runs, plots).
- `analyzer.py` - replication-oriented analysis pipeline with CSV outputs and CI95 plots.
- `client.py` - client/message generation (Poisson arrivals).
- `gateway.py` - queue + server dispatch + drop/serve metrics.
- `server.py` - service process (exponential service times).
- `scheduler.py` - event priority queue.
- `event.py` - event type and event model.
- `message.py` - message model and message timing metrics.
- `my_queue.py` - bounded/unbounded queue implementation.
- `results/` - generated CSV and plot outputs.

## Requirements

- Python 3.10+ (recommended)
- `numpy`
- `matplotlib` (for plots)

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install numpy matplotlib
```

## Usage

### 1) Run simulation via CLI (`main.py`)

Default run:

```bash
python main.py
```

Custom scenario and parameters:

```bash
python main.py \
  --scenario M/M/1/8 \
  --lam 4 6 8 12 \
  --mu 8 \
  --sim-time 30 \
  --runs 100 \
  --clients 1 \
```

This prints per-metric means and 95% CI estimates, and saves 6 plot images (one per metric):
- `results_<scenario>_avg_msgs_gateway.png`
- `results_<scenario>_avg_msgs_queue.png`
- `results_<scenario>_avg_time_gateway.png`
- `results_<scenario>_avg_time_queue.png`
- `results_<scenario>_avg_time_server.png`
- `results_<scenario>_avg_dropped.png`

### 2) Run event trace (single demo run)

```bash
python main.py --trace --scenario M/M/1 --lam 4
```

### 3) Run class test/demo methods

```bash
python main.py --test
```

## CLI Arguments (`main.py`)

- `--scenario`
  Queueing scenario. Choices:
  - `M/M/1`
  - `M/M/1/4`
  - `M/M/1/8`
  - `M/M/3/8`

- `--lam`
  One or more arrival rates (lambda). Example: `--lam 4 6 8 12`

- `--mu`
  Service rate (mu). Default: `8.0`

- `--sim-time`
  Simulated time per run in seconds. Default: `30.0`

- `--runs`
  Number of independent replications per lambda. Default: `100`

- `--clients`
  Number of client nodes. Default: `1`

- `--trace`
  Enable detailed event trace for one run.

- `--test`
  Run built-in class test/demo methods and exit.

## Analysis Workflow (`analyzer.py`)

`analyzer.py` runs multiple models/lambdas/replications, aggregates metrics, writes CSV files, and generates plots.

Run:

```bash
python analyzer.py
```

Default settings in code:
- Replications: `100`
- Simulation time: `30.0`
- Service rate: `8.0`
- Lambdas: `4.0, 6.0, 8.0, 12.0`
- Models: `M/M/1`, `M/M/1/4`, `M/M/1/8`, `M/M/3/8`

Outputs to `results/`:
- `raw_replications.csv`
- `summary_ci95.csv`
- Plot images:
  - `avg_wait.png`
  - `avg_sojourn.png`
  - `avg_nq.png`
  - `avg_nsys.png`
  - `drop_prob.png`

## Metrics (high level)

The project computes metrics such as:
- average number in system/queue,
- average wait time and sojourn time,
- service statistics,
- dropped messages / drop probability,
- confidence intervals (95%).

## Notes

- Queue capacities follow `M/M/s/K` interpretation where `K` is total system capacity.
- For finite-capacity cases, queue limit is effectively `K - s`.
- Arrival and service processes are exponential (`Poisson` arrivals, `Exp(mu)` service times).

## Quick Commands

```bash
# Default simulation
python main.py

# Single trace run
python main.py --trace --scenario M/M/1 --lam 4

# Built-in tests/demos
python main.py --test

# Full analysis + CSV + plots
python analyzer.py
```
