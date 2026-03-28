# Fluxera Benchmark Notes

Last updated: 2026-03-29

This document records the latest local benchmark pass used before the `0.0.1` release candidate.

Label legend:

- `c=`: Fluxera worker concurrency setting used by the benchmark runner
- `t=`: Dramatiq `worker_threads`

## Environment

- machine: `Apple M5 Pro`
- CPU cores: `15`
- OS: `macOS 26.3.1 (25D2128)`
- Python: `3.12.10`
- Redis: local Docker Redis on `redis://127.0.0.1:6379/15`
- comparison target: local `dramatiq` checkout in the sibling repository
- default Fluxera process start method during this run: `spawn`

## Commands

Production-shaped comparison:

```bash
python3 benchmarks/production_compare.py --profile smoke
```

Redis transport comparison:

```bash
python3 benchmarks/redis_transport_compare.py --repeat 3 --long-io-secs 1.5
```

Verification suite:

```bash
python3 -m unittest discover -s tests -v
```

## What These Benchmarks Measure

The benchmark harnesses focus on the areas Fluxera was built to improve:

- high async I/O fanout without thread-per-message scaling
- mixed workloads where long async I/O should not starve short work
- CPU-bound tasks running in a separate process lane
- Redis transport behavior under real at-least-once delivery

The Dramatiq side is intentionally given multiple worker-thread variants so the comparison is not limited to a single default setting.

## Production-Shaped Results

### 1. Single-worker async fanout

Scenario: `single-worker-fanout250`

| Runtime | Wall | CPU | Peak RSS | Total peak threads | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| Fluxera `c=256` | `0.258s` | `0.141s` | `66.52 MiB` | `2` | one loop, no extra loop thread |
| Dramatiq `t=8` | `0.385s` | `0.119s` | `42.50 MiB` | `12` | one loop thread plus worker threads |
| Dramatiq `t=32` | `0.319s` | `0.123s` | `51.98 MiB` | `36` | improved throughput by buying more threads |

Takeaway:

- Fluxera was `1.49x` faster than Dramatiq `t=8`.
- Fluxera was `1.24x` faster than Dramatiq `t=32`.
- The throughput gain came with far fewer threads.

### 2. Single-worker CPU-bound

Scenario: `single-worker-cpu-bound`

| Runtime | Wall | CPU | Peak RSS | Total peak threads |
| --- | ---: | ---: | ---: | ---: |
| Fluxera `c=32` | `1.270s` | `4.340s` | `189.67 MiB` | `6` |
| Dramatiq `t=8` | `3.893s` | `3.657s` | `40.08 MiB` | `11` |
| Dramatiq `t=32` | `3.704s` | `3.644s` | `40.45 MiB` | `35` |

Takeaway:

- Fluxera was about `3.0x` faster because CPU work ran in the dedicated process lane.
- Under the safer `spawn` default, the memory tradeoff is larger: Fluxera used roughly `4.7x` the RSS of Dramatiq in this scenario.

### 3. Single-worker mixed long I/O and short work

Scenario: `single-worker-mixed-long-io`

| Runtime | Wall | Short drain | Long tail | Total peak threads |
| --- | ---: | ---: | ---: | ---: |
| Fluxera `c=64` | `3.142s` | `0.040s` | `2.860s` | `2` |
| Dramatiq `t=8` | `6.417s` | `6.038s` | `6.044s` | `12` |
| Dramatiq `t=32` | `3.224s` | `0.098s` | `2.853s` | `36` |
| Dramatiq `t=64` | `3.224s` | `0.038s` | `2.855s` | `68` |

Takeaway:

- With modest threading (`t=8`), Dramatiq let long async work dominate the queue.
- To match Fluxera's short-drain behavior, Dramatiq had to scale to `32` or `64` worker threads.
- Fluxera reached the same behavior with `2` peak threads.

### 4. Single-worker mixed long I/O plus CPU

Scenario: `single-worker-mixed-long-io-plus-cpu`

| Runtime | Wall | CPU drain | Long tail | Total peak threads |
| --- | ---: | ---: | ---: | ---: |
| Fluxera `c=64` | `3.141s` | `0.295s` | `2.859s` | `5` |
| Dramatiq `t=8` | `6.707s` | `6.349s` | `6.314s` | `12` |
| Dramatiq `t=32` | `3.227s` | `0.699s` | `2.855s` | `36` |
| Dramatiq `t=64` | `3.271s` | `0.709s` | `2.857s` | `68` |

Takeaway:

- Fluxera kept CPU work moving while long async I/O was still in flight.
- Even when wall time became similar at higher Dramatiq thread counts, Fluxera still drained CPU work about `2.4x` faster than Dramatiq `t=32` or `t=64`.

### 5. Thirty-worker mixed long I/O

Scenario: `thirty-workers-mixed-long-io`

| Runtime | Wall | Short drain | Aggregate CPU | Total peak threads | Event loops |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fluxera `c=64` | `3.385s` | `0.012s` | `1.604s` | `60` | `30` |
| Dramatiq `t=8` | `6.483s` | `2.885s` | `2.440s` | `360` | `30 + 30 extra loop threads` |
| Dramatiq `t=32` | `3.507s` | `0.013s` | `3.174s` | `1080` | `30 + 30 extra loop threads` |

Takeaway:

- Fluxera matched the best-case wall time while using drastically fewer threads.
- Dramatiq `t=32` needed `18x` more peak threads to match the same cluster-wide short-drain latency.

### 6. Thirty-worker CPU-bound

Scenario: `thirty-workers-cpu-bound`

| Runtime | Wall | Aggregate CPU | Avg peak RSS | Total peak threads |
| --- | ---: | ---: | ---: | ---: |
| Fluxera `c=32` | `1.812s` | `18.338s` | `107.08 MiB` | `90` |
| Dramatiq `t=8` | `1.188s` | `9.605s` | `41.77 MiB` | `330` |
| Dramatiq `t=32` | `1.225s` | `9.843s` | `42.62 MiB` | `1050` |

Takeaway:

- With the safer default `spawn` policy, Dramatiq was faster in this cluster-scale CPU scenario.
- Fluxera still used significantly fewer threads, but the startup and process-management cost was visible here.
- This is the main tradeoff of the current default: Fluxera is strongest on async-heavy and mixed workloads, not on every CPU-shaped cluster benchmark.

## Redis Transport Results

### 1. Redis async fanout

Scenario: `redis-fanout`

| Runtime | Wall | Peak RSS | Peak threads | Msg/s |
| --- | ---: | ---: | ---: | ---: |
| Fluxera `c=256` | `0.311s` | `41.44 MiB` | `2` | `773.0` |
| Dramatiq `t=8` | `0.379s` | `43.12 MiB` | `12` | `641.4` |
| Dramatiq `t=32` | `0.352s` | `44.38 MiB` | `36` | `685.8` |

Takeaway:

- Fluxera stayed faster on real Redis, not only on the in-memory stub broker.

### 2. Redis mixed long and short work

Scenario: `redis-mixed-long-short`

| Runtime | Wall | Short drain | Peak threads |
| --- | ---: | ---: | ---: |
| Fluxera `c=96` | `1.527s` | `0.081s` | `2` |
| Dramatiq `t=8` | `3.176s` | `1.673s` | `12` |
| Dramatiq `t=32` | `1.713s` | `0.089s` | `36` |

Takeaway:

- Fluxera was `2.08x` faster than Dramatiq `t=8`.
- To match Fluxera's short-drain latency, Dramatiq again needed a much larger worker-thread count.

### 3. Redis stale reclaim

Scenario: `redis-stale-reclaim`

| Runtime | Wall | Reclaim-only time | Messages reclaimed |
| --- | ---: | ---: | ---: |
| Fluxera | `1.398s` | `0.647s` | `2048` |

Takeaway:

- The reclaim path itself is efficient enough for the current alpha design.
- This scenario is transport-internal, so it is not compared directly to Dramatiq.

## Async-Native Check

Fluxera's async-native claim held up in the benchmark outputs:

- Fluxera workers reported `1` event loop per worker process.
- Fluxera did not need an extra loop thread for async actor execution.
- The best async results came from task concurrency on the worker loop, not from scaling worker threads.

In the same scenarios, Dramatiq required larger thread counts to reach similar short-drain behavior.

## Revision Management Check

Revision handoff was revalidated in the test suite:

- Redis handoff test: `test_revision_promotion_drains_unstarted_backlog_to_new_worker`
- Stub handoff test: `test_revision_promotion_drains_unstarted_backlog_to_new_worker`
- CLI check: `test_revision_cli_get_and_promote`

This confirms that a promoted `serving_revision` causes stale workers to drain and hand off unstarted local backlog.

## At-Least-Once Check

At-least-once behavior was revalidated with Redis integration tests:

- `test_at_least_once_recovers_after_worker_process_crash`
- `test_stale_pending_delivery_is_reclaimed_by_another_consumer`
- `test_worker_renews_lease_for_long_running_task`

These confirm:

- a worker crash before `ack` leads to redelivery
- stale pending deliveries are reclaimed by another consumer
- healthy long-running tasks renew their lease and avoid accidental reclaim

## Release Verdict

Based on this benchmark pass:

- Fluxera has a real performance advantage over Dramatiq for the async-heavy and mixed I/O workloads it is targeting.
- Fluxera keeps that advantage on the real Redis transport, not only in the stub runtime.
- CPU-bound support is credible because of the process lane, but with the safer default `spawn` policy the cluster-scale CPU story is no longer a universal win.
- Revision handoff and at-least-once semantics are both covered by integration tests and are behaving as designed.
