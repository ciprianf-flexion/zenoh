# Slow subscriber delays and drops traffic to a fast subscriber

A single-host reproduction with one router and one Python script. zenoh 1.9.0, no root, no network
shaping tools.

## Setup

```
publisher --> router --> fast subscriber                        (loopback)
                 \
                  +--> rate-limiting TCP proxy --> slow subscriber
```

- One publisher session sends a small topic, 1 KB at 500 Hz, and a large topic, 2 MB at 15 Hz. Both use
  `CongestionControl::Drop`.
- The fast subscriber takes only the small topic, directly from the router.
- The slow subscriber takes both topics through a proxy that forwards at 5 Mbit/s.
- All three are clients, so every message goes through the router.
- The only non-default router setting is `max_wait_before_drop_fragments`. Shared memory and multicast
  scouting are disabled.

## Run

```bash
uv run --with eclipse-zenoh==1.9.0 repro.py --budget-ms 500 --no-slow   # reference, no slow subscriber
uv run --with eclipse-zenoh==1.9.0 repro.py --budget-ms 500
uv run --with eclipse-zenoh==1.9.0 repro.py --budget-ms 50              # zenoh default
```

Each run takes about 40 s. Options: `--link-mbit`, `--duration`.

## Results

30 s per run on an x86_64 Linux workstation.

| | no slow subscriber | budget 500 ms | budget 50 ms |
|---|---|---|---|
| small topic published | 15000 | 15000 | 15000 |
| fast subscriber received | 15000 | 14799 | 15000 |
| fast subscriber worst gap | 2.4 ms | 503 ms | 53 ms |
| fast subscriber gaps over 100 ms | 0 | 13 | 0 |
| publisher worst `put()` | 0.5 ms | 51 ms | 1.1 ms |
| slow subscriber received, small / large | - | 174 / 2 | 238 / 2 |

- The fast subscriber stalls for about `max_wait_before_drop_fragments` once per large message. It does
  not subscribe to the large topic.
- Above roughly 100 ms, the stall also back-pressures the publisher. Its `put()` blocks, and small
  samples are lost before they reach the router.
- The slow link carries about 18 MB in 30 s but delivers 2 large messages. Most of its capacity goes into
  large messages that are dropped after some of their fragments were sent. The small topic gets 1 to 2 %
  of its samples through.

## Why

The router forwards each message on the RX task of the incoming link, to every matching face in turn.
A fragmented Drop message toward the congested face waits in `StageIn::push_network_message` for a free
batch, up to `wait_before_drop + max_wait_before_drop_fragments`
(`io/zenoh-transport/src/common/pipeline.rs`). During that wait the RX task reads nothing else from the
publisher link, including the small topic for the fast subscriber.

A large budget is needed to deliver large messages over slow links at all, so the budget trades the slow
subscriber's delivery against the fast subscriber's latency. We are looking for isolation that does not
depend on this trade-off.
