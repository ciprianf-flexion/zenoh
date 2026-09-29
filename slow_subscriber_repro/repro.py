"""A slow subscriber delays and drops traffic to a fast subscriber through a zenoh router.

One host, one router. Every process is a zenoh client, so all traffic goes through the router.

  publisher --> router --> fast subscriber   (loopback)
                   \\
                    +--> rate-limiting TCP proxy --> slow subscriber

The publisher sends a small topic (1 KB at 500 Hz) and a large topic (2 MB at 15 Hz) from one
session. The fast subscriber takes only the small topic. The slow subscriber takes both through a
proxy that forwards at --link-mbit.

Run:
  uv run --with eclipse-zenoh==1.9.0 repro.py --budget-ms 500
  uv run --with eclipse-zenoh==1.9.0 repro.py --budget-ms 500 --no-slow   # reference
"""

import argparse
import asyncio
import json
import multiprocessing as mp
import socket
import threading
import time

import zenoh

SMALL, LARGE = "demo/small", "demo/large"
SMALL_HZ, SMALL_SIZE = 500, 1024
LARGE_HZ, LARGE_SIZE = 15, 2_000_000


def config(mode, endpoint, budget_ms=None):
    c = zenoh.Config()
    c.insert_json5("mode", json.dumps(mode))
    c.insert_json5("scouting/multicast/enabled", "false")
    c.insert_json5("transport/shared_memory/enabled", "false")
    c.insert_json5("listen/endpoints" if mode == "router" else "connect/endpoints", json.dumps([endpoint]))
    if budget_ms is not None:
        c.insert_json5(
            "transport/link/tx/queue/congestion_control/drop/max_wait_before_drop_fragments",
            str(budget_ms * 1000),
        )
    return c


def router(endpoint, budget_ms, seconds):
    session = zenoh.open(config("router", endpoint, budget_ms))
    time.sleep(seconds)
    session.close()


def publisher(endpoint, seconds, out):
    session = zenoh.open(config("client", endpoint))
    small = session.declare_publisher(SMALL, congestion_control=zenoh.CongestionControl.DROP)
    large = session.declare_publisher(LARGE, congestion_control=zenoh.CongestionControl.DROP)
    time.sleep(1)
    stop = time.monotonic() + seconds
    worst_put = [0.0]

    def timed_put(pub, payload):
        t0 = time.monotonic()
        pub.put(payload)
        worst_put[0] = max(worst_put[0], time.monotonic() - t0)

    def large_loop():
        payload, t = bytes(LARGE_SIZE), time.monotonic()
        while time.monotonic() < stop:
            timed_put(large, payload)
            t += 1 / LARGE_HZ
            time.sleep(max(0.0, t - time.monotonic()))

    thread = threading.Thread(target=large_loop)
    thread.start()
    seq, t = 0, time.monotonic()
    while time.monotonic() < stop:
        timed_put(small, seq.to_bytes(8, "little") + bytes(SMALL_SIZE - 8))
        seq += 1
        t += 1 / SMALL_HZ
        time.sleep(max(0.0, t - time.monotonic()))
    thread.join()
    out.put(("publisher", {"small_published": seq, "worst_put_ms": round(worst_put[0] * 1000, 1)}))
    session.close()


def fast_subscriber(endpoint, seconds, out):
    seqs, gaps, last = set(), [], [None]

    def on_sample(sample):
        now = time.monotonic()
        seqs.add(int.from_bytes(bytes(sample.payload)[:8], "little"))
        if last[0] is not None:
            gaps.append((now - last[0]) * 1000)
        last[0] = now

    session = zenoh.open(config("client", endpoint))
    sub = session.declare_subscriber(SMALL, on_sample)
    time.sleep(seconds)
    sub.undeclare()
    out.put(("fast_subscriber", {
        "received": len(seqs),
        "worst_gap_ms": round(max(gaps), 1) if gaps else None,
        "gaps_over_20ms": sum(g > 20 for g in gaps),
        "gaps_over_100ms": sum(g > 100 for g in gaps),
    }))
    session.close()


def slow_subscriber(endpoint, seconds, out):
    counts = {SMALL: 0, LARGE: 0}

    def on_sample(sample):
        counts[str(sample.key_expr)] += 1

    session = zenoh.open(config("client", endpoint))
    sub = session.declare_subscriber("demo/**", on_sample)
    time.sleep(seconds)
    sub.undeclare()
    out.put(("slow_subscriber", {"small_received": counts[SMALL], "large_received": counts[LARGE]}))
    session.close()


def proxy(listen_port, target_port, mbit, seconds):
    """Forward TCP between the slow subscriber and the router at `mbit` in each direction."""
    rate = mbit * 1e6 / 8

    async def pump(reader, writer):
        t_next = time.monotonic()
        try:
            while data := await reader.read(16384):
                t_next = max(t_next, time.monotonic()) + len(data) / rate
                await asyncio.sleep(t_next - time.monotonic())
                writer.write(data)
                await writer.drain()
        except ConnectionError:
            pass
        writer.close()

    async def handle(client_r, client_w):
        router_r, router_w = await asyncio.open_connection("127.0.0.1", target_port)
        await asyncio.gather(pump(client_r, router_w), pump(router_r, client_w))

    async def main():
        server = await asyncio.start_server(handle, "127.0.0.1", listen_port)
        async with server:
            await asyncio.sleep(seconds)

    asyncio.run(main())


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--budget-ms", type=int, default=500, help="router max_wait_before_drop_fragments")
    ap.add_argument("--link-mbit", type=float, default=5, help="slow subscriber link rate")
    ap.add_argument("--duration", type=int, default=30, help="publish time in s")
    ap.add_argument("--no-slow", action="store_true", help="run without the slow subscriber")
    args = ap.parse_args()

    ctx = mp.get_context("spawn")
    out = ctx.Queue()
    router_port, proxy_port = free_port(), free_port()
    router_ep, proxy_ep = f"tcp/127.0.0.1:{router_port}", f"tcp/127.0.0.1:{proxy_port}"
    total = args.duration + 8

    procs = [ctx.Process(target=router, args=(router_ep, args.budget_ms, total + 2))]
    procs[0].start()
    time.sleep(1)
    subs = [ctx.Process(target=fast_subscriber, args=(router_ep, total - 2, out))]
    if not args.no_slow:
        procs.append(ctx.Process(target=proxy, args=(proxy_port, router_port, args.link_mbit, total)))
        subs.append(ctx.Process(target=slow_subscriber, args=(proxy_ep, total - 2, out)))
    for p in procs[1:] + subs:
        p.start()
    time.sleep(2)
    pub = ctx.Process(target=publisher, args=(router_ep, args.duration, out))
    pub.start()
    for p in [pub] + subs:
        p.join()

    results = dict(out.get() for _ in range(len(subs) + 1))
    for p in procs:
        p.terminate()

    pub_r, fast = results["publisher"], results["fast_subscriber"]
    lost = pub_r["small_published"] - fast["received"]
    print(f"router max_wait_before_drop_fragments: {args.budget_ms} ms, "
          f"slow link: {'none' if args.no_slow else f'{args.link_mbit} Mbit/s'}")
    print(f"small topic published:            {pub_r['small_published']}")
    print(f"fast subscriber received:         {fast['received']} (lost {lost})")
    print(f"fast subscriber worst gap:        {fast['worst_gap_ms']} ms (expected 2 ms)")
    print(f"fast subscriber gaps >20 / >100:  {fast['gaps_over_20ms']} / {fast['gaps_over_100ms']}")
    print(f"publisher worst put():            {pub_r['worst_put_ms']} ms")
    if "slow_subscriber" in results:
        slow = results["slow_subscriber"]
        print(f"slow subscriber received:         small {slow['small_received']}, large {slow['large_received']}")


if __name__ == "__main__":
    main()
