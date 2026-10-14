#!/usr/bin/env python3
"""s33-rtt-proxy.py — userspace TCP delay proxy for S3.3 local RTT simulation.

Pure byte-pipe between LightFS and MinIO: it never parses HTTP, so SigV4
headers pass through untouched and signatures stay valid.

Delay model: each direction sleeps rtt_ms/2 before forwarding its bytes.
One HTTP request+response crosses both directions once, so the observed
round trip gains 2 * (rtt_ms/2) = rtt_ms. The realized latency is verified
empirically by the calibration step (s33-run.sh stage 0) and reported as a
measured value — this proxy's --rtt-ms is the *target*, not the result.

Usage:
  python3 s33-rtt-proxy.py --listen 127.0.0.1:9004 --target 127.0.0.1:9003 \
      --rtt-ms 19 [--stats FILE]
"""
import argparse
import asyncio


async def one_way(reader, writer, delay, counters, key):
    try:
        while True:
            data = await reader.read(262144)
            if not data:
                break
            counters[key] += len(data)
            if delay > 0:
                await asyncio.sleep(delay)
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--rtt-ms", type=float, required=True,
                    help="target added round-trip latency in ms")
    ap.add_argument("--stats", default="")
    args = ap.parse_args()

    half = (args.rtt_ms / 2.0) / 1000.0
    lhost, lport = args.listen.rsplit(":", 1)
    thost, tport = args.target.rsplit(":", 1)
    counters = {"conns": 0, "c2s": 0, "s2c": 0}

    async def client_connected(c_reader, c_writer):
        try:
            u_reader, u_writer = await asyncio.open_connection(thost, int(tport))
        except OSError:
            c_writer.close()
            return
        counters["conns"] += 1
        t1 = asyncio.create_task(
            one_way(c_reader, u_writer, half, counters, "c2s"))
        t2 = asyncio.create_task(
            one_way(u_reader, c_writer, half, counters, "s2c"))
        await asyncio.gather(t1, t2)

    server = await asyncio.start_server(
        client_connected, lhost, int(lport))
    print(f"rtt-proxy: listen={args.listen} target={args.target} "
          f"rtt_ms={args.rtt_ms} (half={half * 1000:.2f}ms each direction)",
          flush=True)
    try:
        async with server:
            await server.serve_forever()
    finally:
        line = (f"conns={counters['conns']} bytes_c2s={counters['c2s']} "
                f"bytes_s2c={counters['s2c']}")
        print(line, flush=True)
        if args.stats:
            with open(args.stats, "w") as f:
                f.write(line + "\n")


if __name__ == "__main__":
    main()
