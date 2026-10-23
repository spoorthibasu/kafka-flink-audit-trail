"""Read the page-change stream for a fixed time and report its event rate.

Writes one JSON line per event: {"r": receive time in ms, "i": SSE id, "d": event data
exactly as received}. The output holds raw events, titles and user names included, so
it belongs outside the repository.

  python3 study/wikimedia/probe.py --seconds 300 --out /some/dir/probe.jsonl
"""
import argparse
import gzip
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sse  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=300)
    ap.add_argument("--max-events", type=int, default=0)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    log_records = []

    def log(record):
        log_records.append(record)
        print(json.dumps(record), file=sys.stderr, flush=True)

    events = data_bytes = 0
    first_ms = last_ms = None
    start = time.time()
    with open(args.out, "w", encoding="utf-8") as out:
        for ev_type, ev_id, data, recv_ms in sse.stream(log=log):
            if ev_type != "message":
                log({"kind": "non_message_event", "type": ev_type})
                continue
            out.write(json.dumps({"r": recv_ms, "i": ev_id, "d": data}) + "\n")
            events += 1
            data_bytes += len(data.encode("utf-8"))
            first_ms = first_ms if first_ms is not None else recv_ms
            last_ms = recv_ms
            if (args.max_events and events >= args.max_events) or time.time() - start >= args.seconds:
                break

    with open(args.out, "rb") as f:
        raw = f.read()
    span_s = (last_ms - first_ms) / 1000.0 if events > 1 else 0.0
    print(json.dumps({
        "user_agent": sse.USER_AGENT,
        "events": events,
        "first_recv_ms": first_ms,
        "last_recv_ms": last_ms,
        "span_seconds": span_s,
        "events_per_second": events / span_s if span_s else None,
        "event_data_bytes": data_bytes,
        "capture_file_bytes": len(raw),
        "capture_file_gzip6_bytes": len(gzip.compress(raw, compresslevel=6)),
        "connects": sum(1 for r in log_records if r["kind"] == "connected"),
        "disconnects": sum(1 for r in log_records if r["kind"] == "disconnected"),
    }, indent=2))


if __name__ == "__main__":
    main()
