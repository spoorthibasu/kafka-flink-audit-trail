"""Capture mediawiki.page_change.v1 from Wikimedia EventStreams into hourly gzip JSONL.

Reads both datacenter topics (eqiad and codfw) on one connection for the window
[--start, --end), by event time (meta.dt), or from the earliest retained offset
(--start earliest) up to given offsets (--end-offsets), for extending an earlier capture
backwards. The Kafka position in each event's meta drives everything:

- Resume. After any disconnect the client reconnects with a Last-Event-ID built from
  the next offset of every topic it has seen. The server's own id holds a Kafka
  timestamp plus 1 ms; timestamps repeat and go backwards, so resuming from that id
  can skip events.
- Loss. Offsets within a topic are contiguous, so any jump is logged as a gap with the
  event times on either side.
- Window. A topic is complete at its first offset whose meta.dt is at or after --end,
  the same rule Kafka applies to find the first offset at --start, or at the offset
  given for it in --end-offsets (exclusive).

Canary events (meta.domain == "canary") are counted and dropped, as Wikimedia asks.

Output under --out. Events carry titles and user names, so keep it outside the
repository:
  events/hour=YYYY-MM-DDTHH/<dc>.jsonl.gz  one line per event: {"recv_ms","sse_id","event"}
  capture_log.jsonl                        connects, disconnects, gaps, progress
  manifest.json                            window, per-topic offsets and counts, files
State is checkpointed every --checkpoint-seconds and on SIGTERM or SIGINT, which stop
the capture cleanly (exit status 3); rerunning the same command resumes from the last
checkpoint.

  python3 study/wikimedia/capture.py --start 2026-10-20T00:00:00Z \\
      --end 2026-10-23T02:00:00Z --rate 1000 --out ~/audit-trail-bench-data/wikimedia/capture
  python3 study/wikimedia/capture.py --start earliest \\
      --end-offsets eqiad=1141361946,codfw=839004750 --rate 1000 --out ...
"""
import argparse
import calendar
import gzip
import hashlib
import json
import os
import shutil
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sse  # noqa: E402

TOPICS = ("eqiad.mediawiki.page_change.v1", "codfw.mediawiki.page_change.v1")


def parse_dt(s):
    """'2026-10-23T01:41:06.292Z' -> epoch milliseconds."""
    s = s.rstrip("Z")
    base, _, frac = s.partition(".")
    return calendar.timegm(time.strptime(base, "%Y-%m-%dT%H:%M:%S")) * 1000 + int((frac + "000")[:3])


def new_topic():
    return {"first_offset": None, "first_dt": None, "last_offset": None, "last_dt": None,
            "end_offset": None, "end_dt": None, "written": 0, "event_bytes": 0, "canary": 0,
            "duplicates": 0, "after_end": 0, "dt_before_start": 0, "gaps": []}


class Capture(object):
    def __init__(self, out, start_ms, end_ms, end_offsets, rate, checkpoint_s):
        """start_ms None reads from the earliest retained offset; end_offsets maps
        "topic:partition" to the first offset not to capture."""
        self.out, self.start_ms, self.end_ms, self.end_offsets = out, start_ms, end_ms, end_offsets or {}
        self.rate, self.checkpoint_s = rate, checkpoint_s
        self.stop_requested = False
        self.state_path = os.path.join(out, "state.json")
        self.log_file = open(os.path.join(out, "capture_log.jsonl"), "a", encoding="utf-8")
        self.writers = {}   # (hour, dc) -> [gzip file, relative path, lines]
        self.closed = []    # parts closed since the last checkpoint
        if os.path.exists(self.state_path):
            with open(self.state_path, encoding="utf-8") as f:
                self.state = json.load(f)
            if (self.state["start_ms"], self.state["end_ms"], self.state.get("end_offsets", {})) \
                    != (start_ms, end_ms, self.end_offsets):
                sys.exit("state.json is for a different window")
            self._drop_unrecorded_parts()
            self.log({"kind": "restart_from_checkpoint"})
        else:
            self.state = {"start_ms": start_ms, "end_ms": end_ms, "end_offsets": self.end_offsets,
                          "user_agent": sse.USER_AGENT,
                          "rate_cap": rate, "started_ms": int(time.time() * 1000),
                          "finished_ms": None, "topics": {}, "parts": [], "seq": 0,
                          "received": 0, "connects": 0, "disconnects": 0}
            self.log({"kind": "start", "start_ms": start_ms, "end_ms": end_ms, "end_offsets": self.end_offsets,
                      "rate_cap": rate, "user_agent": sse.USER_AGENT})

    def log(self, record):
        record.setdefault("ts_ms", int(time.time() * 1000))
        self.log_file.write(json.dumps(record) + "\n")
        self.log_file.flush()
        if record["kind"] in ("connected", "disconnected"):
            self.state["connects" if record["kind"] == "connected" else "disconnects"] += 1
        if record["kind"] != "progress":
            print(json.dumps(record), file=sys.stderr, flush=True)

    def resume_id(self):
        """Next offset for every topic seen; the window start time for the others, or
        timestamp 1 (the earliest retained offset) when there is no start time. A
        complete topic resumes at offset -1, the newest, so the server does not resend
        events past its end."""
        ids = []
        keys = list(self.state["topics"]) + ["%s:0" % t for t in TOPICS]
        for key in dict.fromkeys(keys):
            topic, partition = key.rsplit(":", 1)
            s = self.state["topics"].get(key)
            if s and s["end_offset"] is not None:
                ids.append({"topic": topic, "partition": int(partition), "offset": -1})
            elif s and s["last_offset"] is not None:
                ids.append({"topic": topic, "partition": int(partition), "offset": s["last_offset"] + 1})
            else:
                ids.append({"topic": topic, "partition": int(partition), "timestamp": self.start_ms or 1})
        return json.dumps(ids, separators=(",", ":"))

    def done(self):
        topics = self.state["topics"]
        return all(k in topics and topics[k]["end_offset"] is not None for k in ("%s:0" % t for t in TOPICS)) \
            and all(s["end_offset"] is not None for s in topics.values())

    def handle(self, sse_id, data, recv_ms):
        ev = json.loads(data)
        m = ev["meta"]
        key = "%s:%d" % (m["topic"], m["partition"])
        s = self.state["topics"].setdefault(key, new_topic())
        off = m["offset"]
        if s["last_offset"] is not None and off <= s["last_offset"]:
            s["duplicates"] += 1
            return
        if s["end_offset"] is not None:
            s["after_end"] += 1
            s["last_offset"] = off
            return
        if s["last_offset"] is not None and off != s["last_offset"] + 1:
            gap = {"topic": key, "after_offset": s["last_offset"], "next_offset": off,
                   "missing": off - s["last_offset"] - 1, "after_dt": s["last_dt"], "next_dt": m["dt"]}
            s["gaps"].append(gap)
            self.log(dict(gap, kind="gap"))
        if s["first_offset"] is None:
            s["first_offset"], s["first_dt"] = off, m["dt"]
        s["last_offset"], s["last_dt"] = off, m["dt"]
        dt_ms = parse_dt(m["dt"])
        target = self.end_offsets.get(key)
        if (self.end_ms is not None and dt_ms >= self.end_ms) or (target is not None and off >= target):
            s["end_offset"], s["end_dt"] = off, m["dt"]
            self.log({"kind": "topic_complete", "topic": key, "end_offset": off, "end_dt": m["dt"]})
            return
        if m.get("domain") == "canary":
            s["canary"] += 1
            return
        if self.start_ms is not None and dt_ms < self.start_ms:
            s["dt_before_start"] += 1
        if "\n" in data:
            # Newlines in valid JSON can only be whitespace between tokens.
            data = data.replace("\n", " ")
        line = '{"recv_ms":%d,"sse_id":%s,"event":%s}\n' % (recv_ms, json.dumps(sse_id), data)
        w = self._writer(m["dt"][:13], m["topic"].split(".")[0])
        w[0].write(line.encode("utf-8"))
        w[2] += 1
        s["written"] += 1
        s["event_bytes"] += len(data.encode("utf-8"))

    def _writer(self, hour, dc):
        w = self.writers.get((hour, dc))
        if w is None:
            if len(self.writers) >= 16:
                self._close(next(iter(self.writers)))
            self.state["seq"] += 1
            rel = os.path.join("parts", "hour=" + hour, dc, "part-%06d.jsonl.gz" % self.state["seq"])
            os.makedirs(os.path.dirname(os.path.join(self.out, rel)), exist_ok=True)
            w = self.writers[(hour, dc)] = [gzip.open(os.path.join(self.out, rel), "wb", compresslevel=6), rel, 0]
        return w

    def _close(self, key):
        f, rel, lines = self.writers.pop(key)
        f.close()
        self.closed.append({"path": rel, "hour": key[0], "dc": key[1], "lines": lines})

    def checkpoint(self, elapsed_s):
        for key in list(self.writers):
            self._close(key)
        self.state["parts"].extend(self.closed)
        self.closed = []
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.state_path)
        self.log({"kind": "progress", "elapsed_s": round(elapsed_s, 3), "received": self.state["received"],
                  "topics": {k: {"last_offset": v["last_offset"], "last_dt": v["last_dt"], "written": v["written"],
                                 "complete": v["end_offset"] is not None} for k, v in self.state["topics"].items()}})

    def _drop_unrecorded_parts(self):
        recorded = {p["path"] for p in self.state["parts"]}
        for root, _, files in os.walk(os.path.join(self.out, "parts")):
            for name in files:
                rel = os.path.relpath(os.path.join(root, name), self.out)
                if rel not in recorded:
                    os.remove(os.path.join(self.out, rel))

    def _request_stop(self, signum, frame):
        self.stop_requested = True

    def run(self):
        signal.signal(signal.SIGTERM, self._request_stop)
        signal.signal(signal.SIGINT, self._request_stop)
        t0 = time.monotonic()
        received_at_start = self.state["received"]
        last_cp = t0
        for ev_type, ev_id, data, recv_ms in sse.stream(log=self.log, resume_id=self.resume_id):
            if ev_type != "message":
                self.log({"kind": "non_message_event", "type": ev_type})
                continue
            self.state["received"] += 1
            self.handle(ev_id, data, recv_ms)
            now = time.monotonic()
            if self.stop_requested:
                self.checkpoint(now - t0)
                self.log({"kind": "stopped_on_signal", "received": self.state["received"]})
                sys.exit(3)
            if now - last_cp >= self.checkpoint_s:
                self.checkpoint(now - t0)
                last_cp = now
            if self.done():
                break
            if self.rate:
                ahead = (self.state["received"] - received_at_start) / float(self.rate) - (now - t0)
                if ahead > 0:
                    time.sleep(ahead)
        self.state["finished_ms"] = int(time.time() * 1000)
        self.checkpoint(time.monotonic() - t0)
        self.finalize()

    def finalize(self):
        """Join each hour's parts into one gzip file (gzip members concatenate) and verify."""
        groups = {}
        for p in sorted(self.state["parts"], key=lambda p: p["path"].rsplit("-", 1)[1]):
            groups.setdefault((p["hour"], p["dc"]), []).append(p)
        files = []
        for (hour, dc), parts in sorted(groups.items()):
            rel = os.path.join("events", "hour=" + hour, dc + ".jsonl.gz")
            path = os.path.join(self.out, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as dst:
                for p in parts:
                    with open(os.path.join(self.out, p["path"]), "rb") as src:
                        shutil.copyfileobj(src, dst)
            expected = sum(p["lines"] for p in parts)
            sha, lines = hashlib.sha256(), 0
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    sha.update(chunk)
            with gzip.open(path, "rb") as f:
                for _ in f:
                    lines += 1
            if lines != expected:
                sys.exit("line count mismatch in %s: %d != %d" % (rel, lines, expected))
            files.append({"path": rel, "hour": hour, "dc": dc, "lines": lines,
                          "bytes": os.path.getsize(path), "sha256": sha.hexdigest()})
            for p in parts:
                os.remove(os.path.join(self.out, p["path"]))
        shutil.rmtree(os.path.join(self.out, "parts"), ignore_errors=True)
        manifest = {k: self.state.get(k) for k in ("start_ms", "end_ms", "end_offsets", "user_agent", "rate_cap", "started_ms",
                                               "finished_ms", "received", "connects", "disconnects", "topics")}
        manifest["files"] = files
        with open(os.path.join(self.out, "manifest.json"), "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        self.log({"kind": "finished", "files": len(files), "lines": sum(x["lines"] for x in files)})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="window start, e.g. 2026-10-20T00:00:00Z, or 'earliest'")
    ap.add_argument("--end", help="window end by meta.dt (exclusive)")
    ap.add_argument("--end-offsets", help="per-topic first offset not to capture, e.g. eqiad=1141361946,codfw=839004750")
    ap.add_argument("--rate", type=float, default=1000, help="cap on events read per second; 0 for none")
    ap.add_argument("--checkpoint-seconds", type=float, default=30)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    if os.path.exists(os.path.join(args.out, "manifest.json")):
        sys.exit("capture already finished: " + args.out)
    if not args.end and not args.end_offsets:
        sys.exit("give --end, --end-offsets or both")
    end_offsets = {}
    for item in (args.end_offsets or "").split(","):
        if item:
            name, _, offset = item.partition("=")
            key = name if ":" in name else "%s.mediawiki.page_change.v1:0" % name
            end_offsets[key] = int(offset)
    start_ms = None if args.start == "earliest" else parse_dt(args.start)
    Capture(args.out, start_ms, parse_dt(args.end) if args.end else None, end_offsets, args.rate,
            args.checkpoint_seconds).run()


if __name__ == "__main__":
    main()
