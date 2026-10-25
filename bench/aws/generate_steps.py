#!/usr/bin/env python3
"""Write the benchmark's events as step files for the AWS scale run.

Runs bench/benchmark.py generate() unchanged (same generator, seed and attribute list as
`make bench-full`) with its CsvSink replaced by sinks that Avro-encode each row and append it
to the file of its event index: <out>/A/step-001.bin holds every entity's first event in
design A, and so on. Reading steps 1..H in order yields the events in (event_timestamp,
customer_id) order, the order bench/pipeline.py sends them, without sorting 100 million
events in memory. <out>/C/current.bin holds design C, one row per entity. Each frame is a
4-byte big-endian length followed by the Avro binary datum (fastavro schemaless encoding,
which is byte-identical to Avro Java's for the same schema and record).

While writing, every event is checked against the definition of the designs (full model):
B carries A's current values, A's previous_x equals the entity's previous B value (null on
the first event), and C equals the entity's last B row. A mismatch stops the run.

  python3 bench/aws/generate_steps.py --config bench/aws/config.aws.json --out /data/steps
"""
import argparse
import io
import json
import os
import platform
import sys
import time

import fastavro

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import benchmark as b  # noqa: E402


class Checker:
    """Per-event consistency of A, B and C for the full model."""

    def __init__(self, k):
        self.k = k
        self.last_a = None
        self.prev_b = None
        self.counts = {"events_checked": 0, "first_events": 0, "previous_value_checks": 0, "current_state_rows_checked": 0}

    def fail(self, msg):
        raise RuntimeError("design consistency check failed: " + msg)

    def on_a(self, values):
        self.last_a = values

    def on_b(self, values):
        a = self.last_a
        if a is None or a[:3] != values[:3]:
            self.fail("A and B keys differ at %r" % (values[:3],))
        if a[3::2] != values[3:]:
            self.fail("A current values differ from B at %r" % (values[:3],))
        if values[2] == 1:
            if any(v is not None for v in a[4::2]):
                self.fail("A has a previous value on the first event of %s" % values[0])
            self.counts["first_events"] += 1
        else:
            if self.prev_b is None or self.prev_b[0] != values[0] or self.prev_b[2] != values[2] - 1:
                self.fail("B events out of order at %r" % (values[:3],))
            if a[4::2] != self.prev_b[3:]:
                self.fail("A previous values differ from the previous B event at %r" % (values[:3],))
            self.counts["previous_value_checks"] += 1
        self.prev_b = values
        self.counts["events_checked"] += 1

    def on_c(self, values):
        if self.prev_b is None or values != self.prev_b:
            self.fail("C differs from the last B row of %s" % values[0])
        self.counts["current_state_rows_checked"] += 1


class StepSink:
    """Stands in for benchmark.CsvSink: same constructor and row() calls, Avro step files out."""
    out = None
    schemas = None
    checker = None
    totals = {}

    def __init__(self, path, header):
        self.design = {"a.csv": "A", "b.csv": "B", "c.csv": "C"}[os.path.basename(path)]
        self.header = header
        self.schema = StepSink.schemas[self.design]
        self.dir = os.path.join(StepSink.out, self.design)
        os.makedirs(self.dir, exist_ok=True)
        self.files = {}
        self.stats = StepSink.totals.setdefault(self.design, {"records": 0, "datum_bytes": 0, "file_bytes": 0, "files": 0})

    def _file(self, seq):
        key = 0 if self.design == "C" else seq
        fh = self.files.get(key)
        if fh is None:
            name = "current.bin" if self.design == "C" else "step-%03d.bin" % seq
            fh = self.files[key] = open(os.path.join(self.dir, name), "wb", buffering=1 << 20)
            self.stats["files"] += 1
        return fh

    def row(self, values):
        if self.design == "A":
            StepSink.checker.on_a(values)
        elif self.design == "B":
            StepSink.checker.on_b(values)
        else:
            StepSink.checker.on_c(values)
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self.schema, dict(zip(self.header, values)))
        d = buf.getvalue()
        fh = self._file(values[2])
        fh.write(len(d).to_bytes(4, "big"))
        fh.write(d)
        self.stats["records"] += 1
        self.stats["datum_bytes"] += len(d)
        self.stats["file_bytes"] += 4 + len(d)

    def close(self):
        for fh in self.files.values():
            fh.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--model", default="full")
    args = ap.parse_args()
    cfg = json.load(open(args.config))
    if args.model != "full":
        raise SystemExit("the consistency check covers the full model only")
    attrs = b.attributes(cfg["k_attributes"])
    n, h = cfg["n_entities"], cfg["h_events"]
    StepSink.out = args.out
    StepSink.schemas = {"A": fastavro.parse_schema(b.avro_schema("PreviousValue", attrs, True)),
                        "B": fastavro.parse_schema(b.avro_schema("AuditTrail", attrs, False)),
                        "C": fastavro.parse_schema(b.avro_schema("CurrentState", attrs, False))}
    StepSink.checker = Checker(len(attrs))
    b.CsvSink = StepSink
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    t0 = time.perf_counter()
    b.generate(cfg, args.model, n, h, attrs, args.out, None)
    wall = time.perf_counter() - t0
    for d, n_expected in (("A", n * h), ("B", n * h), ("C", n)):
        if StepSink.totals[d]["records"] != n_expected:
            raise RuntimeError("%s holds %d records, expected %d" % (d, StepSink.totals[d]["records"], n_expected))
    manifest = {
        "config_file": args.config, "config": cfg, "model": args.model, "n_entities": n, "h_events": h,
        "k_attributes": len(attrs), "records_per_step": n, "steps": h, "designs": StepSink.totals,
        "checks": StepSink.checker.counts, "generator_wall_seconds": round(wall, 1),
        "microseconds_per_event": round(wall / (n * h) * 1e6, 2), "started_utc": started,
        "python": platform.python_version(), "fastavro": fastavro.__version__, "machine": b.machine_info(),
    }
    with open(os.path.join(args.out, "manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=2)
    print(json.dumps({k: manifest[k] for k in ("designs", "checks", "generator_wall_seconds", "microseconds_per_event")}))


if __name__ == "__main__":
    main()
