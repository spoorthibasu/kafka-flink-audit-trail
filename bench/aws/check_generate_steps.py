#!/usr/bin/env python3
"""Check that bench/aws/generate_steps.py writes exactly the events bench/benchmark.py generates.

Generates a small config both ways: benchmark.generate() with its own CSV sinks, and
generate_steps.py's step files. Every step-file record, decoded, must equal the CSV row of
the same design in (event_timestamp, customer_id) order, and C must equal c.csv. Runs in CI.

  python3 bench/aws/check_generate_steps.py
"""
import csv
import datetime
import io
import json
import os
import subprocess
import sys
import tempfile

import fastavro

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import benchmark as b  # noqa: E402


def frames(path):
    with open(path, "rb") as fh:
        while True:
            head = fh.read(4)
            if not head:
                return
            yield fh.read(int.from_bytes(head, "big"))


EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def csv_value(v):
    """The value as benchmark.CsvSink writes it; fastavro decodes timestamp-millis as a datetime."""
    if isinstance(v, datetime.datetime):
        v = (v - EPOCH) // datetime.timedelta(milliseconds=1)
    return "" if v is None else ("true" if v is True else "false" if v is False else str(v))


def main():
    cfg = {"seed": 20261113, "n_entities": 25, "h_events": 12, "k_attributes": 67,
           "event_models": ["full"], "changed_attributes_per_event": {"min": 1, "max": 5},
           "payload": {"deflate_level": 6}}
    attrs = b.attributes(cfg["k_attributes"])
    with tempfile.TemporaryDirectory() as tmp:
        cfg_path = os.path.join(tmp, "config.json")
        json.dump(cfg, open(cfg_path, "w"))
        csv_dir, step_dir = os.path.join(tmp, "csv"), os.path.join(tmp, "steps")
        os.makedirs(csv_dir)
        b.generate(cfg, "full", cfg["n_entities"], cfg["h_events"], attrs, csv_dir, None)
        subprocess.run([sys.executable, os.path.join(HERE, "generate_steps.py"), "--config", cfg_path, "--out", step_dir],
                       check=True, stdout=subprocess.DEVNULL)
        checked = 0
        for design, csv_name, with_prev, name in (("A", "a.csv", True, "PreviousValue"), ("B", "b.csv", False, "AuditTrail")):
            schema = fastavro.parse_schema(b.avro_schema(name, attrs, with_prev))
            rows = list(csv.reader(open(os.path.join(csv_dir, csv_name))))
            header, rows = rows[0], rows[1:]
            rows.sort(key=lambda r: (int(r[1]), r[0]))
            got = []
            for step in range(1, cfg["h_events"] + 1):
                for d in frames(os.path.join(step_dir, design, "step-%03d.bin" % step)):
                    rec = fastavro.schemaless_reader(io_bytes(d), schema)
                    got.append([csv_value(rec[c]) for c in header])
            assert got == rows, "design %s step files differ from benchmark.generate()" % design
            checked += len(rows)
        schema = fastavro.parse_schema(b.avro_schema("CurrentState", attrs, False))
        rows = list(csv.reader(open(os.path.join(csv_dir, "c.csv"))))
        header, rows = rows[0], rows[1:]
        got = [[csv_value(fastavro.schemaless_reader(io_bytes(d), schema)[c]) for c in header]
               for d in frames(os.path.join(step_dir, "C", "current.bin"))]
        assert got == rows, "design C differs from benchmark.generate()"
        checked += len(rows)
        manifest = json.load(open(os.path.join(step_dir, "manifest.json")))
        print("step files match benchmark.generate(): %d records; consistency checks %s" % (checked, manifest["checks"]))


def io_bytes(d):
    return io.BytesIO(d)


if __name__ == "__main__":
    main()
