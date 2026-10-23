"""Step 5: bytes per event and of prior_state alone, as JSON and as Avro, and Parquet
bytes of the whole capture with and without the prior_state columns.

Per event, from every captured line:
  json_received   the event exactly as EventStreams sent it (UTF-8 bytes)
  json            the parsed event written back compactly: json.dumps with separators
                  (",", ":") and ensure_ascii=False, keys in received order
  json_prior      prior_state alone, written the same way; 0 when the event has none
  avro            Avro binary datum of the event under the derived schema
                  (schema/page_change_1.12.0.avsc), fastavro schemaless_writer: datum
                  bytes only, no container or wire-format framing
  avro_prior      Avro binary datum of prior_state alone under its record schema; 0 when
                  the event has none
The four fields EventStreams adds to meta (topic, partition, offset, key) are not in
the published schema, so the Avro datum leaves them out while both JSON sizes keep them.
The pass also counts explicit JSON nulls and keys outside the schema.
"""
import gzip
import io
import json
import os
import sys
from array import array
from multiprocessing import Pool

import fastavro

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import schema  # noqa: E402

MEASURES = ("json_received", "json", "json_prior", "avro", "avro_prior")
_STATE = {}


def _init(avsc):
    avro = schema.load_avro(avsc)
    prior = schema.non_null(schema.field(avro, "prior_state")["type"])
    _STATE["event"] = fastavro.parse_schema(json.loads(json.dumps(avro)))
    _STATE["prior"] = fastavro.parse_schema(json.loads(json.dumps(prior)))
    _STATE["tree"] = avro


def _avro_datum(value, t):
    """JSON value -> Avro datum: JSON keys renamed to Avro field names."""
    t = schema.non_null(t)
    if value is None or isinstance(t, str):
        return value
    if t["type"] == "record":
        return {f["name"]: _avro_datum(value.get(f["json_name"]), f["type"]) for f in t["fields"]}
    if t["type"] == "map":
        return {k: _avro_datum(v, t["values"]) for k, v in value.items()}
    if t["type"] == "array":
        return [_avro_datum(v, t["items"]) for v in value]
    return value


def _extras(value, t, path, out, nulls):
    """Count keys outside the schema and explicit nulls."""
    t = schema.non_null(t)
    if value is None:
        nulls[path] = nulls.get(path, 0) + 1
        return
    if isinstance(t, dict) and t["type"] == "record" and isinstance(value, dict):
        names = {f["json_name"]: f for f in t["fields"]}
        for k, v in value.items():
            if k in names:
                _extras(v, names[k]["type"], path + "." + k, out, nulls)
            else:
                out[path + "." + k] = out.get(path + "." + k, 0) + 1
    elif isinstance(t, dict) and t["type"] == "map" and isinstance(value, dict):
        for v in value.values():
            _extras(v, t["values"], path + ".<slot>", out, nulls)


def _encode(sch, datum):
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, sch, datum)
    return buf.tell()


def measure_file(path):
    tree = _STATE["tree"]
    prior_type = schema.field(tree, "prior_state")["type"]
    cols = {m: array("q") for m in MEASURES}
    kinds = []
    extras, nulls = {}, {}
    with gzip.open(path, "rb") as f:
        for line in f:
            i = line.index(b',"event":')
            raw = line[i + 9:].rstrip(b"\n")[:-1]
            ev = json.loads(raw)
            cols["json_received"].append(len(raw))
            cols["json"].append(len(json.dumps(ev, separators=(",", ":"), ensure_ascii=False).encode("utf-8")))
            prior = ev.get("prior_state")
            cols["json_prior"].append(0 if prior is None else
                                      len(json.dumps(prior, separators=(",", ":"), ensure_ascii=False).encode("utf-8")))
            cols["avro"].append(_encode(_STATE["event"], _avro_datum(ev, tree)))
            cols["avro_prior"].append(0 if prior is None else _encode(_STATE["prior"], _avro_datum(prior, prior_type)))
            kinds.append(ev.get("page_change_kind"))
            _extras(ev, tree, "event", extras, nulls)
    return {m: cols[m].tobytes() for m in MEASURES}, kinds, extras, nulls


def measure_capture(files, avsc, processes=None):
    """Per-event arrays for every file, in file order."""
    cols = {m: array("q") for m in MEASURES}
    kinds, extras, nulls = [], {}, {}
    with Pool(processes, initializer=_init, initargs=(avsc,)) as pool:
        for data, k, ex, nu in pool.imap(measure_file, files):
            for m in MEASURES:
                a = array("q")
                a.frombytes(data[m])
                cols[m].extend(a)
            kinds.extend(k)
            for d, src in ((extras, ex), (nulls, nu)):
                for key, v in src.items():
                    d[key] = d.get(key, 0) + v
    return cols, kinds, extras, nulls


def parquet_sizes(con, canonical, work, row_group_size=None):
    """The canonical file's rows written four ways: all columns or no prior_state
    columns, snappy or zstd. Rows keep the canonical file's order."""
    cols = [c for (c,) in con.execute("SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('%s'))"
                                      % canonical).fetchall()]
    prior = [c for c in cols if c.startswith("prior_state__")]
    rest = [c for c in cols if not c.startswith("prior_state__")]
    out = []
    con.execute("SET preserve_insertion_order = true")
    for label, chosen in (("with prior_state", cols), ("without prior_state", rest)):
        for comp in ("snappy", "zstd"):
            path = os.path.join(work, "size_%s_%s.parquet" % ("with" if chosen is cols else "without", comp))
            opts = "FORMAT PARQUET, COMPRESSION %s" % comp
            if row_group_size:
                opts += ", ROW_GROUP_SIZE %d" % row_group_size
            con.execute("COPY (SELECT %s FROM read_parquet('%s')) TO '%s' (%s)"
                        % (", ".join('"%s"' % c for c in chosen), canonical, path, opts))
            rows, groups = con.execute("SELECT max(row_group_id) + 1, count(DISTINCT row_group_id) FROM "
                                       "parquet_metadata('%s')" % path).fetchone()
            out.append({"columns": label, "compression": comp, "column_count": len(chosen),
                        "prior_state_columns": len(prior) if chosen is cols else 0,
                        "rows": con.execute("SELECT count(*) FROM read_parquet('%s')" % path).fetchone()[0],
                        "row_groups": groups, "bytes": os.path.getsize(path)})
            os.remove(path)
    return out
