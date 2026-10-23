"""Step 3: load captured page-change events into Parquet and describe the ordering keys.

Every captured line {"recv_ms","sse_id","event"} becomes one row. The event is read
with DuckDB's JSON reader using a type derived from the published schema (schema.py)
plus the four fields EventStreams adds to meta (topic, partition, offset, key). Every
schema leaf becomes a column named by its path joined with "__"; content_slots maps
become one group of columns per slot role found in the data. A null therefore means
the field was absent: the captured JSON holds no explicit nulls (checked by
size.py, which reads every event).

Entity key: (wiki_id, page__page_id). Rows are sorted by the entity and the ordering
key (ORDER_BY below) and written with zstd; the other steps read this file.
"""
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import schema  # noqa: E402

ENTITY = ("wiki_id", "page__page_id")
# Event time, then the revision ID, as the article's (event_timestamp, event_sequence).
# Topic and offset only make the order total when both are tied.
ORDER_BY = ("meta__dt", "revision__rev_id", "meta__topic", "meta__offset")
EXTRA_META = [("topic", "VARCHAR"), ("partition", "BIGINT"), ("offset", "BIGINT"),
              ("key", 'STRUCT("wiki_id" VARCHAR, "page_id" BIGINT)')]
ORDERINGS = [
    ("meta.dt", ("meta__dt",)),
    ("dt", ("dt",)),
    ("revision.rev_id", ("revision__rev_id",)),
    ("meta.dt, revision.rev_id", ("meta__dt", "revision__rev_id")),
    ("revision.rev_id, meta.dt", ("revision__rev_id", "meta__dt")),
    ("Kafka topic, offset", ("meta__topic", "meta__offset")),
]


def read_manifests(capture_dirs):
    out = []
    for d in capture_dirs:
        with open(os.path.join(d, "manifest.json"), encoding="utf-8") as f:
            m = json.load(f)
        m["dir"] = os.path.abspath(d)
        out.append(m)
    return out


def combined_capture(manifests):
    """Per topic: captured offset ranges in order, gaps inside and between captures."""
    topics = {}
    for m in manifests:
        for key, t in m["topics"].items():
            if t["first_offset"] is None:
                continue
            topics.setdefault(key, []).append(dict(t, capture=os.path.basename(m["dir"])))
    out = {}
    for key, segs in topics.items():
        segs.sort(key=lambda s: s["first_offset"])
        gaps = [dict(g, where="inside " + s["capture"]) for s in segs for g in s["gaps"]]
        for a, b in zip(segs, segs[1:]):
            if a["end_offset"] != b["first_offset"]:
                gaps.append({"topic": key, "after_offset": a["end_offset"] - 1, "next_offset": b["first_offset"],
                             "missing": b["first_offset"] - a["end_offset"], "after_dt": a["last_dt"],
                             "next_dt": b["first_dt"], "where": "between %s and %s" % (a["capture"], b["capture"])})
        out[key] = {"segments": segs, "gaps": gaps,
                    "first_offset": segs[0]["first_offset"], "first_dt": segs[0]["first_dt"],
                    "end_offset": segs[-1]["end_offset"], "end_dt": segs[-1]["end_dt"],
                    "written": sum(s["written"] for s in segs), "canary": sum(s["canary"] for s in segs),
                    "duplicates": sum(s["duplicates"] for s in segs)}
    return out


def event_files(manifests):
    files = []
    for m in manifests:
        for f in m["files"]:
            files.append(os.path.join(m["dir"], f["path"]))
    return files


def slot_roles(con, files, duck_event):
    sql = ("SELECT DISTINCT r FROM (SELECT unnest(map_keys(event.revision.content_slots)) AS r FROM %s "
           "UNION ALL SELECT unnest(map_keys(event.prior_state.revision.content_slots)) FROM %s) ORDER BY r"
           % (reader(files, duck_event), reader(files, duck_event)))
    return [r for (r,) in con.execute(sql).fetchall()]


def reader(files, duck_event):
    cols = "{'recv_ms': 'BIGINT', 'sse_id': 'VARCHAR', 'event': '%s'}" % duck_event.replace("'", "''")
    return ("read_ndjson([%s], columns=%s, format='newline_delimited', compression='gzip', "
            "maximum_object_size=67108864)" % (", ".join("'%s'" % f for f in files), cols))


def flat_columns(avro, roles):
    """(column, SQL expression over the struct column `event`, json path) for every leaf."""
    cols = []
    meta = schema.field(avro, "meta")["type"]
    meta = schema.non_null(meta)
    extended = dict(avro)
    extended["fields"] = []
    for f in avro["fields"]:
        if f["json_name"] == "meta":
            mf = dict(meta)
            mf["fields"] = meta["fields"] + [{"name": n, "json_name": n, "type": "x"} for n, _ in EXTRA_META]
            f = dict(f, type=mf)
        extended["fields"].append(f)
    for path, t in schema.leaves(extended):
        expr = "event"
        for key in path:
            expr = "struct_extract(%s, '%s')" % (expr, key)
        if path[-1] == "key" and path[:-1] == ("meta",):
            for sub in ("wiki_id", "page_id"):
                cols.append(("meta__key__" + sub, "struct_extract(%s, '%s')" % (expr, sub), path + (sub,)))
            continue
        if isinstance(t, dict) and t["type"] == "map":
            for role in roles:
                for vf in t["values"]["fields"]:
                    name = "__".join([schema.avro_name(p) for p in path] + [schema.avro_name(role), vf["name"]])
                    cols.append((name, "struct_extract(map_extract_value(%s, '%s'), '%s')" % (expr, role, vf["json_name"]),
                                 path + (role, vf["json_name"])))
            continue
        cols.append(("__".join(schema.avro_name(p) for p in path), expr, path))
    return cols


def load(con, manifests, avro, parquet_path, temp_dir):
    """Write the canonical Parquet file; returns the column list and slot roles."""
    files = event_files(manifests)
    duck_event = schema.duck_type(avro, {"page_change__meta": EXTRA_META})
    roles = slot_roles(con, files, duck_event)
    cols = flat_columns(avro, roles)
    con.execute("SET temp_directory = '%s'" % temp_dir)
    # The sort spills to temp_dir; a cap makes a full disk fail the load instead.
    con.execute("SET max_temp_directory_size = '%s'" % os.environ.get("WIKIMEDIA_MAX_TEMP", "12GB"))
    con.execute("SET preserve_insertion_order = false")
    select = ",\n  ".join('%s AS "%s"' % (expr, name) for name, expr, _ in cols)
    con.execute("COPY (SELECT %s FROM %s ORDER BY %s) TO '%s' (FORMAT PARQUET, COMPRESSION zstd)"
                % (select, reader(files, duck_event), ", ".join(ENTITY + ORDER_BY), parquet_path))
    return cols, roles


def check_loaded(con, parquet_path, manifests):
    rows, distinct, canary = con.execute(
        "SELECT count(*), count(DISTINCT (meta__topic, meta__partition, meta__offset)), "
        "count(*) FILTER (WHERE meta__domain = 'canary') FROM read_parquet('%s')" % parquet_path).fetchone()
    expected = sum(f["lines"] for m in manifests for f in m["files"])
    if rows != expected or distinct != rows or canary:
        raise RuntimeError("loaded %d rows, %d distinct offsets, %d canaries; manifests list %d lines"
                           % (rows, distinct, canary, expected))
    return rows


def ordering_ties(con, parquet_path):
    """Per candidate ordering: tied events within an entity and the number of tie groups."""
    out = []
    ent = ", ".join(ENTITY)
    for label, key in ORDERINGS:
        k = ", ".join(key)
        nulls = con.execute("SELECT count(*) FROM read_parquet('%s') WHERE %s"
                            % (parquet_path, " OR ".join("%s IS NULL" % c for c in key))).fetchone()[0]
        groups, events = con.execute(
            "SELECT count(*), coalesce(sum(n), 0) FROM (SELECT count(*) AS n FROM read_parquet('%s') "
            "GROUP BY %s, %s HAVING count(*) > 1)" % (parquet_path, ent, k)).fetchone()
        out.append({"ordering": label, "columns": " ,".join(key).replace(" ,", ", "), "rows_with_null_key": nulls,
                    "tie_groups": groups, "tied_events": int(events)})
    return out


def main():
    import argparse
    import duckdb
    ap = argparse.ArgumentParser()
    ap.add_argument("--capture", action="append", required=True)
    ap.add_argument("--avro", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema",
                                                   "page_change_1.12.0.avsc"))
    ap.add_argument("--work", required=True)
    args = ap.parse_args()
    os.makedirs(args.work, exist_ok=True)
    con = duckdb.connect()
    manifests = read_manifests(args.capture)
    path = os.path.join(args.work, "events.parquet")
    cols, roles = load(con, manifests, schema.load_avro(args.avro), path, args.work)
    print("rows", check_loaded(con, path, manifests), "columns", len(cols), "slot roles", roles)
    for r in ordering_ties(con, path):
        print(r)
    print(json.dumps(combined_capture(manifests), indent=1)[:2000])


if __name__ == "__main__":
    main()
