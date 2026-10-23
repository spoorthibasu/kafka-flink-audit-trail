#!/usr/bin/env python3
"""Wikimedia field study, steps 3 to 7, on one or more capture directories.

  python3 study/wikimedia/study.py \\
      --capture ~/audit-trail-bench-data/wikimedia/capture-earliest-to-2026-10-20T00Z \\
      --capture ~/audit-trail-bench-data/wikimedia/capture-2026-10-20T00Z-2026-10-23T02Z \\
      --work ~/audit-trail-bench-data/wikimedia/work --out results/wikimedia --summary results/SUMMARY.md

  3 load.py           Parquet of every captured event; ordering key and its ties
  4 reconstruct.py    prior_state against LAG, per field; mismatch classes; rev_parent_id
  5 size.py           JSON and Avro bytes per event and of prior_state; Parquet bytes
  6 distributions.py  prior_state field presence and string lengths
  7 query_cost.py     stored prior_state versus LAG: latency, CPU time, bytes scanned

Writes CSV files and environment.json to --out, the reconstruction query to
--out/reconstruction.sql, and, with --summary, a "## Wikimedia field study" section to
that file, replacing an earlier one and leaving every other section as it is. Raw
events and Parquet files stay in --work. With --expect, compares the results with a
synthetic sample's planted counts (synthetic.py) and exits non-zero on a difference.
"""
import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import time
from fractions import Fraction

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "bench"))
import distributions  # noqa: E402
import load  # noqa: E402
import query_cost  # noqa: E402
import reconstruct  # noqa: E402
import schema  # noqa: E402
import size  # noqa: E402
from benchmark import fmt_bytes, machine_info, md_table, pct  # noqa: E402
from resources import exact  # noqa: E402
from summary_sections import exact_or_prefix, replace_section  # noqa: E402

HEADING = "## Wikimedia field study"
AVSC = os.path.join(HERE, "schema", "page_change_1.12.0.avsc")
SCHEMA_URL = "https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/page/change/1.12.0"


def write_csv(path, rows, fields=None):
    fields = fields or list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: fmt(v) for k, v in r.items()})


def fmt(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, Fraction):
        return exact_or_prefix(v)
    if hasattr(v, "as_tuple"):  # Decimal from the profiler, written exactly
        return exact(Fraction(v))
    return v


def rate(num, den):
    return Fraction(num, den) if den else ""


def utc(ms):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000.0)) if ms else ""


# ------------------------------------------------------------------------- capture


def capture_rows(manifests, combined):
    seg_rows, gap_rows, reconnects = [], [], []
    for m in manifests:
        name = os.path.basename(m["dir"])
        for key, t in sorted(m["topics"].items()):
            seg_rows.append({"capture": name, "topic": key, "window_start_utc": utc(m["start_ms"]) or "earliest retained",
                             "window_end_utc": utc(m["end_ms"]),
                             "end_offset_given": (m.get("end_offsets") or {}).get(key, ""),
                             "first_offset": t["first_offset"], "first_meta_dt": t["first_dt"],
                             "end_offset": t["end_offset"], "end_meta_dt": t["end_dt"],
                             "offsets_in_range": (t["end_offset"] - t["first_offset"]) if t["first_offset"] is not None else 0,
                             "events_written": t["written"], "canary_dropped": t["canary"],
                             "duplicates_skipped": t["duplicates"], "received_after_end": t["after_end"],
                             "meta_dt_before_start": t["dt_before_start"], "gaps": len(t["gaps"]),
                             "started_utc": utc(m["started_ms"]), "finished_utc": utc(m["finished_ms"]),
                             "connects": m["connects"], "rate_cap_per_s": m["rate_cap"]})
        log_path = os.path.join(m["dir"], "capture_log.jsonl")
        if os.path.exists(log_path):
            with open(log_path, encoding="utf-8") as fh:
                for line in fh:
                    r = json.loads(line)
                    if r["kind"] in ("connected", "disconnected", "restart_from_checkpoint", "stopped_on_signal"):
                        reconnects.append({"capture": name, "kind": r["kind"], "utc": utc(r["ts_ms"]), "ts_ms": r["ts_ms"],
                                           "reason": r.get("reason", ""), "events_on_connection": r.get("events_on_connection", ""),
                                           "resume_id_sent": r.get("last_event_id", "") if r["kind"] == "connected" else ""})
    for key, c in sorted(combined.items()):
        for g in c["gaps"]:
            gap_rows.append(dict(g))
    return seg_rows, gap_rows, reconnects


# --------------------------------------------------------------------------- sizes


def size_rows(cols, kinds):
    n = len(kinds)
    with_prior = [i for i in range(n) if cols["json_prior"][i] > 0]
    out = []
    for label, idx in (("all events", range(n)), ("events with prior_state", with_prior)):
        for m in size.MEASURES:
            vals = [cols[m][i] for i in idx]
            if not vals:
                continue
            out.append({"population": label, "measure": m, "events": len(vals), "total_bytes": sum(vals),
                        "mean_bytes": Fraction(sum(vals), len(vals)), "min": min(vals), "median_p50": pct(vals, 50),
                        "p95": pct(vals, 95), "max": max(vals)})
    shares = []
    for enc in ("json", "avro"):
        whole, prior = cols[enc], cols[enc + "_prior"]
        ratios = sorted(((prior[i] / whole[i], prior[i], whole[i]) for i in with_prior))
        if not ratios:
            continue
        k50 = max(1, -(-50 * len(ratios) // 100))
        k95 = max(1, -(-95 * len(ratios) // 100))
        shares.append({"encoding": enc, "events_with_prior_state": len(ratios),
                       "share_p50": Fraction(ratios[k50 - 1][1], ratios[k50 - 1][2]),
                       "share_p95": Fraction(ratios[k95 - 1][1], ratios[k95 - 1][2]),
                       "share_min": Fraction(ratios[0][1], ratios[0][2]), "share_max": Fraction(ratios[-1][1], ratios[-1][2]),
                       "aggregate_share_with_prior_state": Fraction(sum(prior[i] for i in with_prior),
                                                                    sum(whole[i] for i in with_prior)),
                       "aggregate_share_all_events": Fraction(sum(prior), sum(whole))})
    return out, shares


# --------------------------------------------------------------------------- main


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capture", action="append", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--summary")
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--warmup", type=int, default=3)
    ap.add_argument("--buckets", type=int, default=16)
    ap.add_argument("--processes", type=int, default=None)
    ap.add_argument("--expect")
    args = ap.parse_args()
    t_start = time.perf_counter()

    def log(msg):
        print("[%7.1fs] %s" % (time.perf_counter() - t_start, msg), flush=True)

    os.makedirs(args.work, exist_ok=True)
    os.makedirs(args.out, exist_ok=True)
    env = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "machine": machine_info(),
           "duckdb": duckdb.__version__, "schema_url": SCHEMA_URL,
           "avro_schema": os.path.relpath(AVSC, REPO),
           "avro_schema_sha256": hashlib.sha256(open(AVSC, "rb").read()).hexdigest(),
           "captures": [os.path.basename(os.path.abspath(c)) for c in args.capture],
           "runs": args.runs, "warmup_runs_per_run": args.warmup, "entity": list(load.ENTITY),
           "order_by": list(load.ORDER_BY)}
    avro = schema.load_avro(AVSC)
    manifests = load.read_manifests(args.capture)
    combined = load.combined_capture(manifests)
    seg_rows, gap_rows, reconnects = capture_rows(manifests, combined)
    write_csv(os.path.join(args.out, "capture.csv"), seg_rows)
    write_csv(os.path.join(args.out, "capture_gaps.csv"), gap_rows,
              ["topic", "where", "after_offset", "next_offset", "missing", "after_dt", "next_dt"])
    write_csv(os.path.join(args.out, "capture_connections.csv"), reconnects,
              ["capture", "kind", "utc", "ts_ms", "reason", "events_on_connection", "resume_id_sent"])

    # Step 3
    con = duckdb.connect()
    con.execute("SET temp_directory = '%s'" % args.work)
    parquet = os.path.join(args.work, "events.parquet")
    log("step 3: loading %d files" % len(load.event_files(manifests)))
    cols, roles = load.load(con, manifests, avro, parquet, args.work)
    rows = load.check_loaded(con, parquet, manifests)
    env["slot_roles"] = roles
    env["columns"] = len(cols)
    ties = load.ordering_ties(con, parquet)
    log("step 3: %d rows, %d columns" % (rows, len(cols)))

    # Step 4
    log("step 4: reconstruction")
    gaps = [g for c in combined.values() for g in c["gaps"]]
    pairs, ev, field_counts, classes, ev_classes = reconstruct.evaluate(con, parquet, gaps, args.buckets, log)
    with open(os.path.join(args.out, "reconstruction.sql"), "w") as fh:
        fh.write("-- Generated by study/wikimedia/reconstruct.py: the article's LAG form over the loaded capture.\n"
                 "-- engines: duckdb\n\n" + reconstruction_sql_text(pairs) + ";\n")
    ent = con.execute("SELECT count(DISTINCT (wiki_id, page__page_id)), count(DISTINCT wiki_id), "
                      "count(*) FILTER (WHERE prior_state__revision__rev_id IS NOT NULL OR prior_state__page__page_title IS NOT NULL "
                      "OR prior_state__page__namespace_id IS NOT NULL OR prior_state__page__is_redirect IS NOT NULL) "
                      "FROM events").fetchone()
    both, topic_rows = reconstruct.topics(con)
    lineage = reconstruct.lineage(con, args.buckets)
    parents = reconstruct.parent_check(con)
    kinds_count = con.execute("SELECT page_change_kind, count(*) FROM events GROUP BY 1 ORDER BY 1").fetchall()
    versions = con.execute("SELECT _schema, count(*) FROM events GROUP BY 1 ORDER BY 1").fetchall()
    span = con.execute("SELECT min(meta__dt), max(meta__dt) FROM events").fetchone()
    env["schema_versions"] = dict(versions)
    counts = dict(ev, entities=ent[0], wikis=ent[1], pages_in_both_topics=both, first_meta_dt=span[0], last_meta_dt=span[1])
    counts_rows = [{"measure": k, "value": v} for k, v in counts.items()] + \
                  [{"measure": "events_%s" % k, "value": v} for k, v in kinds_count]
    write_csv(os.path.join(args.out, "reconstruction_counts.csv"), counts_rows)
    field_rows = []
    totals = dict.fromkeys(reconstruct.OUTCOMES, 0)
    for prior, cur in pairs:
        fc = field_counts.get(cur, dict.fromkeys(reconstruct.OUTCOMES, 0))
        for k in totals:
            totals[k] += fc[k]
        field_rows.append(field_row("prior_state." + cur.replace("__", "."), fc))
    field_rows.append(field_row("all fields", totals))
    write_csv(os.path.join(args.out, "reconstruction_fields.csv"), field_rows)
    class_rows = [{"cause": k[0], "detail": k[1], "field_group": k[2], "field": "prior_state." + k[3].replace("__", "."),
                   "outcome": k[4], "count": v["count"], "prior_greater": v["prior_greater"], "prior_less": v["prior_less"],
                   "examples_wiki_page_rev": "; ".join(":".join(x) for x in v["examples"])}
                  for k, v in sorted(classes.items(), key=lambda kv: (kv[0][0], kv[0][1], -kv[1]["count"], kv[0][3], kv[0][4]))]
    write_csv(os.path.join(args.out, "mismatch_classes.csv"), class_rows,
              ["cause", "detail", "field_group", "field", "outcome", "count", "prior_greater", "prior_less",
               "examples_wiki_page_rev"])
    event_classes = [{"causes": k[0], "mismatching_field_groups": k[1], "events": v["events"],
                      "examples_wiki_page_rev": "; ".join(":".join(x) for x in v["examples"])}
                     for k, v in sorted(ev_classes.items(), key=lambda kv: (kv[0][0], -kv[1]["events"], kv[0][1]))]
    write_csv(os.path.join(args.out, "mismatch_events.csv"), event_classes,
              ["causes", "mismatching_field_groups", "events", "examples_wiki_page_rev"])
    write_csv(os.path.join(args.out, "lineage.csv"), lineage)
    write_csv(os.path.join(args.out, "parent_check.csv"), parents)
    order_rows = []
    by_label = {t["ordering"]: t for t in topic_rows}
    for t in ties:
        order_rows.append(dict(t, **{k: v for k, v in by_label[t["ordering"]].items() if k != "ordering"}))
    write_csv(os.path.join(args.out, "ordering.csv"), order_rows)
    log("step 4: %d eligible events, %d mismatch classes" % (ev["eligible"], len(classes)))

    # Step 5
    log("step 5: sizes")
    size_cols, size_kinds, extras, nulls = size.measure_capture(load.event_files(manifests), AVSC, args.processes)
    if len(size_kinds) != rows:
        raise RuntimeError("size pass read %d events, Parquet holds %d" % (len(size_kinds), rows))
    env["json_keys_outside_schema"] = extras
    env["json_explicit_nulls"] = nulls
    env["json_received_equals_compact"] = sum(1 for a, b in zip(size_cols["json_received"], size_cols["json"]) if a == b)
    size_ev, shares = size_rows(size_cols, size_kinds)
    write_csv(os.path.join(args.out, "size_events.csv"), size_ev)
    write_csv(os.path.join(args.out, "size_prior_state_share.csv"), shares)
    pq_rows = size.parquet_sizes(con, parquet, args.work)
    write_csv(os.path.join(args.out, "size_parquet.csv"), pq_rows)

    # Step 6
    log("step 6: distributions")
    kinds, dist = distributions.distributions(con, parquet)
    write_csv(os.path.join(args.out, "field_distributions.csv"), dist)
    con.close()

    # Step 7
    log("step 7: query cost")
    qc = query_cost.query_cost(parquet, args.runs, args.warmup, args.work, log)
    write_csv(os.path.join(args.out, "query_cost.csv"), qc)

    env["wall_seconds"] = round(time.perf_counter() - t_start, 1)
    env["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(os.path.join(args.out, "environment.json"), "w") as fh:
        json.dump(env, fh, indent=2)
    results = {"manifests": manifests, "combined": combined, "seg_rows": seg_rows, "reconnects": reconnects,
               "rows": rows, "ties": ties, "counts": counts, "kinds_count": kinds_count, "field_rows": field_rows,
               "class_rows": class_rows, "event_classes": event_classes, "parents": parents, "order_rows": order_rows,
               "size_ev": size_ev, "shares": shares, "pq_rows": pq_rows, "dist": dist, "kinds": kinds, "qc": qc,
               "env": env, "lineage": lineage}
    if args.summary:
        if not os.path.exists(args.summary):
            with open(args.summary, "w") as fh:
                fh.write("# Summary\n")
        replace_section(args.summary, HEADING, summary_section(results, args))
        log("SUMMARY section written to %s" % args.summary)
    if args.expect:
        check_expected(args.expect, results)
        log("synthetic expectations met")
    log("done")


def reconstruction_sql_text(pairs):
    return reconstruct.reconstruction_sql(pairs)


def field_row(name, fc):
    carried = fc["match"] + fc["differ"] + fc["prior_only"]
    omitted = fc["omitted_same"] + fc["omitted_changed"]
    return dict({"field": name}, **fc, carried=carried, exact_match_rate=rate(fc["match"], carried),
                omitted=omitted, omitted_consistent_rate=rate(fc["omitted_same"], omitted),
                compared=sum(fc.values()))


def summary_section(r, args):
    env, c = r["env"], r["counts"]
    lines = []
    w = lines.append
    cmd_study = "python3 study/wikimedia/study.py %s --work <work dir> --out %s --summary %s" % (
        " ".join("--capture <data>/%s" % x for x in env["captures"]), os.path.relpath(args.out, REPO),
        os.path.relpath(args.summary, REPO))
    w(HEADING)
    w("")
    if args.expect:
        w("Synthetic sample generated by `study/wikimedia/synthetic.py` for CI, shaped like a capture; not Wikimedia "
          "data. It checks that the steps run and find the counts the generator planted.")
    else:
        w("Not synthetic: a capture of Wikimedia's public `mediawiki.page_change.v1` stream from EventStreams "
          "(https://stream.wikimedia.org/v2/stream/mediawiki.page_change.v1), read with `study/wikimedia/capture.py`. "
          "Raw events are not in the repository; `study/wikimedia/README.md` gives the commands to capture again.")
    w("")
    w("Every number below was measured by `%s` on %s (%s logical CPUs), DuckDB %s, wall time %s s. "
      "Rates are exact fractions, shown with their first ten decimals truncated where they do not terminate."
      % (cmd_study, env["machine"].get("cpu", env["machine"]["platform"]), env["machine"]["cpu_count"], env["duckdb"],
         env["wall_seconds"]))
    w("")
    w("### Capture")
    w("")
    w("Each topic is one Kafka partition per Wikimedia datacenter. The capture resumes after every disconnect "
      "from the next Kafka offset (`meta.offset`) of each topic and logs any jump in offsets as a gap. Canary "
      "events (`meta.domain` = canary) are dropped. Commands: `capture.py --start earliest --end-offsets ...` "
      "for the oldest retained events up to where the second part began, and `capture.py --start ... --end ...` "
      "for the second part, both at --rate 1000 events per second on one connection.")
    w("")
    w(md_table(["Capture", "Topic", "First offset", "First meta.dt", "End offset (exclusive)", "End meta.dt",
                "Events written", "Canaries dropped", "Duplicates skipped", "Received after end", "Offset gaps"],
               [[x["capture"], x["topic"], x["first_offset"], x["first_meta_dt"], x["end_offset"], x["end_meta_dt"],
                 fmt_bytes(x["events_written"]), x["canary_dropped"], x["duplicates_skipped"], fmt_bytes(x["received_after_end"]),
                 x["gaps"]] for x in r["seg_rows"]]))
    w("")
    conn = {}
    for x in r["reconnects"]:
        conn.setdefault((x["capture"], x["kind"], x["reason"]), 0)
        conn[(x["capture"], x["kind"], x["reason"])] += 1
    w(md_table(["Capture", "Connection record", "Reason", "Count"],
               [[k[0], k[1], k[2], v] for k, v in sorted(conn.items())]))
    w("")
    gaps = [g for t in r["combined"].values() for g in t["gaps"]]
    w("Offset gaps across both parts and between them: %d. %s" % (len(gaps), "Every offset from the first to the end "
      "offset of each topic was received once." if not gaps else "They are listed in capture_gaps.csv; events whose "
      "predecessor interval overlaps one are excluded from the comparison."))
    w("")
    w("### Stream and schema")
    w("")
    w("Loaded events: %s, from meta.dt %s to %s. Schema versions (`$schema`): %s. Events per page_change_kind: %s." % (
        fmt_bytes(r["rows"]), c["first_meta_dt"], c["last_meta_dt"],
        ", ".join("%s %s" % (k, fmt_bytes(v)) for k, v in env["schema_versions"].items()),
        ", ".join("%s %s" % (k, fmt_bytes(v)) for k, v in r["kinds_count"])))
    w("")
    dist = {d["field"]: d for d in r["dist"]}
    w("prior_state fields present, per page_change_kind (events of that kind carrying the field):")
    w("")
    w(md_table(["Field"] + r["kinds"],
               [[d["field"]] + ["%s of %s" % (fmt_bytes(d["present_%s" % k]), fmt_bytes(d["events_%s" % k])) for k in r["kinds"]]
                for d in r["dist"]]))
    w("")
    w("### Ordering key")
    w("")
    w("Entity key (wiki_id, page.page_id). Ordering key `%s`: event time (meta.dt, millisecond precision, set "
      "when the event is produced) and then the revision ID, the article's (event_timestamp, event_sequence). "
      "rev_id is assigned by the wiki's database in commit order, so it breaks equal timestamps the way "
      "event_sequence does; Kafka offsets order events only within one datacenter topic, so they only make the "
      "order total when both are tied. Tied events below are events sharing their entity and every column of the "
      "candidate. \"Equal\" counts events whose LAG(revision.rev_id) under that ordering equals "
      "prior_state.revision.rev_id, out of events with both." % ", ".join(env["order_by"]))
    w("")
    w(md_table(["Ordering", "Tie groups", "Tied events", "Equal / compared", "Pages in both topics: equal / compared",
                "Cross-topic pairs", "Cross-topic: equal / compared"],
               [[o["ordering"], fmt_bytes(o["tie_groups"]), fmt_bytes(o["tied_events"]),
                 "%s / %s" % (fmt_bytes(o["rev_id_equal"]), fmt_bytes(o["compared"])),
                 "%s / %s" % (fmt_bytes(o["rev_id_equal_pages_in_both_topics"]), fmt_bytes(o["compared_pages_in_both_topics"])),
                 fmt_bytes(o["cross_topic_pairs"]),
                 "%s / %s" % (fmt_bytes(o["cross_topic_rev_id_equal"]), fmt_bytes(o["cross_topic_compared"]))]
                for o in r["order_rows"]]))
    w("")
    w("Pages with events in both the eqiad and the codfw topic: %s." % fmt_bytes(c["pages_in_both_topics"]))
    w("")
    w("### Reconstruction with LAG")
    w("")
    w("The query is the article's LAG form (`reconstruction.sql`): one LAG per prior_state field over the "
      "entity, ordered by the key above. An event is compared when its predecessor was captured and no offset "
      "gap falls between the two events' meta.dt.")
    w("")
    w(md_table(["Measure", "Value"], [
        ["Events", fmt_bytes(c["events"])], ["Entities (wiki_id, page_id)", fmt_bytes(c["entities"])],
        ["Wikis", fmt_bytes(c["wikis"])],
        ["Excluded: no captured predecessor", fmt_bytes(c["excluded_no_predecessor"])],
        ["Excluded: predecessor interval not loss-free", fmt_bytes(c["excluded_loss_possible"])],
        ["Compared", fmt_bytes(c["eligible"])],
        ["Compared, no field mismatched", fmt_bytes(c["eligible_without_mismatch"])]]))
    w("")
    w("Per field, over compared events. Carried: prior_state has the field; exact match: equal to LAG. "
      "Omitted: prior_state lacks the field; unchanged: LAG equals the event's own value, as \"only present if "
      "changed\" implies.")
    w("")
    w(md_table(["Field", "Carried", "Match", "Differ", "LAG null", "Exact-match rate", "Omitted", "Omitted, unchanged",
                "Omitted, changed", "Omission-consistent rate"],
               [[f["field"], fmt_bytes(f["carried"]), fmt_bytes(f["match"]), fmt_bytes(f["differ"]), fmt_bytes(f["prior_only"]),
                 fmt(f["exact_match_rate"]), fmt_bytes(f["omitted"]), fmt_bytes(f["omitted_same"]),
                 fmt_bytes(f["omitted_changed"]), fmt(f["omitted_consistent_rate"])] for f in r["field_rows"]]))
    w("")
    w("Mismatch classes. A mismatch is a compared (event, field) pair that differs, that LAG leaves null, or that "
      "prior_state omits although LAG shows it changed. Causes follow the rules in `reconstruct.py`; examples are "
      "wiki_id:page_id:rev_id. Per-field rows are in mismatch_classes.csv.")
    w("")
    agg = {}
    for x in r["class_rows"]:
        a = agg.setdefault((x["cause"], x["detail"]), {"count": 0, "fields": set(), "examples": []})
        a["count"] += x["count"]
        a["fields"].add(x["field"])
        a["examples"] = sorted(set(a["examples"]) | set(e for e in x["examples_wiki_page_rev"].split("; ") if e))[:5]
    w(md_table(["Cause", "Detail", "(event, field) mismatches", "Fields", "Examples"],
               [[k[0], k[1], fmt_bytes(v["count"]), len(v["fields"]), "; ".join(v["examples"])]
                for k, v in sorted(agg.items(), key=lambda kv: (kv[0][0], -kv[1]["count"]))]))
    w("")
    w("Per event: causes of its mismatched fields and the field groups involved.")
    w("")
    w(md_table(["Causes", "Field groups", "Events", "Examples"],
               [[x["causes"], x["mismatching_field_groups"], fmt_bytes(x["events"]), x["examples_wiki_page_rev"]]
                for x in sorted(r["event_classes"], key=lambda x: -x["events"])]))
    w("")
    w("prior_state.revision.rev_id against revision.rev_parent_id:")
    w("")
    w(md_table(["page_change_kind", "Events", "Equal", "Differ", "Only prior_state", "Only rev_parent_id", "Neither"],
               [[p["page_change_kind"], fmt_bytes(p["events"]), fmt_bytes(p["equal"]), fmt_bytes(p["differ"]),
                 fmt_bytes(p["prior_only"]), fmt_bytes(p["parent_only"]), fmt_bytes(p["neither"])] for p in r["parents"]]))
    w("")
    w("### Size")
    w("")
    w("Per event: json is the event as received (compact JSON; writing the parsed event back with "
      "separators (\",\", \":\") gives the same bytes for %s of %s events); avro is the Avro binary datum under "
      "`%s`, derived by `study/wikimedia/schema.py` from %s: object to record, object with only "
      "additionalProperties to map, array to array, string to string, integer to long, number to double, boolean "
      "to boolean, a property outside `required` to a union with null. The four meta fields EventStreams adds "
      "(topic, partition, offset, key) are outside the published schema, so the Avro datum omits them. Percentiles "
      "are nearest-rank." % (fmt_bytes(env["json_received_equals_compact"]), fmt_bytes(r["rows"]), env["avro_schema"],
                              env["schema_url"]))
    w("")
    w(md_table(["Population", "Measure", "Events", "Median bytes", "p95 bytes", "Min", "Max", "Total bytes"],
               [[x["population"], x["measure"], fmt_bytes(x["events"]), x["median_p50"], x["p95"], x["min"], x["max"],
                 fmt_bytes(x["total_bytes"])] for x in r["size_ev"] if x["measure"] != "json_received"]))
    w("")
    w("Share of each event that prior_state takes, over events that carry it:")
    w("")
    w(md_table(["Encoding", "Events", "Median share", "p95 share", "Min", "Max", "Sum of prior_state / sum of events",
                "Same, over all events"],
               [[x["encoding"], fmt_bytes(x["events_with_prior_state"]), fmt(x["share_p50"]), fmt(x["share_p95"]),
                 fmt(x["share_min"]), fmt(x["share_max"]), fmt(x["aggregate_share_with_prior_state"]),
                 fmt(x["aggregate_share_all_events"])] for x in r["shares"]]))
    w("")
    w("Parquet of the whole capture, one column per leaf field, rows sorted by entity and ordering key, DuckDB "
      "%s default row groups:" % env["duckdb"])
    w("")
    w(md_table(["Columns", "Compression", "Column count", "Rows", "Row groups", "Bytes"],
               [[x["columns"], x["compression"], x["column_count"], fmt_bytes(x["rows"]), x["row_groups"],
                 fmt_bytes(x["bytes"])] for x in r["pq_rows"]]))
    w("")
    w("### prior_state field distributions")
    w("")
    w("Presence over all events; string lengths over present values in UTF-8 bytes, nearest-rank.")
    w("")
    w(md_table(["Field", "Present", "Of events", "Length p50", "Length p95", "Length max"],
               [[d["field"], fmt_bytes(d["present"]), fmt_bytes(d["events"]), d.get("len_bytes_p50", ""),
                 d.get("len_bytes_p95", ""), d.get("len_bytes_max", "")] for d in r["dist"]]))
    w("")
    w("### Query cost: stored prior_state versus LAG")
    w("")
    w("On the zstd Parquet file, measured as in the resources stage: a fresh in-memory DuckDB per run, %d warm-up "
      "runs, then one profiled run; %d runs; p50 and p95 nearest-rank. CPU ms is the profiler's cpu_time, latency "
      "its latency; bytes scanned are the compressed column chunks the scan reads in row groups its filters do not "
      "prune, from parquet_metadata(). The previous value is rev_id, rev_dt, rev_size and rev_sha1." % (
          env["warmup_runs_per_run"], env["runs"]))
    w("")
    w(md_table(["Scope", "Method", "Rows", "Latency ms p50", "Latency ms p95", "CPU ms p50", "CPU ms p95",
                "Bytes scanned", "Columns read", "Row groups read"],
               [[q["scope"] + (" (%s, %s events)" % (q["entity"], q["entity_events"]) if q["entity"] else ""), q["method"],
                 fmt_bytes(q["rows_returned"]), fmt(q["latency_ms_p50"]), fmt(q["latency_ms_p95"]), fmt(q["cpu_ms_p50"]),
                 fmt(q["cpu_ms_p95"]), fmt_bytes(q["bytes_scanned"]), "%d of %d" % (q["columns_read"], q["columns_in_file"]),
                 "%d of %d" % (q["row_groups_read"], q["row_groups_in_file"])] for q in r["qc"]]))
    w("")
    w("Files in results/wikimedia: capture.csv, capture_gaps.csv, capture_connections.csv, ordering.csv, "
      "reconstruction_counts.csv, reconstruction_fields.csv, mismatch_classes.csv, mismatch_events.csv, lineage.csv, "
      "parent_check.csv, size_events.csv, size_prior_state_share.csv, size_parquet.csv, field_distributions.csv, "
      "query_cost.csv, reconstruction.sql, environment.json.")
    w("")
    return "\n".join(lines)


def check_expected(path, r):
    """Compare a synthetic run with the counts synthetic.py planted."""
    with open(path) as fh:
        exp = json.load(fh)
    c = r["counts"]
    got = {"rows": r["rows"], "entities": c["entities"], "wikis": c["wikis"],
           "excluded_no_predecessor": c["excluded_no_predecessor"], "excluded_loss_possible": c["excluded_loss_possible"],
           "pages_in_both_topics": c["pages_in_both_topics"],
           "gaps": sum(len(t["gaps"]) for t in r["combined"].values()),
           "parent_check_differ": sum(p["differ"] for p in r["parents"]),
           "ordering_tie_groups_meta_dt_rev_id": next(o["tie_groups"] for o in r["order_rows"]
                                                      if o["ordering"] == "meta.dt, revision.rev_id")}
    errors = ["%s: expected %s, got %s" % (k, exp[k], v) for k, v in got.items() if exp[k] != v]
    for cause, n in exp["event_causes"].items():
        found = sum(x["events"] for x in r["event_classes"] if cause in x["causes"].split(", "))
        if found != n:
            errors.append("events with cause %r: expected %d, got %d" % (cause, n, found))
    for fc in exp["field_classes"]:
        found = sum(x["count"] for x in r["class_rows"] if (x["cause"], x["detail"], x["field"], x["outcome"]) ==
                    (fc["cause"], fc["detail"], fc["field"], fc["outcome"]))
        if found != fc["count"]:
            errors.append("class %s / %s / %s / %s: expected %d, got %d" % (
                fc["cause"], fc["detail"], fc["field"], fc["outcome"], fc["count"], found))
    if any(x["population"] == "all events" and x["measure"] == "avro" and x["events"] != r["rows"] for x in r["size_ev"]):
        errors.append("Avro sizes missing for some events")
    if errors:
        raise SystemExit("synthetic expectations not met:\n  " + "\n  ".join(errors))


if __name__ == "__main__":
    main()
