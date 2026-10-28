#!/usr/bin/env python3
"""The "ordering" revision mismatches against each event's own parent revision.

reconstruct.py puts a revision mismatch (prior_state.revision.rev_id differs from
LAG(revision.rev_id)) in the "ordering" class when the embedded previous revision was
captured for the same page, in a row other than the LAG row. That rule is positional: it
does not say whether a row is out of place. Each revision carries its own parent,
revision.rev_parent_id, which MediaWiki records when it saves the revision. This check
compares the two candidates with it:

  misorder        the event's own rev_parent_id equals prior_state.revision.rev_id: the
                  revision history agrees with prior_state, so LAG returned a row that the
                  ordering put in the wrong place
  not ordering    the event's own rev_parent_id equals LAG(revision.rev_id): the revision
                  history agrees with LAG, and prior_state names an older revision

under the three orderings of ordering_sensitivity.csv. Under the primary key (event time,
then revision ID) it then splits every revision mismatch three ways (stale prior_state:
own parent equals LAG; misorder; and the rest, whose embedded revision no captured event
carries), describes each misorder (the LAG row and the row carrying the embedded revision)
and checks, for every stale prior_state event, that the embedded revision is exactly one
revision before the event's own parent: the own parent's rev_parent_id in the archive and,
with --api, in the wiki's revision history through the public MediaWiki Action API.

Raw events carry page titles, user names and comments: they stay in --raw-dir and --work,
outside the repository. Results hold counts, wiki_id:page_id:rev_id identifiers, revision
IDs and event times only; titles are compared, never written.

  python3 study/wikimedia/ordering_check.py download --archive s3://<bucket>/wikimedia-page-change-v1/ \\
      --profile audit-trail-bench --raw-dir <dir>
  python3 study/wikimedia/ordering_check.py analyze --raw-dir <dir> --work <dir> --out results/wikimedia --api
  python3 study/wikimedia/ordering_check.py summary --out results/wikimedia --summary results/SUMMARY.md
"""
import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "bench"))
import load  # noqa: E402
import reconstruct  # noqa: E402
import schema  # noqa: E402
from summary_sections import replace_section  # noqa: E402

HEADING = "## Wikimedia field study: ordering mismatches against parent revisions"
AVSC = os.path.join(HERE, "schema", "page_change_1.12.0.avsc")
PREFIX = "wikimedia-page-change-v1/"
REST = ("meta__dt", "meta__topic", "meta__offset")
ORDERINGS = [  # labels and keys of ordering_sensitivity.csv, each completed with load.ORDER_BY
    ("receive time: meta.dt", ("meta__dt",)),
    ("event time first: dt, rev_id (primary)", ("dt", "revision__rev_id")),
    ("sequence first: rev_id, dt", ("revision__rev_id", "dt")),
]
PRIMARY = ORDERINGS[1][0]
FILES = ("ordering_parent_check.csv", "ordering_parent_split.csv", "ordering_misorders.csv",
         "ordering_stale_prior.csv", "ordering_parent_check_environment.json")


def write_csv(path, rows, fields):
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def yes(v):
    return "yes" if v else "no"


# ---------------------------------------------------------------------- download


def verify(raw_dir):
    """Every file of SHA256SUMS.txt (hash, size, path under the prefix) against its copy."""
    n = 0
    for line in open(os.path.join(raw_dir, "SHA256SUMS.txt")):
        digest, size, path = line.split(None, 2)
        p = os.path.join(raw_dir, path.strip().split("/", 1)[1])
        h = hashlib.sha256()
        with open(p, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        if h.hexdigest() != digest or os.path.getsize(p) != int(size):
            raise SystemExit("%s does not match SHA256SUMS.txt" % path.strip())
        n += 1
    return n


def download(a):
    if not a.archive.startswith("s3://") or not a.archive.rstrip("/").endswith(PREFIX.rstrip("/")):
        raise SystemExit("--archive must be s3://<bucket>/%s" % PREFIX)
    os.makedirs(a.raw_dir, exist_ok=True)
    cmd = ["aws", "s3", "cp", a.archive.rstrip("/") + "/", a.raw_dir, "--recursive", "--only-show-errors"]
    subprocess.run(cmd + (["--profile", a.profile] if a.profile else []), check=True)
    print("%d files match SHA256SUMS.txt" % verify(a.raw_dir))


# ----------------------------------------------------------------------- analyze


def mismatches(con, order):
    """Revision mismatches under one ordering, with the study's classes and the own parent."""
    w = reconstruct.window(order)
    con.execute("CREATE OR REPLACE TEMP TABLE pos AS SELECT wiki_id, page__page_id, revision__rev_id, "
                "row_number() %s AS position FROM events" % w)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE mm AS
        WITH m AS (SELECT wiki_id, page__page_id, revision__rev_id AS rev, page_change_kind AS kind,
                          revision__rev_parent_id AS parent, prior_state__revision__rev_id AS prior,
                          prior_state__revision__rev_dt::TIMESTAMPTZ AS prior_dt,
                          LAG(revision__rev_id) %(w)s AS lag, row_number() %(w)s AS position FROM events)
        SELECT m.*,
          (SELECT max(p.position) FROM pos p WHERE p.wiki_id = m.wiki_id AND p.page__page_id = m.page__page_id
             AND p.revision__rev_id = m.prior) AS this_page,
          EXISTS (SELECT 1 FROM revs x WHERE x.wiki_id = m.wiki_id AND x.page__page_id <> m.page__page_id
             AND x.revision__rev_id = m.prior) AS other_page
        FROM m WHERE prior IS NOT NULL AND lag IS NOT NULL AND prior <> lag""" % {"w": w})
    con.execute("""
        CREATE OR REPLACE TEMP TABLE mm AS SELECT *,
          CASE WHEN other_page THEN 'merge or re-keying: prior revision captured under another page_id'
               WHEN this_page IS NOT NULL AND this_page > position THEN 'ordering: prior revision ordered after this event'
               WHEN this_page IS NOT NULL THEN 'ordering: prior revision ordered before the LAG row'
               WHEN prior > lag THEN 'missed event: prior revision newer than the LAG revision, not captured'
               ELSE 'missed event: prior revision older than the LAG revision, not captured' END AS class
        FROM mm""")


def analyze(a):
    import duckdb
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    t0 = time.perf_counter()

    def log(msg):
        print("[%7.1fs] %s" % (time.perf_counter() - t0, msg), flush=True)

    verified = verify(a.raw_dir)
    captures = sorted(os.path.join(a.raw_dir, d) for d in os.listdir(a.raw_dir)
                      if os.path.exists(os.path.join(a.raw_dir, d, "manifest.json")))
    manifests = load.read_manifests(captures)
    os.makedirs(a.work, exist_ok=True)
    parquet = os.path.join(a.work, "events.parquet")
    con = duckdb.connect()
    if not os.path.exists(parquet):
        load.load(con, manifests, schema.load_avro(AVSC), parquet, a.work)
    rows = load.check_loaded(con, parquet, manifests)
    log("%d files verified, %d events loaded" % (verified, rows))
    con.execute("CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('%s')" % parquet)
    con.execute("CREATE OR REPLACE TEMP TABLE revs AS SELECT DISTINCT wiki_id, page__page_id, revision__rev_id FROM events")

    committed = {r["ordering"]: r for r in csv.DictReader(open(os.path.join(a.out, "ordering_sensitivity.csv")))}
    check_rows, split_rows, misorder_rows, stale_rows = [], [], [], []
    for label, key in ORDERINGS:
        order = key + tuple(c for c in load.ORDER_BY if c not in key)
        mismatches(con, order)
        compared, differ = con.execute(
            "SELECT count(*) FILTER (WHERE prior IS NOT NULL AND lag IS NOT NULL), "
            "count(*) FILTER (WHERE prior <> lag) FROM (SELECT prior_state__revision__rev_id AS prior, "
            "LAG(revision__rev_id) %s AS lag FROM events)" % reconstruct.window(order)).fetchone()
        classes = dict(con.execute("SELECT class, count(*) FROM mm GROUP BY 1").fetchall())
        want = committed[label]
        class_columns = [c for c in want if ": " in c]
        if set(classes) - set(class_columns):
            raise RuntimeError("%s: classes %s are not columns of ordering_sensitivity.csv" % (label, set(classes) - set(class_columns)))
        for c in class_columns:
            if int(want[c] or 0) != classes.get(c, 0):
                raise RuntimeError("%s, %s: %d here, %s in ordering_sensitivity.csv" % (label, c, classes.get(c, 0), want[c]))
        if (compared, differ) != (int(want["compared"]), int(want["differ"])):
            raise RuntimeError("%s: compared %d, differ %d do not equal ordering_sensitivity.csv" % (label, compared, differ))
        n_ord, n_mis, n_lag, n_neither = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE parent = prior), count(*) FILTER (WHERE parent = lag), "
            "count(*) FILTER (WHERE parent IS DISTINCT FROM prior AND parent IS DISTINCT FROM lag) "
            "FROM mm WHERE class LIKE 'ordering:%'").fetchone()
        check_rows.append({"ordering": label, "order_by": ", ".join(order), "compared": compared, "mismatched": differ,
                           "ordering_class": n_ord, "misorder_own_parent_equals_prior_state": n_mis,
                           "not_ordering_own_parent_equals_lag": n_lag, "neither": n_neither})
        log("%s: ordering class %d, misorder %d, own parent equals LAG %d" % (label, n_ord, n_mis, n_lag))
        if label != PRIMARY:
            continue

        # The primary key's mismatches, split three ways.
        con.execute("""CREATE OR REPLACE TEMP TABLE prim AS SELECT *,
            CASE WHEN parent = lag THEN 'stale prior_state: own parent equals LAG'
                 WHEN class LIKE 'ordering:%' AND parent = prior THEN 'misorder: own parent equals prior_state'
                 WHEN class LIKE 'missed event:%' THEN 'not captured: no captured event carries the embedded revision'
                 ELSE 'other' END AS cause FROM mm""")
        first_dt = con.execute("SELECT min(meta__dt) FROM events").fetchone()[0]
        for cause, cls, n, before in con.execute(
                "SELECT cause, class, count(*), count(*) FILTER (WHERE prior_dt < ?::TIMESTAMPTZ) FROM prim "
                "GROUP BY 1, 2 ORDER BY 3 DESC", [first_dt]).fetchall():
            split_rows.append({"cause": cause, "committed_class": cls, "events": n,
                               "embedded_revision_dated_before_first_captured_event": before})
        if sum(r["events"] for r in split_rows) != differ or any(r["cause"] == "other" for r in split_rows):
            raise RuntimeError("the primary split does not cover every mismatch: %s" % split_rows)

        # Each misorder: the LAG row and the row that carries the embedded revision.
        w_recv = reconstruct.window(("meta__dt",) + tuple(c for c in load.ORDER_BY if c != "meta__dt"))
        for r in con.execute("""
            WITH p AS (SELECT * FROM prim WHERE cause LIKE 'misorder:%%'),
                 pagerows AS (SELECT e.*, row_number() %s AS position, LAG(revision__rev_id) %s AS lag_received
                              FROM events e SEMI JOIN p ON p.wiki_id = e.wiki_id AND p.page__page_id = e.page__page_id)
            SELECT p.wiki_id || ':' || p.page__page_id || ':' || p.rev, me.dt, l.page_change_kind, l.dt, l.meta__dt,
                   l.revision__rev_id, c.page_change_kind, c.dt, c.meta__dt, c.revision__rev_id, me.lag_received = p.prior
            FROM p JOIN pagerows me ON me.wiki_id = p.wiki_id AND me.page__page_id = p.page__page_id AND me.position = p.position
                   JOIN pagerows l ON l.wiki_id = p.wiki_id AND l.page__page_id = p.page__page_id AND l.position = p.position - 1
                   JOIN pagerows c ON c.wiki_id = p.wiki_id AND c.page__page_id = p.page__page_id AND c.position = p.this_page
            ORDER BY 1""" % (reconstruct.window(order), w_recv)).fetchall():
            (event, dt, lk, ldt, lmdt, lrev, ck, cdt, cmdt, crev, recv_ok) = r
            misorder_rows.append({"event": event, "event_dt": dt, "lag_row_kind": lk, "lag_row_dt": ldt,
                                  "lag_row_rev_id": lrev, "prior_row_kind": ck, "prior_row_dt": cdt, "prior_row_rev_id": crev,
                                  "same_dt": yes(ldt == cdt), "prior_row_rev_id_below_lag_row_rev_id": yes(crev < lrev),
                                  "prior_row_received_after_lag_row": yes(cmdt > lmdt),
                                  "lag_by_receive_time_equals_prior_state": yes(recv_ok)})

        # Each stale prior_state event: is the embedded revision the own parent's parent?
        for r in con.execute("""
            WITH s AS (SELECT * FROM prim WHERE cause LIKE 'stale%%'),
                 pagerows AS (SELECT e.*, row_number() %s AS position
                              FROM events e SEMI JOIN s ON s.wiki_id = e.wiki_id AND s.page__page_id = e.page__page_id)
            SELECT s.wiki_id, s.page__page_id, s.rev, s.kind, s.class, s.parent, s.prior, l.revision__rev_parent_id,
                   l.page_change_kind, epoch(me.dt::TIMESTAMPTZ) - epoch(l.dt::TIMESTAMPTZ),
                   me.page__page_title = l.page__page_title AND me.prior_state__page__page_title = l.prior_state__page__page_title,
                   me.meta__domain
            FROM s JOIN pagerows me ON me.wiki_id = s.wiki_id AND me.page__page_id = s.page__page_id AND me.position = s.position
                   JOIN pagerows l ON l.wiki_id = s.wiki_id AND l.page__page_id = s.page__page_id AND l.position = s.position - 1
            ORDER BY 1, 2, 3""" % reconstruct.window(order)).fetchall():
            (wiki, page, rev, kind, cls, parent, prior, parent_of_parent, lag_kind, seconds, same_titles, domain) = r
            stale_rows.append({"event": "%s:%d:%d" % (wiki, page, rev), "kind": kind, "committed_class": cls,
                               "own_parent": parent, "embedded_revision": prior, "own_parent_rev_parent_id": parent_of_parent,
                               "embedded_is_one_revision_before_own_parent": yes(parent_of_parent == prior),
                               "lag_row_kind": lag_kind, "seconds_after_lag_row": int(seconds),
                               "same_titles_as_lag_row": yes(same_titles), "domain": domain})
    con.close()

    env = {"started_utc": started, "archive_prefix": PREFIX, "files_verified_against_sha256sums": verified,
           "captures": [os.path.basename(c) for c in captures], "events": rows, "duckdb": duckdb.__version__,
           "first_captured_meta_dt": first_dt,
           "git_commit": subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, stdout=subprocess.PIPE,
                                        text=True).stdout.strip()}
    if a.api:
        check_api(stale_rows, env, log)
    for r in stale_rows:
        r.setdefault("api_parent_of_own_parent", "")
        r.setdefault("api_parent_of_event", "")
        r.pop("domain")
    env["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    write_csv(os.path.join(a.out, FILES[0]), check_rows, list(check_rows[0]))
    write_csv(os.path.join(a.out, FILES[1]), split_rows, list(split_rows[0]))
    write_csv(os.path.join(a.out, FILES[2]), misorder_rows, list(misorder_rows[0]))
    write_csv(os.path.join(a.out, FILES[3]), stale_rows, list(stale_rows[0]))
    with open(os.path.join(a.out, FILES[4]), "w") as fh:
        json.dump(env, fh, indent=2)
    for name, rs in zip(FILES, (check_rows, split_rows, misorder_rows, stale_rows)):
        print(name)
        for r in rs:
            print("  ", r)


def check_api(stale_rows, env, log):
    """The parent of each stale event's own revision and of its own parent, from the wiki."""
    import api_checks
    api = api_checks.Api(pause=1.0, log=log)
    by_domain = {}
    for r in stale_rows:
        by_domain.setdefault(r["domain"], []).append(r)
    for domain, rs in sorted(by_domain.items()):
        revids = sorted({int(r["event"].rsplit(":", 1)[1]) for r in rs} | {r["own_parent"] for r in rs})
        body = api.get(domain, action="query", prop="revisions", revids="|".join(map(str, revids)), rvprop="ids")
        parents = {rv["revid"]: rv.get("parentid") for p in body.get("query", {}).get("pages", [])
                   for rv in p.get("revisions", [])}
        for r in rs:
            rev = int(r["event"].rsplit(":", 1)[1])
            r["api_parent_of_event"] = parents.get(rev, "not returned")
            r["api_parent_of_own_parent"] = parents.get(r["own_parent"], "not returned")
    env["api_checked_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    env["api_requests"] = api.requests


# ----------------------------------------------------------------------- summary


def md(rows, headers):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    return "\n".join(out + ["| " + " | ".join(str(c) for c in r) + " |" for r in rows])


def summary(a):
    read = lambda name: list(csv.DictReader(open(os.path.join(a.out, name))))  # noqa: E731
    checks, split, misorders, stale = (read(f) for f in FILES[:4])
    env = json.load(open(os.path.join(a.out, FILES[4])))
    fmt = lambda v: "{:,}".format(int(v))  # noqa: E731
    total = sum(int(r["events"]) for r in split)
    by_cause = {}
    for r in split:
        by_cause[r["cause"].split(":")[0]] = by_cause.get(r["cause"].split(":")[0], 0) + int(r["events"])
    lines = [HEADING, ""]
    w = lines.append
    w("Measured by `study/wikimedia/ordering_check.py` on %s from the archived capture (`%s`, %d files that match its "
      "SHA256SUMS.txt), loaded again with `load.py` (%s events, DuckDB %s). Under each ordering below, the compared "
      "events, the revision mismatches and every class count equal `ordering_sensitivity.csv`; the script stops "
      "otherwise." % (env["started_utc"][:10], env["archive_prefix"], env["files_verified_against_sha256sums"],
                       fmt(env["events"]), env["duckdb"]))
    w("")
    w("The \"ordering\" class is positional: the embedded previous revision (prior_state.revision.rev_id) was captured "
      "for the same page, in a row other than the LAG row. Each event's own revision also names its parent "
      "(revision.rev_parent_id), as MediaWiki recorded it. A misorder is an ordering-class event whose own parent equals "
      "prior_state.revision.rev_id: the revision history agrees with prior_state, and LAG returned a row the ordering put "
      "in the wrong place. Where the own parent equals LAG(revision.rev_id) instead, the revision history agrees with "
      "LAG, and no ordering is at fault.")
    w("")
    w(md([[r["ordering"], fmt(r["compared"]), fmt(r["mismatched"]), fmt(r["ordering_class"]),
           fmt(r["misorder_own_parent_equals_prior_state"]), fmt(r["not_ordering_own_parent_equals_lag"]),
           fmt(r["neither"])] for r in checks],
         ["Ordering", "Compared", "Mismatched", "Ordering class", "Misorder: own parent = prior_state",
          "Not ordering: own parent = LAG", "Neither"]))
    w("")
    early = sum(int(r["embedded_revision_dated_before_first_captured_event"]) for r in split
                if r["cause"].startswith("not captured"))
    w("Under the primary key (event time, then revision ID), the %s revision mismatches split into %s revisions that "
      "never reached the stream (%s of them dated before the capture began), %s events whose prior_state points one "
      "revision too far back, and %s misorders:" % (
          fmt(total), fmt(by_cause.get("not captured", 0)), fmt(early), fmt(by_cause.get("stale prior_state", 0)),
          fmt(by_cause.get("misorder", 0))))
    w("")
    w(md([[r["cause"], r["committed_class"], fmt(r["events"]), fmt(r["embedded_revision_dated_before_first_captured_event"])]
          for r in split],
         ["Cause", "Class in revision_mismatches.csv", "Events", "Embedded revision dated before the first captured event"]))
    w("")
    w("\"Never reached the stream\": no captured event carries the embedded revision, and the capture has no offset gaps. "
      "The last column counts embedded revisions whose rev_dt is earlier than the first captured event (%s): an event "
      "for those could have been emitted before the capture began." % env["first_captured_meta_dt"])
    w("")
    tie = all(r["same_dt"] == "yes" and r["lag_row_kind"] == "delete" and r["prior_row_kind"] == "undelete"
              and r["prior_row_rev_id_below_lag_row_rev_id"] == "yes" and r["prior_row_received_after_lag_row"] == "yes"
              and r["lag_by_receive_time_equals_prior_state"] == "yes" for r in misorders)
    if misorders and tie:
        w("Misorders under the primary key. Each page was deleted and restored within one second. The delete (the LAG "
          "row) and the undelete (the row with the embedded revision) share one whole-second `dt`, so the revision ID "
          "breaks the tie. An undelete carries the restored revision's ID, older than the deleted revision's, so it sorts "
          "first although it was received second. The event's own parent is the restored revision, and ordering by "
          "receive time returns it.")
    else:
        w("Misorders under the primary key, with the LAG row and the row that carries the embedded revision:")
    w("")
    w(md([[r["event"], r["event_dt"], "%s %s, rev %s" % (r["lag_row_kind"], r["lag_row_dt"], r["lag_row_rev_id"]),
           "%s %s, rev %s" % (r["prior_row_kind"], r["prior_row_dt"], r["prior_row_rev_id"]), r["same_dt"],
           r["prior_row_rev_id_below_lag_row_rev_id"], r["prior_row_received_after_lag_row"],
           r["lag_by_receive_time_equals_prior_state"]] for r in misorders],
         ["Event (wiki_id:page_id:rev_id)", "Event dt", "LAG row", "Row with the embedded revision", "Same dt",
          "Its rev_id below the LAG row's", "Received after the LAG row", "Receive-time LAG equals prior_state"]))
    w("")
    moves = all(r["kind"] == "move" and r["lag_row_kind"] == "move" and r["same_titles_as_lag_row"] == "yes" for r in stale)
    one_back = [r["event"] for r in stale if r["embedded_is_one_revision_before_own_parent"] != "yes"
                or str(r["api_parent_of_own_parent"]) not in (r["embedded_revision"], "")
                or str(r["api_parent_of_event"]) not in (r["own_parent"], "")]
    w("Stale prior_state under the primary key: the event's own parent equals LAG(revision.rev_id), and prior_state "
      "names another revision. %s%s \"Archive\" reads the own parent's parent from its revision.rev_parent_id in the "
      "capture; \"wiki\" reads both parents from the public MediaWiki Action API (%s, %s requests)." % (
          "Each is a move event that follows another move event of the same page, with the same title before and after. "
          if stale and moves else "",
          "In every one, the embedded revision is the parent of the event's own parent: exactly one revision before it, "
          "in the archive%s." % (" and in the wiki" if all(r["api_parent_of_own_parent"] for r in stale) else "")
          if stale and not one_back else
          "Exceptions to \"the embedded revision is one revision before the own parent\": %s." % ", ".join(one_back),
          env.get("api_checked_utc", "not checked"), env.get("api_requests", 0)))
    w("")
    w(md([[r["event"], r["committed_class"], r["own_parent"], r["embedded_revision"], r["own_parent_rev_parent_id"],
           r["api_parent_of_own_parent"] or "not checked", r["api_parent_of_event"] or "not checked",
           r["embedded_is_one_revision_before_own_parent"], r["lag_row_kind"], r["seconds_after_lag_row"],
           r["same_titles_as_lag_row"]] for r in stale],
         ["Event (wiki_id:page_id:rev_id)", "Class in revision_mismatches.csv", "Own parent (= LAG)", "Embedded revision",
          "Own parent's parent: archive", "Own parent's parent: wiki", "Event's parent: wiki",
          "Embedded is one revision before own parent", "LAG row", "Seconds after it", "Same titles"]))
    w("")
    w("Files: `ordering_parent_check.csv`, `ordering_parent_split.csv`, `ordering_misorders.csv`, "
      "`ordering_stale_prior.csv`, `ordering_parent_check_environment.json`.")
    w("")
    replace_section(a.summary, HEADING, "\n".join(lines))
    print("\n".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["download", "analyze", "summary"])
    ap.add_argument("--archive", help="s3://<bucket>/%s" % PREFIX)
    ap.add_argument("--profile", help="AWS CLI profile for the download")
    ap.add_argument("--raw-dir", help="the archive's copy, outside the repository")
    ap.add_argument("--work", help="the Parquet file's directory, outside the repository")
    ap.add_argument("--out", default="results/wikimedia")
    ap.add_argument("--summary", default="results/SUMMARY.md")
    ap.add_argument("--api", action="store_true", help="also read the stale events' parents from the MediaWiki API")
    a = ap.parse_args()
    {"download": download, "analyze": analyze, "summary": summary}[a.step](a)


if __name__ == "__main__":
    main()
