"""Step 4: reproduce prior_state with LAG, field by field, and classify every difference.

The reconstruction is the article's query form (sql/lag_history.sql): for every field
that prior_state can carry, LAG of the matching current-state column over the entity
(wiki_id, page__page_id), ordered by load.ORDER_BY. reconstruction_sql() writes that
query out; evaluate() runs it per hash bucket of the entity key, which only splits the
work, since every window partition falls inside one bucket.

An event is compared when its predecessor (the previous captured event of the same
entity) exists and no offset gap of either topic falls between the two events' meta.dt
values (in either order). For each field, the outcome is one of:

  match            prior_state has the field and LAG returns the same value
  differ           both present, values differ
  prior_only       prior_state has the field, LAG returns null
  omitted_same     prior_state omits the field and LAG equals the current value
                   (consistent with "only present if changed")
  omitted_changed  prior_state omits the field but LAG differs from the current value
  both_absent      neither has a value

differ, prior_only and omitted_changed are mismatches. Each (event, field) mismatch gets
one cause, by the first rule that applies, and a detail naming what was established:

  1. prior_state.revision.rev_id is present and differs from LAG(revision.rev_id):
     merge or re-keying  the prior revision was captured under another page_id
     ordering            the prior revision was captured for this page, in another row;
                         the detail says whether that row is ordered before or after
     missed event        no captured event carries the prior revision; the detail says
                         whether it is newer or older than the LAG revision
  2. other, editor record: a revision.editor field, and both sides describe the same
     revision (prior_state.revision.rev_id equals LAG(rev_id), or, where prior_state
     omits the field, the event's own rev_id equals LAG(rev_id)), so the difference is
     a change to the user's record between the two events
  3. delete and undelete  this event or its predecessor is a delete or undelete
  4. visibility change    this event or its predecessor is a visibility_change
  5. page move            this event or its predecessor is a move
  6. other                anything else; the detail names the outcome
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import load  # noqa: E402

PRIOR = "prior_state__"
OUTCOMES = ("match", "differ", "prior_only", "omitted_same", "omitted_changed", "both_absent")
MISMATCH = ("differ", "prior_only", "omitted_changed")


def prior_pairs(columns):
    """(prior_state column, current-state column) for every prior_state leaf."""
    names = set(columns)
    out = []
    for c in columns:
        if c.startswith(PRIOR):
            cur = c[len(PRIOR):]
            if cur not in names:
                raise RuntimeError("no current-state column for %s" % c)
            out.append((c, cur))
    return out


def window(order_by=load.ORDER_BY):
    return "OVER (PARTITION BY %s ORDER BY %s)" % (", ".join(load.ENTITY), ", ".join(order_by))


def reconstruction_sql(pairs, source="events", where=""):
    """The article's LAG form: one LAG per field, same window, no previous-value columns read."""
    w = window()
    lines = ["SELECT", "  " + ",\n  ".join(load.ENTITY + load.ORDER_BY) + ",",
             "  page_change_kind,",
             "  row_number() %s AS position," % w,
             "  LAG(meta__topic) %s AS previous_meta__topic," % w,
             "  LAG(meta__offset) %s AS previous_meta__offset," % w,
             "  LAG(meta__dt) %s AS previous_meta__dt," % w,
             "  LAG(page_change_kind) %s AS previous_page_change_kind," % w]
    body = []
    for prior, cur in pairs:
        body.append('  "%s",\n  "%s",\n  LAG("%s") %s AS "previous_%s"' % (prior, cur, cur, w, cur))
    lines.append(",\n".join(body))
    lines.append("FROM %s%s" % (source, (" WHERE " + where) if where else ""))
    return "\n".join(lines)


def outcome_sql(prior, cur):
    p, c, l = '"%s"' % prior, '"%s"' % cur, '"previous_%s"' % cur
    return ("CASE WHEN {p} IS NOT NULL AND {l} IS NOT NULL THEN CASE WHEN {p} = {l} THEN 'match' ELSE 'differ' END "
            "WHEN {p} IS NOT NULL THEN 'prior_only' "
            "WHEN {l} IS NOT NULL THEN CASE WHEN {l} IS NOT DISTINCT FROM {c} THEN 'omitted_same' ELSE 'omitted_changed' END "
            "ELSE 'both_absent' END").format(p=p, c=c, l=l)


def direction_sql(prior, cur, dtype):
    """'prior_greater' or 'prior_less' for ordered scalar types when both values differ."""
    if dtype.startswith(("BIGINT", "VARCHAR", "INTEGER", "DOUBLE")):
        p, l = '"%s"' % prior, '"previous_%s"' % cur
        return "CASE WHEN {p} > {l} THEN 'prior_greater' WHEN {p} < {l} THEN 'prior_less' END".format(p=p, l=l)
    return "NULL"


def field_group(cur):
    parts = cur.split("__")
    if parts[0] == "page":
        return "page"
    if parts[:2] == ["revision", "editor"]:
        return "revision.editor"
    if parts[:2] == ["revision", "content_slots"]:
        return "revision.content_slots"
    return "revision"


def cause_sql(prior, cur):
    """(cause, detail) SQL expressions for one field's mismatch."""
    p = '"%s"' % prior
    lineage = ("prior_state__revision__rev_id IS NOT NULL AND "
               "prior_state__revision__rev_id IS DISTINCT FROM previous_revision__rev_id")
    same_revision = ("CASE WHEN {p} IS NOT NULL THEN prior_state__revision__rev_id = previous_revision__rev_id "
                     "ELSE revision__rev_id = previous_revision__rev_id END").format(p=p)
    editor = "true" if field_group(cur) == "revision.editor" else "false"
    cause = """CASE
  WHEN {lineage} THEN CASE WHEN prior_rev_on_other_page THEN 'merge or re-keying'
                           WHEN prior_rev_on_this_page THEN 'ordering' ELSE 'missed event' END
  WHEN {editor} AND {same} THEN 'other'
  WHEN page_change_kind IN ('delete', 'undelete') OR previous_page_change_kind IN ('delete', 'undelete') THEN 'delete and undelete'
  WHEN page_change_kind = 'visibility_change' OR previous_page_change_kind = 'visibility_change' THEN 'visibility change'
  WHEN page_change_kind = 'move' OR previous_page_change_kind = 'move' THEN 'page move'
  ELSE 'other' END""".format(lineage=lineage, editor=editor, same=same_revision)
    detail = """CASE
  WHEN {lineage} THEN CASE
    WHEN prior_rev_on_other_page THEN 'prior revision captured under another page_id'
    WHEN prior_rev_on_this_page THEN CASE WHEN prior_rev_ordered_after THEN 'prior revision ordered after this event'
                                          ELSE 'prior revision ordered before the LAG row' END
    WHEN prior_state__revision__rev_id > previous_revision__rev_id THEN 'prior revision newer than the LAG revision, not captured'
    ELSE 'prior revision older than the LAG revision, not captured' END
  WHEN {editor} AND {same} THEN 'editor record changed between the two events'
  WHEN page_change_kind IN ('delete', 'undelete', 'visibility_change', 'move') THEN 'this event is ' || CASE page_change_kind WHEN 'undelete' THEN 'an ' ELSE 'a ' END || page_change_kind
  WHEN previous_page_change_kind IN ('delete', 'undelete', 'visibility_change', 'move') THEN 'previous event is ' || CASE previous_page_change_kind WHEN 'undelete' THEN 'an ' ELSE 'a ' END || previous_page_change_kind
  ELSE 'no event kind or lineage explains it' END""".format(lineage=lineage, editor=editor, same=same_revision)
    return cause, detail


def evaluate(con, parquet, gaps, buckets, log=print):
    """Per-field outcome counts, mismatch classes per field and per event, event counts."""
    con.execute("CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('%s')" % parquet)
    types = dict(con.execute("SELECT column_name, column_type FROM (DESCRIBE events)").fetchall())
    pairs = prior_pairs(list(types))
    con.execute("CREATE OR REPLACE TEMP TABLE gaps (topic VARCHAR, after_dt VARCHAR, next_dt VARCHAR)")
    for g in gaps:
        con.execute("INSERT INTO gaps VALUES (?, ?, ?)", [g["topic"], g["after_dt"], g["next_dt"]])
    con.execute("CREATE OR REPLACE TEMP TABLE revs AS SELECT wiki_id, page__page_id, revision__rev_id, "
                "row_number() %s AS position FROM events" % window())
    field_counts = {cur: dict.fromkeys(OUTCOMES, 0) for _, cur in pairs}
    classes, event_classes = {}, {}
    events = {"events": 0, "with_predecessor": 0, "eligible": 0, "excluded_no_predecessor": 0,
              "excluded_loss_possible": 0, "eligible_without_mismatch": 0}
    ent = ", ".join(load.ENTITY)
    outcome_cols = ", ".join("%s AS o_%d, %s AS d_%d, %s AS c_%d, %s AS t_%d" % (
        (outcome_sql(p, c), i, direction_sql(p, c, types[c]), i) + (cause_sql(p, c)[0], i, cause_sql(p, c)[1], i))
        for i, (p, c) in enumerate(pairs))
    groups_expr = "coalesce(array_to_string(list_sort(list_distinct(list_filter([%s], x -> x IS NOT NULL))), ', '), '')" % ", ".join(
        "CASE WHEN o_%d IN %s THEN '%s' END" % (i, str(MISMATCH), field_group(c)) for i, (_, c) in enumerate(pairs))
    causes_expr = "coalesce(array_to_string(list_sort(list_distinct(list_filter([%s], x -> x IS NOT NULL))), ', '), '')" % ", ".join(
        "CASE WHEN o_%d IN %s THEN c_%d END" % (i, str(MISMATCH), i) for i in range(len(pairs)))
    example = ("list([wiki_id, CAST(page__page_id AS VARCHAR), CAST(revision__rev_id AS VARCHAR)] "
               "ORDER BY wiki_id, page__page_id, revision__rev_id)[1:5]")
    for b in range(buckets):
        where = "hash(%s) %% %d = %d" % (ent, buckets, b) if buckets > 1 else ""
        con.execute("CREATE OR REPLACE TEMP TABLE r AS SELECT *, "
                    "previous_meta__offset IS NOT NULL AS has_predecessor, "
                    "previous_meta__offset IS NOT NULL AND NOT EXISTS (SELECT 1 FROM gaps g "
                    "WHERE g.after_dt <= greatest(q.meta__dt, q.previous_meta__dt) "
                    "AND g.next_dt >= least(q.meta__dt, q.previous_meta__dt)) AS eligible, "
                    "EXISTS (SELECT 1 FROM revs v WHERE v.wiki_id = q.wiki_id AND v.page__page_id = q.page__page_id "
                    "AND v.revision__rev_id = q.prior_state__revision__rev_id) AS prior_rev_on_this_page, "
                    "EXISTS (SELECT 1 FROM revs v WHERE v.wiki_id = q.wiki_id AND v.page__page_id <> q.page__page_id "
                    "AND v.revision__rev_id = q.prior_state__revision__rev_id) AS prior_rev_on_other_page, "
                    "EXISTS (SELECT 1 FROM revs v WHERE v.wiki_id = q.wiki_id AND v.page__page_id = q.page__page_id "
                    "AND v.revision__rev_id = q.prior_state__revision__rev_id AND v.position > q.position) "
                    "AS prior_rev_ordered_after "
                    "FROM (%s) q" % reconstruction_sql(pairs, where=where))
        n, pred, elig = con.execute("SELECT count(*), count(*) FILTER (WHERE has_predecessor), "
                                    "count(*) FILTER (WHERE eligible) FROM r").fetchone()
        events["events"] += n
        events["with_predecessor"] += pred
        events["eligible"] += elig
        events["excluded_no_predecessor"] += n - pred
        events["excluded_loss_possible"] += pred - elig
        con.execute("CREATE OR REPLACE TEMP TABLE o AS SELECT wiki_id, page__page_id, revision__rev_id, %s "
                    "FROM r WHERE eligible" % outcome_cols)
        con.execute("DROP TABLE r")
        for i, (prior, cur) in enumerate(pairs):
            for outcome, cnt in con.execute("SELECT o_%d, count(*) FROM o GROUP BY 1" % i).fetchall():
                field_counts[cur][outcome] += cnt
            for outcome, cause, detail, direction, cnt, examples in con.execute(
                    "SELECT o_%d, c_%d, t_%d, d_%d, count(*), %s FROM o WHERE o_%d IN %s GROUP BY ALL"
                    % (i, i, i, i, example, i, str(MISMATCH))).fetchall():
                key = (cause, detail, field_group(cur), cur, outcome)
                c = classes.setdefault(key, {"count": 0, "prior_greater": 0, "prior_less": 0, "examples": []})
                c["count"] += cnt
                if direction:
                    c[direction] += cnt
                c["examples"] = sorted(c["examples"] + [tuple(x) for x in examples])[:5]
        for cause, groups, cnt, examples in con.execute(
                "SELECT cs, g, count(*), %s FROM (SELECT *, %s AS g, %s AS cs FROM o) GROUP BY ALL"
                % (example, groups_expr, causes_expr)).fetchall():
            if groups == "":
                events["eligible_without_mismatch"] += cnt
                continue
            c = event_classes.setdefault((cause, groups), {"events": 0, "examples": []})
            c["events"] += cnt
            c["examples"] = sorted(c["examples"] + [tuple(x) for x in examples])[:5]
        con.execute("DROP TABLE o")
        log("  reconstruction bucket %d of %d: %d events" % (b + 1, buckets, n))
    return pairs, events, field_counts, classes, event_classes


def lineage(con, buckets):
    """Event-level comparison of prior_state.revision.rev_id with LAG(revision.rev_id)."""
    w = window()
    rows = con.execute(
        "SELECT page_change_kind, previous_kind, cross_topic, "
        "count(*) FILTER (WHERE prior_rev IS NOT NULL AND prior_rev = lag_rev), "
        "count(*) FILTER (WHERE prior_rev IS NOT NULL AND lag_rev IS NOT NULL AND prior_rev <> lag_rev), "
        "count(*) FILTER (WHERE prior_rev IS NOT NULL AND lag_rev IS NULL), "
        "count(*) FILTER (WHERE prior_rev IS NULL) "
        "FROM (SELECT page_change_kind, prior_state__revision__rev_id AS prior_rev, "
        "LAG(revision__rev_id) %s AS lag_rev, LAG(page_change_kind) %s AS previous_kind, "
        "LAG(meta__topic) %s <> meta__topic AS cross_topic FROM events) GROUP BY ALL ORDER BY ALL" % (w, w, w)).fetchall()
    return [{"page_change_kind": k, "previous_kind": pk, "cross_topic": ct, "prior_rev_equals_lag": a,
             "prior_rev_differs": d, "no_predecessor": np_, "no_prior_rev": nr} for k, pk, ct, a, d, np_, nr in rows]


def parent_check(con):
    """prior_state.revision.rev_id against revision.rev_parent_id, per page_change_kind."""
    rows = con.execute(
        "SELECT page_change_kind, count(*), "
        "count(*) FILTER (WHERE prior_state__revision__rev_id IS NOT NULL AND revision__rev_parent_id IS NOT NULL "
        "AND prior_state__revision__rev_id = revision__rev_parent_id), "
        "count(*) FILTER (WHERE prior_state__revision__rev_id IS NOT NULL AND revision__rev_parent_id IS NOT NULL "
        "AND prior_state__revision__rev_id <> revision__rev_parent_id), "
        "count(*) FILTER (WHERE prior_state__revision__rev_id IS NOT NULL AND revision__rev_parent_id IS NULL), "
        "count(*) FILTER (WHERE prior_state__revision__rev_id IS NULL AND revision__rev_parent_id IS NOT NULL), "
        "count(*) FILTER (WHERE prior_state__revision__rev_id IS NULL AND revision__rev_parent_id IS NULL) "
        "FROM events GROUP BY 1 ORDER BY 1").fetchall()
    return [{"page_change_kind": k, "events": n, "equal": eq, "differ": ne, "prior_only": po, "parent_only": pa,
             "neither": nn} for k, n, eq, ne, po, pa, nn in rows]


def topics(con):
    """Pages with events in both datacenter topics, and how each ordering candidate
    reproduces prior_state.revision.rev_id, overall and across topics."""
    both = con.execute("SELECT count(*) FROM (SELECT %s FROM events GROUP BY ALL HAVING count(DISTINCT meta__topic) > 1)"
                       % ", ".join(load.ENTITY)).fetchone()[0]
    con.execute("CREATE OR REPLACE TEMP TABLE both_pages AS SELECT %s FROM events GROUP BY ALL "
                "HAVING count(DISTINCT meta__topic) > 1" % ", ".join(load.ENTITY))
    out = []
    for label, key in load.ORDERINGS:
        order = key + tuple(c for c in load.ORDER_BY if c not in key)
        w = window(order)
        r = con.execute(
            "SELECT count(*) FILTER (WHERE prior_rev IS NOT NULL AND lag_rev IS NOT NULL), "
            "count(*) FILTER (WHERE prior_rev = lag_rev), "
            "count(*) FILTER (WHERE in_both AND prior_rev IS NOT NULL AND lag_rev IS NOT NULL), "
            "count(*) FILTER (WHERE in_both AND prior_rev = lag_rev), "
            "count(*) FILTER (WHERE cross_topic AND prior_rev IS NOT NULL), "
            "count(*) FILTER (WHERE cross_topic AND prior_rev = lag_rev), "
            "count(*) FILTER (WHERE cross_topic) "
            "FROM (SELECT prior_state__revision__rev_id AS prior_rev, LAG(revision__rev_id) %s AS lag_rev, "
            "LAG(meta__topic) %s <> meta__topic AS cross_topic, "
            "(wiki_id, page__page_id) IN (SELECT (wiki_id, page__page_id) FROM both_pages) AS in_both FROM events)"
            % (w, w)).fetchone()
        out.append({"ordering": label, "order_by": ", ".join(order), "compared": r[0], "rev_id_equal": r[1],
                    "compared_pages_in_both_topics": r[2], "rev_id_equal_pages_in_both_topics": r[3],
                    "cross_topic_pairs": r[6], "cross_topic_compared": r[4], "cross_topic_rev_id_equal": r[5]})
    return both, out


SENSITIVITY = [
    ("receive time: meta.dt", ("meta__dt",)),
    ("event time first: dt, rev_id (primary)", ("dt", "revision__rev_id")),
    ("sequence first: rev_id, dt", ("revision__rev_id", "dt")),
]


def revision_mismatches(con, key, gaps=()):
    """Events whose prior_state.revision.rev_id differs from LAG(revision.rev_id) under
    an ordering that starts with key (completed with the rest of load.ORDER_BY), by
    cause: merge or re-keying, ordering (before the LAG row or after the event under
    this ordering), missed event (newer or older than the LAG revision). Events whose
    interval since their predecessor overlaps an offset gap are left out, as in evaluate()."""
    order = key + tuple(c for c in load.ORDER_BY if c not in key)
    w = window(order)
    con.execute("CREATE OR REPLACE TEMP TABLE rm_gaps (after_dt VARCHAR, next_dt VARCHAR)")
    for g in gaps:
        con.execute("INSERT INTO rm_gaps VALUES (?, ?)", [g["after_dt"], g["next_dt"]])
    loss_free = ("NOT EXISTS (SELECT 1 FROM rm_gaps g WHERE g.after_dt <= greatest(meta__dt, lag_dt) "
                 "AND g.next_dt >= least(meta__dt, lag_dt))")
    con.execute("CREATE OR REPLACE TEMP TABLE pos AS SELECT wiki_id, page__page_id, revision__rev_id, "
                "row_number() %s AS position FROM events" % w)
    totals = con.execute(
        "SELECT count(*) FILTER (WHERE prior_rev IS NOT NULL AND lag_rev IS NOT NULL), "
        "count(*) FILTER (WHERE prior_rev = lag_rev) FROM (SELECT meta__dt, prior_state__revision__rev_id AS prior_rev, "
        "LAG(revision__rev_id) %s AS lag_rev, LAG(meta__dt) %s AS lag_dt FROM events) WHERE lag_dt IS NULL OR %s"
        % (w, w, loss_free)).fetchone()
    rows = con.execute(
        "SELECT cause, detail, page_change_kind, previous_kind, cross_topic, count(*), "
        "list([wiki_id, CAST(page__page_id AS VARCHAR), CAST(revision__rev_id AS VARCHAR)] "
        "ORDER BY wiki_id, page__page_id, revision__rev_id)[1:5] FROM ("
        "SELECT q.*, CASE WHEN other_page THEN 'merge or re-keying' WHEN this_page IS NOT NULL THEN 'ordering' "
        "ELSE 'missed event' END AS cause, "
        "CASE WHEN other_page THEN 'prior revision captured under another page_id' "
        "WHEN this_page IS NOT NULL AND this_page > q.position THEN 'prior revision ordered after this event' "
        "WHEN this_page IS NOT NULL THEN 'prior revision ordered before the LAG row' "
        "WHEN prior_rev > lag_rev THEN 'prior revision newer than the LAG revision, not captured' "
        "ELSE 'prior revision older than the LAG revision, not captured' END AS detail FROM ("
        "SELECT m.*, (SELECT max(p.position) FROM pos p WHERE p.wiki_id = m.wiki_id AND p.page__page_id = m.page__page_id "
        "AND p.revision__rev_id = m.prior_rev) AS this_page, "
        "EXISTS (SELECT 1 FROM pos p WHERE p.wiki_id = m.wiki_id AND p.page__page_id <> m.page__page_id "
        "AND p.revision__rev_id = m.prior_rev) AS other_page FROM ("
        "SELECT wiki_id, page__page_id, revision__rev_id, page_change_kind, prior_state__revision__rev_id AS prior_rev, "
        "row_number() %s AS position, LAG(revision__rev_id) %s AS lag_rev, LAG(page_change_kind) %s AS previous_kind, "
        "LAG(meta__topic) %s <> meta__topic AS cross_topic, meta__dt, LAG(meta__dt) %s AS lag_dt FROM events) m "
        "WHERE prior_rev IS NOT NULL AND lag_rev IS NOT NULL AND prior_rev <> lag_rev AND %s) q) "
        "GROUP BY ALL ORDER BY 6 DESC" % (w, w, w, w, w, loss_free)).fetchall()
    con.execute("DROP TABLE pos")
    return {"compared": totals[0], "equal": totals[1], "differ": totals[0] - totals[1]}, [
        {"cause": c, "detail": d, "page_change_kind": k, "previous_kind": pk, "cross_topic": ct, "events": n,
         "examples_wiki_page_rev": "; ".join(":".join(x) for x in ex)} for c, d, k, pk, ct, n, ex in rows]

