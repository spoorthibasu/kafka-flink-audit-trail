"""Step 6: how often each prior_state field is present, and string value lengths.

Presence is counted over all events and per page_change_kind. String lengths are over
present values, in UTF-8 bytes (DuckDB strlen) and in characters (DuckDB length).
Percentiles are nearest-rank: the value at rank ceil(p/100 * n), from exact length
histograms.
"""
import math

PRIOR = "prior_state__"


def nearest_rank(hist, p):
    """hist: sorted [(value, count)]; value at rank ceil(p/100 * n)."""
    n = sum(c for _, c in hist)
    rank = max(1, math.ceil(p * n / 100))
    seen = 0
    for value, c in hist:
        seen += c
        if seen >= rank:
            return value
    raise ValueError("empty histogram")


def distributions(con, parquet):
    types = dict(con.execute("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('%s'))"
                             % parquet).fetchall())
    prior = [c for c in types if c.startswith(PRIOR)]
    con.execute("CREATE OR REPLACE TEMP VIEW d AS SELECT page_change_kind, %s FROM read_parquet('%s')"
                % (", ".join('"%s"' % c for c in prior), parquet))
    kinds = con.execute("SELECT page_change_kind, count(*) FROM d GROUP BY 1 ORDER BY 1").fetchall()
    total = sum(n for _, n in kinds)
    presence = con.execute("SELECT page_change_kind, %s FROM d GROUP BY 1 ORDER BY 1"
                           % ", ".join('count("%s")' % c for c in prior)).fetchall()
    rows = []
    for i, c in enumerate(prior):
        field = c[len(PRIOR):].replace("__", ".")
        overall = sum(r[i + 1] for r in presence)
        row = {"field": "prior_state." + field, "type": types[c], "events": total, "present": overall}
        for (kind, n), r in zip(kinds, presence):
            row["present_%s" % kind] = r[i + 1]
            row["events_%s" % kind] = n
        if types[c] == "VARCHAR":
            for unit, fn in (("bytes", "strlen"), ("chars", "length")):
                hist = con.execute('SELECT %s("%s") AS v, count(*) FROM d WHERE "%s" IS NOT NULL GROUP BY v ORDER BY v'
                                   % (fn, c, c)).fetchall()
                if hist:
                    row["len_%s_min" % unit] = hist[0][0]
                    row["len_%s_p50" % unit] = nearest_rank(hist, 50)
                    row["len_%s_p95" % unit] = nearest_rank(hist, 95)
                    row["len_%s_max" % unit] = hist[-1][0]
                    row["len_%s_total" % unit] = sum(v * n for v, n in hist)
        rows.append(row)
    return [k for k, _ in kinds], rows
