#!/usr/bin/env python3
"""Fetch the official pages that define LAST_VALUE with IGNORE NULLS and copy the
syntax lines out of them.

bench/engine_docs.py holds what was copied by hand on 2026-10-20. This script fetches
the pages itself, so the lines and versions come from the HTML as served on the day it
runs. It starts from the URLs in bench/engine_docs.py, follows HTTP and meta-refresh
redirects, and follows only links that appear on a fetched page: DuckDB's version
selector entry for the DuckDB release the benchmark runs (requirements.txt pins
1.4.5), and the Athena functions page, whose URL a web search returned on 2026-10-23.

Per page it records the final URL, the SHA-256 of the HTML, the version the page names,
and every text line matching the engine's LAST_VALUE or IGNORE NULLS pattern, verbatim
apart from whitespace collapsing and the permalink "#" Trino renders after a signature.
Writes engine_docs_fetched.csv and its SUMMARY.md section.

  python3 bench/engine_docs_fetch.py --out results
"""
import argparse
import csv
import hashlib
import html
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from benchmark import md_table  # noqa: E402
from summary_sections import replace_section  # noqa: E402

SECTION_HEADING = "## Engine documentation for LAST_VALUE with IGNORE NULLS, fetched by script"
USER_AGENT = "kafka-flink-audit-trail engine_docs_fetch (https://github.com/spoorthibasu/kafka-flink-audit-trail)"
DUCKDB_RUN_VERSION = "1.4"


def fetch(url):
    """GET, following HTTP redirects (urllib in Python 3.9 does not follow 308) and
    meta-refresh redirects."""
    hops = []
    for _ in range(10):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = resp.read()
                final = resp.geturl()
                status = resp.status
        except urllib.error.HTTPError as exc:
            if exc.code in (301, 302, 303, 307, 308) and exc.headers.get("Location"):
                nxt = urllib.parse.urljoin(url, exc.headers["Location"])
                hops.append("%s -> %s (HTTP %d)" % (url, nxt, exc.code))
                url = nxt
                continue
            raise
        text = body.decode("utf-8", "replace")
        if final != url:
            hops.append("%s -> %s (HTTP)" % (url, final))
        m = re.search(r'<meta http-equiv="refresh" content="\d+;\s*url=([^"]+)"', text, re.I)
        if m and len(body) < 4096:
            nxt = urllib.parse.urljoin(final, html.unescape(m.group(1)))
            hops.append("%s -> %s (meta refresh)" % (final, nxt))
            url = nxt
            continue
        return {"url": final, "status": status, "html": text, "sha256": hashlib.sha256(body).hexdigest(), "hops": hops}
    raise RuntimeError("too many redirects from %s" % url)


def text_lines(page_html):
    page_html = re.sub(r"<(script|style)\b.*?</\1>", "", page_html, flags=re.S | re.I)
    txt = html.unescape(re.sub(r"<[^>]+>", "", page_html))
    return [re.sub(r"\s+", " ", ln).strip() for ln in txt.splitlines() if ln.strip()]


def title(page_html):
    m = re.search(r"<title>(.*?)</title>", page_html, re.S)
    return re.sub(r"\s+", " ", html.unescape(m.group(1))).strip() if m else ""


def title_version(page_html):
    """The page title as the documented version. Trino separates the page name from the
    version with an em dash; only the part after it is quoted."""
    t = title(page_html)
    if "\u2014" in t:
        return "page title ends \"%s\"" % t.rsplit("\u2014", 1)[1].strip()
    return "page title reads \"%s\"" % t


def matching(lines, pattern):
    seen, out = set(), []
    for ln in lines:
        ln = re.sub(r"#$", "", ln)
        if re.search(pattern, ln) and ln not in seen:
            seen.add(ln)
            out.append(ln)
    return out


def duckdb_pages(start):
    pages = []
    page = fetch(start)
    selected = re.search(r'class="selectedversion">(.*?)</button>', page["html"], re.S)
    version = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", selected.group(1)))).strip() if selected else ""
    pages.append((page, "version selector shows \"%s\"" % version))
    link = re.search(r"<a href='([^']+)'>%s(?:<|\s)" % re.escape(DUCKDB_RUN_VERSION), page["html"])
    if link:
        p2 = fetch(urllib.parse.urljoin(page["url"], link.group(1)))
        note = re.search(r"This page is for DuckDB [^<]+", p2["html"])
        pages.append((p2, "page reads \"%s\"" % note.group(0).strip() if note else "no version note on the page"))
    return pages


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="results")
    args = ap.parse_args()
    fetched_on = time.strftime("%Y-%m-%d", time.gmtime())
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rows = []

    def add(engine, page, version, syntax, context, note=""):
        """syntax: the pattern of the signature line; context: other lines worth keeping."""
        lines = text_lines(page["html"])
        signature = matching(lines, syntax) if syntax else []
        found = matching(lines, "%s|%s" % (syntax, context) if syntax else context)
        rows.append({"engine": engine, "requested_or_redirect": " ; ".join(page["hops"]), "url": page["url"],
                     "http_status": page["status"], "html_sha256": page["sha256"], "version": version, "syntax_pattern": syntax, "context_pattern": context,
                     "first_syntax_line": signature[0] if signature else "", "matching_lines": " || ".join(found),
                     "note": note, "fetched": fetched_on})
        print("%-9s %s\n          %s\n          %s" % (engine, page["url"], version, found[:4]), flush=True)

    def trino_links(page):
        return "links to %s" % ", ".join(sorted(set(re.findall(r'href="(https://trino\.io/[^"]*window[^"]*)"', page["html"]))))

    for page, version in duckdb_pages("https://duckdb.org/docs/lts/sql/functions/window_functions"):
        add("DuckDB", page, version, r"^last_value\(expr", r"^last_value\(")
    spark = fetch("https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html")
    add("Spark SQL", spark, title_version(spark["html"]), r"^window_function \[ nulls_option \] OVER$",
        r"^LAST_VALUE\(v\) IGNORE NULLS OVER|IGNORE NULLS\.$")
    trino = fetch("https://trino.io/docs/current/functions/window.html")
    add("Trino", trino, title_version(trino["html"]), r"^last_value\(x\)", r"IGNORE NULLS")
    trino_select = fetch("https://trino.io/docs/current/sql/select.html")
    add("Trino", trino_select, title_version(trino_select["html"]), "", r"IGNORE NULLS", "SELECT page searched for IGNORE NULLS")
    for url in ("https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html",
                "https://docs.aws.amazon.com/athena/latest/ug/functions-env3.html"):
        page = fetch(url)
        add("Athena", page, title_version(page["html"]), r"(?i)^last_value\(", r"(?i)last_value|IGNORE NULLS", trino_links(page))

    with open(os.path.join(args.out, "engine_docs_fetched.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    replace_section(os.path.join(args.out, "SUMMARY.md"), SECTION_HEADING, section(started, rows))
    print("engine_docs_fetched.csv written; SUMMARY.md section written")


def section(started, rows):
    lines = [SECTION_HEADING, ""]
    w = lines.append
    w("Fetched by `bench/engine_docs_fetch.py` on %s. Each syntax line is the page's own text, copied by the script; "
      "the page's SHA-256 is in engine_docs_fetched.csv. \"none\" means no line on the page gives a LAST_VALUE "
      "signature. The section \"Engine documentation for LAST_VALUE with IGNORE NULLS\" holds the hand copy of "
      "2026-10-20 and is unchanged. Text lines are the page's text split where the HTML breaks lines, so a sentence "
      "can span two lines." % started)
    w("")
    w(md_table(["Engine", "URL as served", "Documented version", "Syntax line on the page"],
               [[r["engine"], r["url"], r["version"],
                 ("`%s`" % r["first_syntax_line"]) if r["first_syntax_line"] else "none"]
                for r in rows]))
    w("")
    for r in rows:
        extra = []
        if r["requested_or_redirect"]:
            extra.append("redirect: %s" % r["requested_or_redirect"])
        if r["matching_lines"] and r["matching_lines"] != r["first_syntax_line"]:
            extra.append("lines mentioning LAST_VALUE or IGNORE NULLS: %s" % r["matching_lines"])
        if r["note"]:
            extra.append(r["note"])
        if extra:
            w("- %s, %s: %s." % (r["engine"], r["url"], "; ".join(extra)))
    w("")
    w("Files: `engine_docs_fetched.csv`.")
    w("")
    return "\n".join(lines)


if __name__ == "__main__":
    main()
