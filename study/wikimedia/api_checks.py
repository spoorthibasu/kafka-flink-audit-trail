"""Checks of sampled study events against Wikimedia's public MediaWiki Action API.

Two checks, each on a seeded random sample of event identifiers:

  redirect   events where prior_state.page omits is_redirect but LAG shows it changed.
             The previous revision (prior_state.revision.rev_id, or LAG(rev_id) where
             prior_state has no revision) and the event's own revision are read, and
             each is classified as a redirect or not from its content: for wikitext,
             the wiki's redirect magic word followed by a [[link]] at the start, as
             MediaWiki parses it; for Wikibase entities, a "redirect" key in the JSON.
             confirms LAG: the previous revision has LAG's value and the event's own
             revision the event's value, so the status changed and prior_state omitted
             it. confirms prior_state: the previous revision already has the event's
             value. ambiguous: content hidden, revision missing, another content model,
             or neither pattern.
  revision   events whose prior_state.revision.rev_id no captured event carries. The
             revision is looked up by ID: missing (badrevids, which is what deleted
             revisions also return to anonymous readers), content or editor hidden
             (revision deletion), suppressed, attached to another page_id now, and the
             page's import, delete and merge log entries, and any log entry at the
             revision's own timestamp.

The API is used politely: one request at a time, at most one per --pause seconds, the
study's User-Agent, maxlag=5, and Retry-After honoured. Revision content read for the
redirect check is discarded; only identifiers and verdicts are kept. Page titles are
used in requests and not written out.
"""
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import load  # noqa: E402
import sse  # noqa: E402


class Api(object):
    def __init__(self, pause=1.0, log=print):
        self.pause, self.last, self.requests, self.log = pause, 0.0, 0, log
        self.magic = {}

    def get(self, domain, **params):
        params.update(format="json", formatversion="2", maxlag="5")
        url = "https://%s/w/api.php?%s" % (domain, urllib.parse.urlencode(params))
        for attempt in range(6):
            wait = self.last + self.pause - time.time()
            if wait > 0:
                time.sleep(wait)
            req = urllib.request.Request(url, headers={"User-Agent": sse.USER_AGENT, "Accept": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                self.last = time.time()
                delay = int(e.headers.get("Retry-After") or 10)
                self.log("  HTTP %d from %s, retrying in %d s" % (e.code, domain, delay))
                time.sleep(delay)
                continue
            except (OSError, ValueError) as e:
                self.last = time.time()
                self.log("  %s from %s, retrying" % (type(e).__name__, domain))
                time.sleep(10)
                continue
            self.last = time.time()
            self.requests += 1
            if body.get("error", {}).get("code") == "maxlag":
                time.sleep(int(body["error"].get("lag", 5)) + 1)
                continue
            return body
        raise RuntimeError("no answer from %s after retries" % domain)

    def redirect_words(self, domain):
        if domain not in self.magic:
            body = self.get(domain, action="query", meta="siteinfo", siprop="magicwords")
            words = next((m for m in body["query"]["magicwords"] if m["name"] == "redirect"),
                         {"aliases": ["#REDIRECT"], "case-sensitive": False})
            self.magic[domain] = words
        return self.magic[domain]

    def revision(self, domain, rev_id, with_content):
        props = "ids|timestamp|flags|size|sha1|contentmodel" + ("|content" if with_content else "")
        body = self.get(domain, action="query", prop="revisions", revids=str(rev_id), rvprop=props, rvslots="main")
        q = body.get("query", {})
        if q.get("badrevids"):
            return {"found": False}
        for page in q.get("pages", []):
            for r in page.get("revisions", []):
                slot = r.get("slots", {}).get("main", {})
                return {"found": True, "page_id": page.get("pageid"), "timestamp": r.get("timestamp"),
                        "texthidden": bool(slot.get("texthidden") or r.get("texthidden")),
                        "userhidden": bool(r.get("userhidden")), "commenthidden": bool(r.get("commenthidden")),
                        "suppressed": bool(r.get("suppressed")), "sha1hidden": bool(r.get("sha1hidden")),
                        "contentmodel": slot.get("contentmodel"), "content": slot.get("content")}
        return {"found": False}

    def log_types(self, domain, title, letype=None, start=None, end=None):
        params = dict(action="query", list="logevents", letitle=title, lelimit="500", leprop="type|timestamp|ids")
        if letype:
            params["letype"] = letype
        if start and end:
            params.update(lestart=start, leend=end, ledir="newer")
        body = self.get(domain, **params)
        return [(e["type"], e.get("action"), e["timestamp"]) for e in body.get("query", {}).get("logevents", [])]


def is_redirect(api, domain, rev):
    """True, False or None (cannot tell) from a revision's content."""
    if not rev.get("found") or rev.get("texthidden") or rev.get("content") is None:
        return None, "content not readable"
    model, text = rev["contentmodel"], rev["content"]
    if model == "wikitext":
        words = api.redirect_words(domain)
        flags = 0 if words.get("case-sensitive") else re.IGNORECASE
        alt = "|".join(re.escape(a) for a in words["aliases"])
        return bool(re.match(r"\s*(?:%s)\s*:?\s*\[\[[^\]\n]+\]\]" % alt, text, flags)), "wikitext"
    if model.startswith("wikibase-"):
        try:
            return "redirect" in json.loads(text), model
        except ValueError:
            return None, model + " content not JSON"
    return None, "content model " + model


def redirect_candidates(con):
    w = "OVER (PARTITION BY %s ORDER BY %s)" % (", ".join(load.ENTITY), ", ".join(load.ORDER_BY))
    return con.execute(
        "SELECT wiki_id, page__page_id, revision__rev_id, meta__domain, page_change_kind, previous_kind, "
        "coalesce(prior_state__revision__rev_id, previous_rev) AS previous_rev, prior_state__revision__rev_id = previous_rev, "
        "previous_is_redirect, page__is_redirect "
        "FROM (SELECT *, LAG(page__is_redirect) %s AS previous_is_redirect, LAG(revision__rev_id) %s AS previous_rev, "
        "LAG(page_change_kind) %s AS previous_kind FROM events) "
        "WHERE prior_state__page__is_redirect IS NULL AND previous_is_redirect IS NOT NULL "
        "AND previous_is_redirect IS DISTINCT FROM page__is_redirect "
        "ORDER BY wiki_id, page__page_id, revision__rev_id" % (w, w, w)).fetchall()


def check_redirects(con, api, n, seed, log=print):
    cands = redirect_candidates(con)
    sample = sorted(random.Random(seed).sample(cands, min(n, len(cands))))
    rows = []
    for wiki, page, rev, domain, kind, prev_kind, prev_rev, aligned, lag_val, cur_val in sample:
        prev = api.revision(domain, prev_rev, True)
        prev_redirect, prev_how = is_redirect(api, domain, prev)
        cur = api.revision(domain, rev, True)
        cur_redirect, cur_how = is_redirect(api, domain, cur)
        if prev_redirect is None or cur_redirect is None:
            verdict = "ambiguous"
        elif prev_redirect == lag_val and cur_redirect == cur_val:
            verdict = "confirms LAG"
        elif prev_redirect == cur_val:
            verdict = "confirms prior_state"
        else:
            verdict = "ambiguous"
        rows.append({"wiki_id": wiki, "page_id": page, "rev_id": rev, "page_change_kind": kind,
                     "previous_kind": prev_kind, "previous_rev_id": prev_rev, "lineage_aligned": aligned,
                     "lag_is_redirect": lag_val, "event_is_redirect": cur_val,
                     "previous_revision_redirect_api": prev_redirect, "previous_revision_read_as": prev_how,
                     "event_revision_redirect_api": cur_redirect, "event_revision_read_as": cur_how, "verdict": verdict})
        log("  redirect %s:%s:%s -> %s" % (wiki, page, rev, verdict))
    return len(cands), rows


def missed_candidates(con):
    w = "OVER (PARTITION BY %s ORDER BY %s)" % (", ".join(load.ENTITY), ", ".join(load.ORDER_BY))
    return con.execute(
        "SELECT wiki_id, page__page_id, revision__rev_id, meta__domain, page__page_title, prior_rev, previous_rev, "
        "prior_state__revision__rev_dt, previous_rev_dt "
        "FROM (SELECT *, prior_state__revision__rev_id AS prior_rev, LAG(revision__rev_id) %s AS previous_rev, "
        "LAG(revision__rev_dt) %s AS previous_rev_dt FROM events) q "
        "WHERE prior_rev IS NOT NULL AND previous_rev IS NOT NULL AND prior_rev <> previous_rev "
        "AND NOT EXISTS (SELECT 1 FROM events v WHERE v.wiki_id = q.wiki_id AND v.revision__rev_id = q.prior_rev) "
        "ORDER BY wiki_id, page__page_id, revision__rev_id" % (w, w)).fetchall()


def check_revisions(con, api, n, seed, log=print):
    """Imported: the page has import log entries and the revision's ID is higher than
    the previous captured revision's while its timestamp is older, the inversion an
    import produces because IDs are assigned at import time."""
    cands = missed_candidates(con)
    sample = sorted(random.Random(seed).sample(cands, min(n, len(cands))))
    rows = []
    for wiki, page, rev, domain, title, prior_rev, prev_rev, prior_dt, prev_dt in sample:
        r = api.revision(domain, prior_rev, False)
        logs = api.log_types(domain, title)
        types = sorted({t for t, _, _ in logs})
        ts = r.get("timestamp")
        at_ts = sorted({"%s/%s" % (t, a) for t, a, when in logs if ts and when == ts})
        inverted = prior_rev > prev_rev and prior_dt is not None and prev_dt is not None and prior_dt < prev_dt
        if not r.get("found"):
            category = "missing: deleted or not visible"
        elif r.get("suppressed"):
            category = "suppressed"
        elif r.get("texthidden") or r.get("userhidden") or r.get("commenthidden"):
            category = "revision deleted: parts hidden"
        elif "import" in types and inverted:
            category = "imported"
        else:
            category = "exists"
        rows.append({"wiki_id": wiki, "page_id": page, "rev_id": rev, "missing_prior_rev_id": prior_rev,
                     "lag_rev_id": prev_rev, "prior_newer_than_lag": prior_rev > prev_rev,
                     "id_time_inverted": inverted, "found": r.get("found", False),
                     "page_id_now": r.get("page_id", ""),
                     "on_other_page_now": bool(r.get("found")) and r.get("page_id") != page,
                     "texthidden": r.get("texthidden", ""), "userhidden": r.get("userhidden", ""),
                     "commenthidden": r.get("commenthidden", ""), "suppressed": r.get("suppressed", ""),
                     "page_log_types": " ".join(types), "log_at_revision_timestamp": " ".join(at_ts),
                     "category": category})
        log("  revision %s:%s:%s prior %s -> %s %s" % (wiki, page, rev, prior_rev, category, " ".join(at_ts)))
    return len(cands), rows
