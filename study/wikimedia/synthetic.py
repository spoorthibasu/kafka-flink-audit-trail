#!/usr/bin/env python3
"""Synthetic page-change capture shaped like a real one, for CI.

  python3 study/wikimedia/synthetic.py --out results-smoke-wikimedia/capture

Writes events/hour=YYYY-MM-DDTHH/{eqiad,codfw}.jsonl.gz, manifest.json and
capture_log.jsonl in the layout capture.py produces, plus expected.json with the counts
study.py must find (study.py --expect). Every value is synthetic: titles, user names
and comments are generated strings. Planted on purpose:

  missed event        an edit whose event is not written; the next edit's prior_state
                      names it
  ordering            two consecutive edits whose meta.dt values are swapped
  merge or re-keying  an edit written under another page's page_id
  delete and undelete a delete then an undelete; the undelete drops page.revision_count
  visibility change   a visibility_change that hides the latest revision's comment
  page move           a move with its null revision (no mismatch expected)
  is_redirect         an edit that turns the page into a redirect while prior_state.page
                      omits is_redirect, as the production stream does
  editor record       shared editors keep editing, so prior_state's editor counters
                      (as of the later event) differ from those in the earlier event
  datacenter switch   a stretch of events goes to the codfw topic
  offset gap          three eqiad offsets are skipped, with their events lost
"""
import argparse
import gzip
import hashlib
import json
import os
import random
import time

START_MS = 1767225600000  # 2026-01-01T00:00:00Z
WIKIS = ("enwiki", "dewiki", "commonswiki")
DOMAINS = {"enwiki": "en.wikipedia.org", "dewiki": "de.wikipedia.org", "commonswiki": "commons.wikimedia.org"}


def iso_ms(ms):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms // 1000)) + ".%03dZ" % (ms % 1000)


def iso_s(ms):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms // 1000))


class World(object):
    def __init__(self, seed):
        self.rng = random.Random(seed)
        self.rev = {w: 1000 for w in WIKIS}
        self.page = {w: 100 for w in WIKIS}
        self.users = {}
        self.revisions = {}  # (wiki, rev_id) -> revision dict as it was written

    def user(self, wiki, n, shared=True):
        key = (wiki, n)
        if key not in self.users:
            self.users[key] = {"user_id": 10 + n, "user_central_id": 5000 + n, "user_text": "SyntheticUser%d" % n,
                               "edit_count": 100 * (n + 1), "edit_global_count": 300 * (n + 1),
                               "groups": ["*", "user"] + (["bot"] if n % 7 == 6 else []), "is_bot": n % 7 == 6,
                               "is_system": False, "is_temp": False, "registration_dt": "2020-01-0%dT00:00:00Z" % (1 + n % 9),
                               "first_edit_dt": "2020-02-0%dT00:00:00Z" % (1 + n % 9), "wiki_id": wiki}
        return self.users[key]

    @staticmethod
    def snapshot(u):
        return json.loads(json.dumps(u))


def revision(world, wiki, rev_id, parent, ms, editor, size, comment, minor=False):
    sha = hashlib.sha1(("%s:%d" % (wiki, rev_id)).encode()).hexdigest()[:31]
    r = {"rev_id": rev_id, "rev_parent_id": parent, "rev_dt": iso_s(ms), "rev_size": size, "rev_sha1": sha,
         "is_minor_edit": minor, "comment": comment, "editor": World.snapshot(editor),
         "content_slots": {"main": {"slot_role": "main", "content_model": "wikitext", "content_format": "text/x-wiki",
                                    "content_sha1": sha, "content_size": size, "origin_rev_id": rev_id}},
         "is_comment_visible": True, "is_content_visible": True, "is_editor_visible": True}
    if parent == 0:
        del r["rev_parent_id"]
    return r


def plan(world, n_background):
    """Return [(ms, wiki, page_key, action, args)] for the background and planted pages."""
    acts = []
    rng = world.rng
    span = 6 * 3600 * 1000
    for i in range(n_background):
        wiki = WIKIS[i % len(WIKIS)]
        key = ("bg", i)
        t = START_MS + rng.randrange(0, span // 3)
        acts.append((t, wiki, key, "create", {"user": rng.randrange(0, 12)}))
        for _ in range(rng.randrange(2, 9)):
            t += rng.randrange(60_000, 2_400_000)
            if t >= START_MS + span:
                break
            acts.append((t, wiki, key, "edit", {"user": rng.randrange(0, 12)}))
    # Planted pages use dedicated editors (numbers 100+) that edit nowhere else, so no
    # editor-record drift mixes into their counts. They act early, away from the
    # datacenter switch and the gap.
    base = START_MS + 10 * 60_000
    u = iter(range(100, 200))

    def seq(key, wiki, steps, start):
        t = start
        for action, args in steps:
            acts.append((t, wiki, key, action, args))
            t += 120_000

    for k in range(3):
        seq(("missed", k), WIKIS[k], [("create", {"user": next(u)}), ("edit", {"user": next(u)}),
                                     ("edit", {"user": next(u), "drop": True}), ("edit", {"user": next(u)})],
            base + k * 7_000)
    for k in range(2):
        seq(("swap", k), WIKIS[k], [("create", {"user": next(u)}), ("edit", {"user": next(u)}),
                                   ("edit", {"user": next(u), "swap_with_next": True}), ("edit", {"user": next(u)}),
                                   ("edit", {"user": next(u)})], base + 60_000 + k * 7_000)
    seq(("rekey_target", 0), "dewiki", [("create", {"user": next(u)})], base + 30_000)
    seq(("rekey", 0), "dewiki", [("create", {"user": next(u)}), ("edit", {"user": next(u)}),
                                ("edit", {"user": next(u), "rekey_to": ("rekey_target", 0)}), ("edit", {"user": next(u)})],
        base + 35_000)
    for k in range(2):
        seq(("delund", k), WIKIS[k], [("create", {"user": next(u)}), ("edit", {"user": next(u)}),
                                     ("delete", {"user": next(u)}), ("undelete", {"user": next(u)}),
                                     ("edit", {"user": next(u)})], base + 90_000 + k * 7_000)
    for k in range(2):
        seq(("vis", k), WIKIS[k], [("create", {"user": next(u)}), ("edit", {"user": next(u)}),
                                  ("edit", {"user": next(u)}), ("visibility_change", {"user": next(u)}),
                                  ("edit", {"user": next(u)})], base + 120_000 + k * 7_000)
    for k in range(2):
        seq(("move", k), WIKIS[k], [("create", {"user": next(u)}), ("edit", {"user": next(u)}),
                                   ("move", {"user": next(u)}), ("edit", {"user": next(u)})], base + 150_000 + k * 7_000)
    for k in range(2):
        seq(("redirect", k), WIKIS[k], [("create", {"user": next(u)}), ("edit", {"user": next(u)}),
                                       ("edit", {"user": next(u), "make_redirect": True}), ("edit", {"user": next(u)})],
            base + 180_000 + k * 7_000)
    acts.sort(key=lambda a: (a[0], a[1], str(a[2])))
    return acts


def run(world, acts):
    """Apply actions in time order; return the events to write (dicts with _ms keys)."""
    pages = {}
    events = []
    for ms, wiki, key, action, args in acts:
        user = world.user(wiki, args["user"])
        if action == "create":
            world.page[wiki] += 1
            world.rev[wiki] += 1
            pid = world.page[wiki]
            user["edit_count"] += 1
            user["edit_global_count"] += 1
            rev = revision(world, wiki, world.rev[wiki], 0, ms, user, 100 + world.rng.randrange(900), "synthetic create")
            pages[key] = {"wiki": wiki, "page_id": pid, "title": "Synthetic_page_%d" % pid, "ns": 0,
                          "is_redirect": False, "rev": rev, "deleted": False, "count": 1}
            events.append(make_event(wiki, "create", "insert", pages[key], rev, user, ms, None))
            continue
        p = pages[key]
        if action in ("edit", "move"):
            world.rev[wiki] += 1
            user["edit_count"] += 1
            user["edit_global_count"] += 1
            old = p["rev"]
            # prior_state.revision describes the previous revision with its editor's
            # record as of now, which is what the production stream carries.
            prior_rev = json.loads(json.dumps(old))
            prior_rev["editor"] = World.snapshot(world.users[(wiki, old["editor"]["user_id"] - 10)])
            prior = {"revision": prior_rev}
            if action == "move":
                rev = revision(world, wiki, world.rev[wiki], old["rev_id"], ms, user, old["rev_size"], "synthetic move")
                rev["rev_sha1"] = old["rev_sha1"]
                rev["content_slots"] = json.loads(json.dumps(old["content_slots"]))
                prior["page"] = {"page_title": p["title"]}
                old_title = p["title"]
                p["title"] = p["title"] + "_moved"
            else:
                rev = revision(world, wiki, world.rev[wiki], old["rev_id"], ms, user,
                               old["rev_size"] + world.rng.randrange(-50, 200), "synthetic edit")
                if args.get("make_redirect"):
                    p["is_redirect"] = True
            p["rev"] = rev
            p["count"] += 1
            target = pages[args["rekey_to"]] if args.get("rekey_to") else p
            ev = make_event(wiki, action, "update", target if args.get("rekey_to") else p, rev, user, ms, prior)
            if action == "move":
                ev["created_redirect_page"] = {"page_id": 0, "page_title": old_title, "is_redirect": True}
            ev["_drop"] = bool(args.get("drop"))
            ev["_swap"] = bool(args.get("swap_with_next"))
            events.append(ev)
        elif action == "delete":
            p["deleted"] = True
            ev = make_event(wiki, "delete", "delete", p, p["rev"], user, ms, None)
            ev["page"]["revision_count"] = p["count"]
            events.append(ev)
        elif action == "undelete":
            p["deleted"] = False
            events.append(make_event(wiki, "undelete", "insert", p, p["rev"], user, ms, None))
        elif action == "visibility_change":
            p["rev"] = json.loads(json.dumps(p["rev"]))
            p["rev"]["is_comment_visible"] = False
            events.append(make_event(wiki, "visibility_change", "update", p, p["rev"], user, ms, None))
    return events


def make_event(wiki, kind, changelog, p, rev, performer, ms, prior):
    ev = {"$schema": "/mediawiki/page/change/1.12.0", "changelog_kind": changelog, "dt": iso_s(ms),
          "meta": {"domain": DOMAINS[wiki], "dt": iso_ms(ms), "id": hashlib.md5(("%s%d%s" % (wiki, ms, kind)).encode()).hexdigest(),
                   "request_id": "synthetic", "stream": "mediawiki.page_change.v1",
                   "uri": "https://%s/wiki/%s" % (DOMAINS[wiki], p["title"])},
          "page": {"page_id": p["page_id"], "page_title": p["title"], "namespace_id": p["ns"],
                   "is_redirect": p["is_redirect"], "namespace_is_content": True},
          "page_change_kind": kind, "performer": World.snapshot(performer),
          "revision": json.loads(json.dumps(rev)), "wiki_id": wiki, "_ms": ms}
    if prior is not None:
        ev["prior_state"] = prior
    return ev


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--background-pages", type=int, default=150)
    ap.add_argument("--seed", type=int, default=20261113)
    args = ap.parse_args()
    world = World(args.seed)
    events = run(world, plan(world, args.background_pages))
    events.sort(key=lambda e: e["_ms"])
    # Ordering swaps: exchange meta.dt of a planted edit and the page's next event.
    by_page = {}
    for e in events:
        by_page.setdefault((e["wiki_id"], e["page"]["page_id"]), []).append(e)
    for evs in by_page.values():
        for a, b in zip(evs, evs[1:]):
            if a.get("_swap"):
                a["meta"]["dt"], b["meta"]["dt"] = b["meta"]["dt"], a["meta"]["dt"]
    # Datacenter switch for the middle stretch, then offsets; three eqiad offsets lost.
    switch = (START_MS + int(2.7 * 3600e3), START_MS + int(3.6 * 3600e3))
    gap_at = START_MS + int(4.5 * 3600e3)
    offsets = {"eqiad": 5000, "codfw": 900}
    written, lost, gap = [], [], None
    for e in events:
        dc = "codfw" if switch[0] <= e["_ms"] < switch[1] else "eqiad"
        if e.get("_drop"):
            continue
        if dc == "eqiad" and gap is None and e["_ms"] >= gap_at and len(lost) < 3 and e["page_change_kind"] == "edit":
            lost.append(e)
            offsets["eqiad"] += 1
            if len(lost) == 3:
                gap = {"after_offset": None, "next_offset": None}
            continue
        e["meta"].update({"topic": "%s.mediawiki.page_change.v1" % dc, "partition": 0, "offset": offsets[dc],
                          "key": {"wiki_id": e["wiki_id"], "page_id": e["page"]["page_id"]}})
        offsets[dc] += 1
        written.append(e)
    write(args.out, written, lost)


def write(out, written, lost):
    os.makedirs(out, exist_ok=True)
    topics, files = {}, {}
    gaps = []
    prev = {}
    for e in written:
        m = e["meta"]
        key = "%s:0" % m["topic"]
        t = topics.setdefault(key, {"first_offset": m["offset"], "first_dt": m["dt"], "last_offset": None, "last_dt": None,
                                    "end_offset": None, "end_dt": None, "written": 0, "event_bytes": 0, "canary": 3,
                                    "duplicates": 0, "after_end": 0, "dt_before_start": 0, "gaps": []})
        if key in prev and m["offset"] != prev[key][0] + 1:
            g = {"topic": key, "after_offset": prev[key][0], "next_offset": m["offset"],
                 "missing": m["offset"] - prev[key][0] - 1, "after_dt": prev[key][1], "next_dt": m["dt"]}
            t["gaps"].append(g)
            gaps.append(g)
        prev[key] = (m["offset"], m["dt"])
        t["last_offset"], t["last_dt"] = m["offset"], m["dt"]
        t["end_offset"], t["end_dt"] = m["offset"] + 1, m["dt"]
        clean = {k: v for k, v in e.items() if not k.startswith("_")}
        data = json.dumps(clean, separators=(",", ":"))
        t["written"] += 1
        t["event_bytes"] += len(data.encode())
        sse_id = json.dumps([{"topic": m["topic"], "partition": 0, "offset": m["offset"]}], separators=(",", ":"))
        line = '{"recv_ms":%d,"sse_id":%s,"event":%s}\n' % (e["_ms"], json.dumps(sse_id), data)
        rel = os.path.join("events", "hour=" + m["dt"][:13], m["topic"].split(".")[0] + ".jsonl.gz")
        files.setdefault(rel, []).append(line)
    entries = []
    for rel, lines in sorted(files.items()):
        os.makedirs(os.path.dirname(os.path.join(out, rel)), exist_ok=True)
        with gzip.open(os.path.join(out, rel), "wt", encoding="utf-8") as f:
            f.writelines(lines)
        entries.append({"path": rel, "hour": rel.split("=")[1][:13], "dc": os.path.basename(rel).split(".")[0],
                        "lines": len(lines), "bytes": os.path.getsize(os.path.join(out, rel)), "sha256": ""})
    manifest = {"start_ms": START_MS, "end_ms": START_MS + 6 * 3600 * 1000, "end_offsets": {},
                "user_agent": "synthetic", "rate_cap": 0, "started_ms": START_MS, "finished_ms": START_MS + 7 * 3600 * 1000,
                "received": len(written), "connects": 2, "disconnects": 1, "topics": topics, "files": entries}
    with open(os.path.join(out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    with open(os.path.join(out, "capture_log.jsonl"), "w") as f:
        f.write(json.dumps({"kind": "connected", "ts_ms": START_MS, "last_event_id": "synthetic"}) + "\n")
        f.write(json.dumps({"kind": "disconnected", "ts_ms": START_MS + 3600e3, "reason": "synthetic",
                            "events_on_connection": 0}) + "\n")
        f.write(json.dumps({"kind": "connected", "ts_ms": START_MS + 3600e3 + 1000, "last_event_id": "synthetic"}) + "\n")
    with open(os.path.join(out, "expected.json"), "w") as f:
        json.dump(expected(written, gaps), f, indent=2)
    print("wrote %d events in %d files to %s" % (len(written), len(entries), out))


def expected(written, gaps):
    """Counts computed from the generated events, independently of the study's SQL."""
    pages = {}
    for e in written:
        pages.setdefault((e["wiki_id"], e["page"]["page_id"]), []).append(e)
    key = lambda e: (e["meta"]["dt"], e["revision"]["rev_id"], e["meta"]["topic"], e["meta"]["offset"])
    no_pred = loss = drift = 0
    for evs in pages.values():
        evs.sort(key=key)
        no_pred += 1
        for a, b in zip(evs, evs[1:]):
            if any(g["after_dt"] <= b["meta"]["dt"] and g["next_dt"] >= a["meta"]["dt"] for g in gaps):
                loss += 1
                continue
            pr = b.get("prior_state", {}).get("revision")
            if pr and pr["rev_id"] == a["revision"]["rev_id"] and \
                    pr["editor"]["edit_count"] != a["revision"]["editor"]["edit_count"]:
                drift += 1
    topics_per_page = {k: {e["meta"]["topic"] for e in v} for k, v in pages.items()}
    return {
        "rows": len(written),
        "entities": len(pages),
        "wikis": len({e["wiki_id"] for e in written}),
        "excluded_no_predecessor": no_pred,
        "excluded_loss_possible": loss,
        "pages_in_both_topics": sum(1 for t in topics_per_page.values() if len(t) > 1),
        "gaps": len(gaps),
        "event_causes": {"missed event": 3, "ordering": 6, "merge or re-keying": 2, "delete and undelete": 2,
                         "visibility change": 2},
        "field_classes": [
            {"cause": "other", "detail": "editor record changed between the two events",
             "field": "prior_state.revision.editor.edit_count", "outcome": "differ", "count": drift},
            {"cause": "other", "detail": "no event kind or lineage explains it",
             "field": "prior_state.page.is_redirect", "outcome": "omitted_changed", "count": 2},
            {"cause": "delete and undelete", "detail": "this event is an undelete",
             "field": "prior_state.page.revision_count", "outcome": "omitted_changed", "count": 2},
            {"cause": "visibility change", "detail": "this event is a visibility_change",
             "field": "prior_state.revision.is_comment_visible", "outcome": "omitted_changed", "count": 2},
        ],
        "parent_check_differ": 0,
        "ordering_tie_groups_meta_dt_rev_id": 0,
    }


if __name__ == "__main__":
    main()
